/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file.s3

import aws.sdk.kotlin.runtime.auth.credentials.AssumeRoleParameters
import aws.sdk.kotlin.runtime.auth.credentials.DefaultChainCredentialsProvider
import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.runtime.auth.credentials.StsAssumeRoleCredentialsProvider
import aws.sdk.kotlin.runtime.auth.credentials.StsWebIdentityCredentialsProvider
import java.io.File
import aws.sdk.kotlin.services.s3.model.CopyObjectRequest
import aws.sdk.kotlin.services.s3.model.CreateMultipartUploadRequest
import aws.sdk.kotlin.services.s3.model.Delete
import aws.sdk.kotlin.services.s3.model.DeleteObjectRequest
import aws.sdk.kotlin.services.s3.model.DeleteObjectsRequest
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.HeadObjectRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.sdk.kotlin.services.s3.model.ObjectIdentifier
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.smithy.kotlin.runtime.auth.awscredentials.CredentialsProvider
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.toInputStream
import aws.smithy.kotlin.runtime.http.engine.crt.CrtHttpEngine
import aws.smithy.kotlin.runtime.net.url.Url
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import io.airbyte.cdk.load.command.aws.AWSAccessKeyConfigurationProvider
import io.airbyte.cdk.load.command.aws.AWSArnRoleConfigurationProvider
import io.airbyte.cdk.load.command.aws.AwsAssumeRoleCredentials
import io.airbyte.cdk.load.command.object_storage.ObjectStorageUploadConfigurationProvider
import io.airbyte.cdk.load.command.s3.S3BucketConfiguration
import io.airbyte.cdk.load.command.s3.S3BucketConfigurationProvider
import io.airbyte.cdk.load.command.s3.S3ClientConfigurationProvider
import io.airbyte.cdk.load.file.object_storage.ObjectStorageClient
import io.airbyte.cdk.load.file.object_storage.RemoteObject
import io.airbyte.cdk.load.file.object_storage.StreamingUpload
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import java.io.InputStream
import kotlinx.coroutines.flow.flow

private const val DELETE_BATCH_SIZE = 1000

// TEL-21303: default path for the Kubernetes-projected ServiceAccount token every pod gets,
// even one Airbyte spun up itself with no AWS-specific wiring of its own. When role_arn is set
// and no static access key is given, we exchange this token for temporary AWS credentials via
// STS AssumeRoleWithWebIdentity instead of requiring a separate bootstrap credential.
private const val DEFAULT_WEB_IDENTITY_TOKEN_FILE_PATH =
    "/var/run/secrets/kubernetes.io/serviceaccount/token"

data class S3Object(override val key: String, override val storageConfig: S3BucketConfiguration) :
    RemoteObject<S3BucketConfiguration> {
    val keyWithBucketName
        get() = "${storageConfig.s3BucketName}/$key"
}

interface S3Client : ObjectStorageClient<S3Object>

/**
 * The primary and recommended S3 client implementation -- kotlin-friendly with suspend functions.
 * However, there's a bug that can cause hard failures under high-concurrency. (Partial workaround
 * in place https://github.com/awslabs/aws-sdk-kotlin/issues/1214#issuecomment-2464831817).
 */
@SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION", justification = "Kotlin async continuation")
class S3KotlinClient(
    private val client: aws.sdk.kotlin.services.s3.S3Client,
    val bucketConfig: S3BucketConfiguration,
) : S3Client {
    private val log = KotlinLogging.logger {}

    override suspend fun list(prefix: String) = flow {
        var token: String? = null
        do {
            val resp =
                client.listObjectsV2(
                    ListObjectsV2Request {
                        bucket = bucketConfig.s3BucketName
                        this.prefix = prefix
                        continuationToken = token
                    }
                )
            resp.contents?.forEach { emit(S3Object(it.key!!, bucketConfig)) }
            token = resp.nextContinuationToken
        } while (token != null)
    }

    override suspend fun move(remoteObject: S3Object, toKey: String): S3Object {
        val request = CopyObjectRequest {
            bucket = bucketConfig.s3BucketName
            this.key = toKey
            copySource = remoteObject.keyWithBucketName
        }
        client.copyObject(request)
        delete(remoteObject)
        return S3Object(toKey, bucketConfig)
    }

    override suspend fun move(key: String, toKey: String): S3Object {
        return move(S3Object(key, bucketConfig), toKey)
    }

    override suspend fun <R> get(key: String, block: (InputStream) -> R): R {
        val request = GetObjectRequest {
            bucket = bucketConfig.s3BucketName
            this.key = key
        }
        return client.getObject(request) {
            val inputStream =
                it.body?.toInputStream()
                    ?: throw IllegalStateException(
                        "S3 object body is null (this indicates a failure, not an empty object)"
                    )
            block(inputStream)
        }
    }

    override suspend fun getMetadata(key: String): Map<String, String> {
        val request = HeadObjectRequest {
            bucket = bucketConfig.s3BucketName
            this.key = key
        }
        return client.headObject(request).metadata ?: emptyMap()
    }

    override suspend fun put(key: String, bytes: ByteArray): S3Object {
        val request = PutObjectRequest {
            bucket = bucketConfig.s3BucketName
            this.key = key
            body = ByteStream.fromBytes(bytes)
        }
        client.putObject(request)
        return S3Object(key, bucketConfig)
    }

    override suspend fun delete(remoteObject: S3Object) {
        val request = DeleteObjectRequest {
            bucket = remoteObject.storageConfig.s3BucketName
            this.key = remoteObject.key
        }
        client.deleteObject(request)
    }

    override suspend fun delete(key: String) {
        delete(S3Object(key, bucketConfig))
    }

    override suspend fun delete(keys: Set<String>) {
        keys.chunked(DELETE_BATCH_SIZE).forEach { chunk ->
            val deleteObjects = Delete { objects = chunk.map { ObjectIdentifier { key = it } } }
            val request = DeleteObjectsRequest {
                bucket = bucketConfig.s3BucketName
                delete = deleteObjects
            }
            client.deleteObjects(request)
        }
    }

    override suspend fun startStreamingUpload(
        key: String,
        metadata: Map<String, String>
    ): StreamingUpload<S3Object> {
        val request = CreateMultipartUploadRequest {
            this.bucket = bucketConfig.s3BucketName
            this.key = key
            this.metadata = metadata
        }
        val response = client.createMultipartUpload(request)

        log.debug { "Starting multipart upload for $key (uploadId=${response.uploadId})" }

        return S3StreamingUpload(client, bucketConfig, response)
    }
}

/**
 * [assumeRoleCredentials] is required if [keyConfig] does not have an access key, _and_ [arnRole]
 * includes a nonnull role ARN. Otherwise, it is ignored.
 */
@Factory
class S3ClientFactory(
    private val arnRole: AWSArnRoleConfigurationProvider,
    private val keyConfig: AWSAccessKeyConfigurationProvider,
    private val bucketConfig: S3BucketConfigurationProvider,
    private val assumeRoleCredentials: AwsAssumeRoleCredentials?,
    private val s3ClientConfig: S3ClientConfigurationProvider? = null,
) {
    private val log = KotlinLogging.logger {}

    companion object {
        const val AIRBYTE_STS_SESSION_NAME = "airbyte-sts-session"

        fun <T> make(config: T, assumeRoleCredentials: AwsAssumeRoleCredentials?) where
        T : S3BucketConfigurationProvider,
        T : AWSAccessKeyConfigurationProvider,
        T : AWSArnRoleConfigurationProvider,
        T : ObjectStorageUploadConfigurationProvider =
            S3ClientFactory(config, config, config, assumeRoleCredentials).make()
    }

    @Singleton
    @Secondary
    fun make(): S3Client {
        if (s3ClientConfig?.s3ClientConfiguration?.useLegacyJavaClient == true) {
            log.info { "Creating S3 client using legacy Java SDK" }
            return if (
                arnRole.awsArnRoleConfiguration.roleArn != null && assumeRoleCredentials != null
            ) {
                S3LegacyJavaClientFactory()
                    .createFromAssumeRole(
                        arnRole.awsArnRoleConfiguration,
                        assumeRoleCredentials,
                        bucketConfig.s3BucketConfiguration
                    )
            } else {
                S3LegacyJavaClientFactory()
                    .createFromAccessKey(
                        keyConfig.awsAccessKeyConfiguration,
                        bucketConfig.s3BucketConfiguration
                    )
            }
        }

        log.info { "Creating S3 client using Kotlin SDK" }

        val credsProvider: CredentialsProvider =
            if (keyConfig.awsAccessKeyConfiguration.accessKeyId != null) {
                // TEL-21303: session token is set when the caller passed short-lived STS
                // credentials instead of long-lived IAM user keys. Leaving it null keeps the
                // long-lived-key behaviour byte for byte.
                if (keyConfig.awsAccessKeyConfiguration.sessionToken != null) {
                    log.info {
                        "Using temporary STS credentials supplied in the connector configuration"
                    }
                }
                StaticCredentialsProvider {
                    accessKeyId = keyConfig.awsAccessKeyConfiguration.accessKeyId
                    secretAccessKey = keyConfig.awsAccessKeyConfiguration.secretAccessKey
                    sessionToken = keyConfig.awsAccessKeyConfiguration.sessionToken
                }
            } else if (
                arnRole.awsArnRoleConfiguration.roleArn != null &&
                    File(DEFAULT_WEB_IDENTITY_TOKEN_FILE_PATH).exists()
            ) {
                // TEL-21303: role_arn given, no static bootstrap credentials required -- the
                // pod's own Kubernetes ServiceAccount token (mounted by default in every pod,
                // no special wiring needed) is exchanged directly for temporary AWS credentials.
                // This is what lets a connector job pod that Airbyte spun up on its own -- with
                // no AWS env vars or projected token volume of its own -- still assume role_arn.
                log.info {
                    "role_arn is set; assuming it via STS web identity using the pod's default ServiceAccount token"
                }
                StsWebIdentityCredentialsProvider(
                    roleArn = arnRole.awsArnRoleConfiguration.roleArn!!,
                    webIdentityTokenFilePath = DEFAULT_WEB_IDENTITY_TOKEN_FILE_PATH,
                    region = bucketConfig.s3BucketConfiguration.s3BucketRegion,
                    roleSessionName = AIRBYTE_STS_SESSION_NAME,
                )
            } else if (
                arnRole.awsArnRoleConfiguration.roleArn != null && assumeRoleCredentials != null
            ) {
                // Legacy path: role_arn given alongside a separate bootstrap credential the
                // Platform injected (e.g. Airbyte Cloud). Kept for backward compatibility.
                val assumeRoleParams =
                    AssumeRoleParameters(
                        roleArn = arnRole.awsArnRoleConfiguration.roleArn!!,
                        roleSessionName = AIRBYTE_STS_SESSION_NAME,
                        externalId = assumeRoleCredentials.externalId,
                    )
                val creds = StaticCredentialsProvider {
                    accessKeyId = assumeRoleCredentials.accessKey
                    secretAccessKey = assumeRoleCredentials.secretKey
                }
                StsAssumeRoleCredentialsProvider(
                    bootstrapCredentialsProvider = creds,
                    assumeRoleParameters = assumeRoleParams
                )
            } else {
                DefaultChainCredentialsProvider()
            }

        val s3SdkClient =
            aws.sdk.kotlin.services.s3.S3Client {
                region = bucketConfig.s3BucketConfiguration.s3BucketRegion
                credentialsProvider = credsProvider
                endpointUrl =
                    bucketConfig.s3BucketConfiguration.s3Endpoint?.let {
                        if (it.isNotBlank()) {
                            Url.parse(it)
                        } else null
                    }

                // Fix for connection reset issue:
                // https://github.com/awslabs/aws-sdk-kotlin/issues/1214#issuecomment-2464831817
                httpClient(CrtHttpEngine)
            }

        return S3KotlinClient(
            s3SdkClient,
            bucketConfig.s3BucketConfiguration,
        )
    }
}

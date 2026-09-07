/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.command.aws

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle

/**
 * Mix-in to a configuration to add AWS access key id and secret access key fields as properties.
 *
 * See [io.airbyte.cdk.load.command.DestinationConfiguration] for more details on how to use this
 * interface.
 */
interface AWSAccessKeySpecification {
    @get:JsonSchemaTitle("Access Key ID")
    @get:JsonPropertyDescription(
        "The access key ID to access the S3 bucket. Airbyte requires Read and Write permissions to the given bucket. Read more <a href=\"https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys\">here</a>."
    )
    @get:JsonProperty("access_key_id")
    @get:JsonSchemaInject(
        json =
            """{"examples":["A012345678910EXAMPLE"],"airbyte_secret": true,"always_show": true}"""
    )
    val accessKeyId: String?

    @get:JsonSchemaTitle("Secret Access Key")
    @get:JsonPropertyDescription(
        "The corresponding secret to the access key ID. Read more <a href=\"https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys\">here</a>"
    )
    @get:JsonProperty("secret_access_key")
    @get:JsonSchemaInject(
        json =
            """{"examples":["a012345678910ABCDEFGH/AbCdEfGhEXAMPLEKEY"],"airbyte_secret": true,"always_show": true}"""
    )
    val secretAccessKey: String?

    // TEL-21303: temporary STS credentials carried in the connector config.
    // A connector job pod that Airbyte creates for a sync has no identity of its own -- no
    // ServiceAccount token (replication pods set automountServiceAccountToken: false), no AWS
    // env vars, no mounted secrets. Its configuration is the only channel in, so short-lived
    // credentials minted by the caller have to travel alongside the access key they belong to.
    @get:JsonSchemaTitle("Session Token")
    @get:JsonPropertyDescription(
        "The session token for temporary credentials. Required when the access key ID and secret access key are short-lived credentials issued by AWS STS, and omitted for long-lived IAM user keys."
    )
    @get:JsonProperty("session_token")
    @get:JsonSchemaInject(
        json = """{"examples":["FwoGZXIvYXdzEBYaEXAMPLESESSIONTOKEN"],"airbyte_secret": true}"""
    )
    val sessionToken: String?
        get() = null

    fun toAWSAccessKeyConfiguration(): AWSAccessKeyConfiguration {
        return AWSAccessKeyConfiguration(accessKeyId, secretAccessKey, sessionToken)
    }
}

data class AWSAccessKeyConfiguration(
    val accessKeyId: String?,
    val secretAccessKey: String?,
    val sessionToken: String? = null,
)

interface AWSAccessKeyConfigurationProvider {
    val awsAccessKeyConfiguration: AWSAccessKeyConfiguration
}

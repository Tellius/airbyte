/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file.object_storage

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Duration
import java.util.UUID

/**
 * Data class representing the owner of a datasource from middleware API.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class DatasourceOwner(
    @JsonProperty("id") val id: String,
    @JsonProperty("username") val username: String,
    @JsonProperty("firstName") val firstName: String,
    @JsonProperty("lastName") val lastName: String,
    @JsonProperty("email") val email: String
)

/**
 * Data class representing a datasource response from middleware API.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class DatasourceResponse(
    @JsonProperty("id") val id: String,
    @JsonProperty("name") val name: String,
    @JsonProperty("type") val type: String,
    @JsonProperty("owner") val owner: DatasourceOwner
)

/**
 * Data class representing the auth token response from kaiya-unstructured-service.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class AuthTokenResponse(
    @JsonProperty("token") val token: String
)

/**
 * HTTP client for calling the middleware service to notify about uploaded documents.
 * This is a hard-coded integration point for Tellius middleware service.
 * 
 * @param datasourceId Optional datasource ID to fetch owner information for middleware integration
 */
class MiddlewareApiClient(private val datasourceId: String? = null) {
    private val log = KotlinLogging.logger {}
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)  // Force HTTP/1.1 (middleware has issues with HTTP/2)
        .connectTimeout(Duration.ofSeconds(30))
        .build()
    private val objectMapper = ObjectMapper()
    
    // Cached owner information from datasource lookup
    private var cachedUserId: String? = null
    private var cachedDatasourceId: String? = null

    init {
        if (datasourceId != null) {
            try {
                fetchDatasourceOwner()
            } catch (e: Exception) {
                log.error(e) { "Failed to fetch datasource owner for ID: $datasourceId" }
                throw RuntimeException("Failed to initialize middleware client: Unable to fetch datasource owner", e)
            }
        }
    }

    companion object {
        private const val MIDDLEWARE_URL_BASE = "http://middleware-service:8082/unstructure"
        private const val KAIYA_AUTH_TOKEN_URL = "http://kaiya-unstructured-service:8080/api/unstructured/internal-auth-token"
        
        // Hard-coded headers as per curl specification
        private const val HEADER_ACCEPT = "application/json, text/plain, */*"
        private const val HEADER_ACCEPT_LANGUAGE = "en-GB,en-US;q=0.9,en;q=0.8"
        private const val HEADER_CSRF = "a3so9snoi9"
        private const val HEADER_CACHE_CONTROL = "no-cache"
        private const val HEADER_DNT = "1"
        private const val HEADER_PRAGMA = "no-cache"
        private const val HEADER_REFERER = "https://dev1.dev.tellius.com/data/business-view"
        private const val HEADER_SEC_FETCH_DEST = "empty"
        private const val HEADER_SEC_FETCH_MODE = "cors"
        private const val HEADER_SEC_FETCH_SITE = "same-origin"
        private const val HEADER_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        private const val HEADER_SEC_CH_UA = "\"Not=A?Brand\";v=\"24\", \"Chromium\";v=\"140\""
        private const val HEADER_SEC_CH_UA_MOBILE = "?0"
        private const val HEADER_SEC_CH_UA_PLATFORM = "\"macOS\""
        private const val HEADER_USERID = "TELLIUS_SUPERUSER_ID"
        private const val HEADER_CONTENT_TYPE = "application/json"
    }

    /**
     * Send document metadata to the middleware service.
     * 
     * @param metadata The document metadata to send
     * @return true if successful, false otherwise
     */
    suspend fun notifyDocumentUpload(metadata: DocumentMetadata): Boolean {
        if (!metadata.hasEssentialMetadata()) {
            log.warn { 
                "Skipping API call - essential metadata is missing. " +
                "name=${metadata.name}, sourcePath=${metadata.sourcePath}, " +
                "nameIsBlank=${metadata.name.isNullOrBlank()}, " +
                "sourcePathIsBlank=${metadata.sourcePath.isNullOrBlank()}, " +
                "metadata=$metadata"
            }
            return false
        }

        return try {
            val payload = buildPayload(metadata)
            val request = buildRequest(payload)
            
            // Build complete HTTP request representation for logging
            val requestHeaders = StringBuilder()
            request.headers().map().forEach { (name, values) ->
                values.forEach { value ->
                    requestHeaders.append("  $name: $value\n")
                }
            }
            
            val userIdInfo = if (cachedUserId != null) {
                "from datasource owner (ID: $cachedUserId)"
            } else {
                "fallback SUPERUSER"
            }
            
            val sourceInfo = if (!metadata.fileId.isNullOrBlank()) {
                "fileId=${metadata.fileId}, sourceUri=${metadata.sourceUri}"
            } else {
                "sourcePath=${metadata.sourcePath}"
            }
            
            log.info {
                "Sending document to middleware API:\n" +
                "  Document: ${metadata.name}\n" +
                "  Source: $sourceInfo\n" +
                "  User: $userIdInfo\n" +
                "  Method: ${request.method()}\n" +
                "  URL: ${request.uri()}\n" +
                "  content_type=${metadata.contentType}, prefix_path=${metadata.prefixPath}, " +
                "  storage_class=${metadata.storageClass}, access_tier=${metadata.accessTier}, " +
                "  owner=${metadata.owner}, created_at=${metadata.createdAt}, " +
                "  file_size_from_source=${metadata.fileSizeFromSource}"
            }
            
            log.debug { 
                "Request details:\n" +
                "Headers:\n$requestHeaders" +
                "Payload:\n$payload"
            }
            
            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            
            if (response.statusCode() in 200..299) {
                log.info { 
                    "Successfully notified middleware: ${metadata.name} (HTTP ${response.statusCode()})"
                }
                true
            } else {
                log.warn { 
                    "Middleware returned non-success status: HTTP ${response.statusCode()}, " +
                    "document: ${metadata.name}, body: ${response.body()}" 
                }
                false
            }
        } catch (e: Exception) {
            log.error(e) { 
                "Failed to call middleware API for document: ${metadata.name}. " +
                "Error will be logged but sync will continue."
            }
            false
        }
    }

    /**
     * Fetch datasource owner information from the middleware API.
     * This is called once during initialization if datasourceId is provided.
     */
    private fun fetchDatasourceOwner() {
        log.info { "Fetching datasource owner for datasource ID: $datasourceId" }
        
        val url = "$MIDDLEWARE_URL_BASE/datasources/$datasourceId"
        val authToken = fetchAuthorizationToken()
        
        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .version(HttpClient.Version.HTTP_1_1)
            .header("Content-Type", HEADER_CONTENT_TYPE)
            .header("Authorization", authToken)
            .header("USERID", HEADER_USERID)
            .GET()
            .timeout(Duration.ofSeconds(30))
            .build()
        
        log.debug { "Datasource lookup - GET $url" }
        
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        
        if (response.statusCode() != 200) {
            throw RuntimeException("Failed to fetch datasource: HTTP ${response.statusCode()} - ${response.body()}")
        }
        
        val datasourceResponse = objectMapper.readValue(response.body(), DatasourceResponse::class.java)
        cachedUserId = datasourceResponse.owner.id
        cachedDatasourceId = datasourceResponse.id
        
        log.info { "Successfully fetched datasource owner - user ID: $cachedUserId, datasource ID: $cachedDatasourceId" }
    }

    /**
     * Fetch the authorization token from kaiya-unstructured-service.
     * This method is called each time we need to make a request to ensure we have the latest token.
     * 
     * @return The authorization token
     * @throws RuntimeException if the token cannot be fetched
     */
    private fun fetchAuthorizationToken(): String {
        log.info { "Fetching authorization token from kaiya service" }
        
        val request = HttpRequest.newBuilder()
            .uri(URI.create(KAIYA_AUTH_TOKEN_URL))
            .version(HttpClient.Version.HTTP_1_1)
            .header("USERID", HEADER_USERID)
            .GET()
            .timeout(Duration.ofSeconds(30))
            .build()
        
        return try {
            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            
            if (response.statusCode() != 200) {
                throw RuntimeException(
                    "Failed to fetch auth token from kaiya service: HTTP ${response.statusCode()} - ${response.body()}"
                )
            }
            
            val tokenResponse = objectMapper.readValue(response.body(), AuthTokenResponse::class.java)
            log.info { "Successfully fetched authorization token from kaiya service" }
            
            tokenResponse.token
        } catch (e: Exception) {
            log.error(e) { "Failed to fetch authorization token from kaiya service at $KAIYA_AUTH_TOKEN_URL" }
            throw RuntimeException("Unable to fetch authorization token from kaiya service", e)
        }
    }

    /**
     * Build the JSON payload for the API request.
     */
    private fun buildPayload(metadata: DocumentMetadata): String {
        val payload = objectMapper.createObjectNode()

        // Generate a NEW random UUID for this middleware entry
        // Each sync creates a new entry with a unique ID
        payload.put("id", UUID.randomUUID().toString())

        // Add required fields
        payload.put("name", metadata.name ?: "unknown")
        payload.put("type", metadata.type ?: "unknown")
        payload.put("internal_path", metadata.internalPath ?: "")

        // Format timestamp to match middleware expectations (no timezone, with microseconds)
        val formattedTimestamp = formatTimestampForMiddleware(metadata.docUpdatedAt)
        payload.put("doc_updated_at", formattedTimestamp ?: "")

        // Prefer actual file size from source (_ab_source_file_size) over serialized record size
        val effectiveFileSize = metadata.fileSizeFromSource ?: metadata.fileSizeBytes
        val fileSizeInt = effectiveFileSize.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        payload.put("file_size_bytes", fileSizeInt)

        payload.put("source_path", metadata.sourcePath ?: "")

        // Add datasource_id if available from datasource lookup
        // Note: user_id is sent in USERID header, NOT in payload
        if (cachedDatasourceId != null) {
            payload.put("datasource_id", cachedDatasourceId)
        }

        // Build meta_data JSONB object
        // Contains: deterministic fingerprint for dedup, source identifiers, and all
        // new document metadata fields extracted from the source connectors.
        val metaDataObject = objectMapper.createObjectNode()

        // Deterministic fingerprint — stays the same across syncs for the same source document.
        // Priority: fileId (GDrive/SharePoint) > sourcePath (S3/Azure/GCS/Gong) > name
        val (fingerprintInput, fieldUsed) = when {
            metadata.fileId?.isNotBlank() == true -> Pair(metadata.fileId, "fileId")
            metadata.sourcePath?.isNotBlank() == true -> Pair(metadata.sourcePath, "sourcePath")
            metadata.name?.isNotBlank() == true -> Pair(metadata.name, "name")
            else -> Pair("unknown", "fallback")
        }

        val documentFingerprint = generateDeterministicUUID(fingerprintInput)

        log.info {
            "Generated document_fingerprint for '${metadata.name}': " +
            "field_used=$fieldUsed, value='$fingerprintInput', fingerprint=$documentFingerprint"
        }

        metaDataObject.put("document_fingerprint", documentFingerprint)
        metaDataObject.put("airbyte_sync_timestamp", System.currentTimeMillis())

        // Source file identifiers (Google Drive, SharePoint)
        if (!metadata.fileId.isNullOrBlank()) {
            metaDataObject.put("source_file_id", metadata.fileId)
        }
        if (!metadata.sourceUri.isNullOrBlank()) {
            metaDataObject.put("source_redirect_uri", metadata.sourceUri)
        }

        // --- New document metadata fields (Phase 2) ---

        // Common across all sources
        metadata.contentType?.let      { metaDataObject.put("content_type", it) }
        metadata.prefixPath?.let       { metaDataObject.put("prefix_path", it) }

        // GDrive-specific
        metadata.createdAt?.let        { metaDataObject.put("created_at", it) }
        metadata.owner?.let            { metaDataObject.put("owner", it) }
        metadata.lastModifiedBy?.let   { metaDataObject.put("last_modified_by", it) }
        metadata.shared?.let           { metaDataObject.put("shared", it) }

        // S3 / GCS storage class
        metadata.storageClass?.let     { metaDataObject.put("storage_class", it) }

        // Azure access tier
        metadata.accessTier?.let       { metaDataObject.put("access_tier", it) }

        // S3 user metadata and object tags (stored as JSON strings by the source connector)
        metadata.userMetadata?.let     { metaDataObject.put("user_metadata", it) }
        metadata.objectTags?.let       { metaDataObject.put("object_tags", it) }

        // Azure blob and container metadata (JSON strings)
        metadata.blobMetadata?.let     { metaDataObject.put("blob_metadata", it) }
        metadata.containerMetadata?.let { metaDataObject.put("container_metadata", it) }

        // GCS custom metadata (JSON string)
        metadata.customMetadata?.let   { metaDataObject.put("custom_metadata", it) }

        // SharePoint-specific fields
        metadata.siteName?.let         { metaDataObject.put("site_name", it) }
        metadata.libraryName?.let      { metaDataObject.put("library_name", it) }

        // Actual file size from source (for informational purposes; file_size_bytes top-level
        // is already set to this value, but also store here for downstream queries on meta_data)
        metadata.fileSizeFromSource?.let { metaDataObject.put("file_size_bytes_source", it) }

        payload.set<ObjectNode>("meta_data", metaDataObject)

        return objectMapper.writeValueAsString(payload)
    }
    
    /**
     * Generate a deterministic UUID v3 (MD5-based) as a document fingerprint.
     * Same source document will always generate the same fingerprint UUID across syncs.
     * This allows the middleware to track the same source document over multiple sync runs.
     * 
     * The input string is prioritized as follows:
     * - fileId (Google Drive, SharePoint): Prevents collisions from duplicate filenames
     * - sourcePath (S3, Azure Blob, Gong): Uses unique file path as identifier
     * - name: Fallback when neither fileId nor sourcePath is available
     * 
     * @param sourcePath The source identifier (fileId, path, or name) of the document
     * @return A deterministic UUID string that serves as the document fingerprint
     */
    private fun generateDeterministicUUID(sourcePath: String): String {
        // Generate MD5 hash of the source identifier
        val md5 = MessageDigest.getInstance("MD5")
        val hash = md5.digest(sourcePath.toByteArray(StandardCharsets.UTF_8))
        
        // Convert to UUID v3 format (MD5-based UUID)
        // Set version bits (version 3 = MD5)
        hash[6] = ((hash[6].toInt() and 0x0f) or 0x30).toByte()
        // Set variant bits (RFC 4122 variant)
        hash[8] = ((hash[8].toInt() and 0x3f) or 0x80).toByte()
        
        // Format as standard UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        return "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x".format(
            hash[0], hash[1], hash[2], hash[3],
            hash[4], hash[5],
            hash[6], hash[7],
            hash[8], hash[9],
            hash[10], hash[11], hash[12], hash[13], hash[14], hash[15]
        )
    }
    
    /**
     * Format timestamp to middleware expectations:
     * "2025-10-31T15:42:30.123456" (no timezone, with microseconds)
     */
    private fun formatTimestampForMiddleware(timestamp: String?): String? {
        if (timestamp.isNullOrBlank()) return null
        
        return try {
            // Remove timezone indicators
            var cleaned = timestamp
                .replace("Z", "")
                .replace("+00:00", "")
                .replace(Regex("[+-]\\d{2}:\\d{2}$"), "")
            
            // Ensure microseconds are present
            if (cleaned.contains(".")) {
                val parts = cleaned.split(".")
                if (parts.size == 2) {
                    val fractional = parts[1].padEnd(6, '0').take(6)
                    "${parts[0]}.$fractional"
                } else {
                    cleaned
                }
            } else {
                // No fractional seconds, add .000000
                "$cleaned.000000"
            }
        } catch (e: Exception) {
            log.warn(e) { "Could not format timestamp: $timestamp" }
            timestamp
        }
    }

    /**
     * Build the HTTP request with only the essential headers.
     * Unnecessary browser headers can cause middleware crashes.
     * Uses the fetched user ID if available, otherwise falls back to TELLIUS_SUPERUSER_ID.
     * Fetches the authorization token dynamically from kaiya service.
     */
    private fun buildRequest(payload: String): HttpRequest {
        val url = "$MIDDLEWARE_URL_BASE/documents"
        // Use cached user ID from datasource lookup if available, otherwise fall back to superuser
        val userIdHeader = cachedUserId ?: HEADER_USERID
        
        // Fetch the authorization token dynamically each time
        val authToken = fetchAuthorizationToken()
        
        return HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", HEADER_CONTENT_TYPE)
            .header("Authorization", authToken)
            .header("USERID", userIdHeader)
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .timeout(Duration.ofSeconds(30))
            .build()
    }
}


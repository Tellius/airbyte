/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file.object_storage

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID

/**
 * HTTP client for calling the middleware service to notify about uploaded documents.
 * This is a hard-coded integration point for Tellius middleware service.
 */
class MiddlewareApiClient {
    private val log = KotlinLogging.logger {}
    private val httpClient: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .build()
    private val objectMapper = ObjectMapper()

    companion object {
        private const val MIDDLEWARE_URL = "http://middleware-service:8082/unstructure/documents"
        
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
        private const val HEADER_AUTHORIZATION = "uCamYLcggKFEzL6XCgTS"
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
            
            log.info { "!!!!HARSH's " +
                "Calling middleware API for document: ${metadata.name} at ${metadata.internalPath}" 
            }
            log.info { "!!!!HARSH's " +  "API payload: $payload" }
            
            val request = buildRequest(payload)
            
            log.info { "!!!!HARSH's " + "Sending HTTP request to $MIDDLEWARE_URL" }
            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            
            if (response.statusCode() in 200..299) {
                log.info { "!!!!HARSH's " +
                    "Successfully notified middleware about document upload: ${metadata.name}, " +
                    "status=${response.statusCode()}, response=${response.body()}"
                }
                true
            } else {
                log.info { "!!!!HARSH's " +
                    "Middleware API returned non-success status: ${response.statusCode()}, " +
                    "body: ${response.body()}" 
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
     * Build the JSON payload for the API request.
     */
    private fun buildPayload(metadata: DocumentMetadata): String {
        val payload = objectMapper.createObjectNode()
        
        // Generate a unique ID for this document notification
        payload.put("id", UUID.randomUUID().toString())
        
        // Add required fields
        payload.put("name", metadata.name ?: "unknown")
        payload.put("type", metadata.type ?: "unknown")
        payload.put("internal_path", metadata.internalPath ?: "")
        payload.put("doc_updated_at", metadata.docUpdatedAt ?: "")
        payload.put("file_size_bytes", metadata.fileSizeBytes)
        payload.put("source_path", metadata.sourcePath ?: "")
        
        return objectMapper.writeValueAsString(payload)
    }

    /**
     * Build the HTTP request with all required headers.
     */
    private fun buildRequest(payload: String): HttpRequest {
        return HttpRequest.newBuilder()
            .uri(URI.create(MIDDLEWARE_URL))
            .header("Accept", HEADER_ACCEPT)
            .header("Accept-Language", HEADER_ACCEPT_LANGUAGE)
            .header("CSRF", HEADER_CSRF)
            .header("Cache-Control", HEADER_CACHE_CONTROL)
            .header("DNT", HEADER_DNT)
            .header("Pragma", HEADER_PRAGMA)
            .header("Referer", HEADER_REFERER)
            .header("Sec-Fetch-Dest", HEADER_SEC_FETCH_DEST)
            .header("Sec-Fetch-Mode", HEADER_SEC_FETCH_MODE)
            .header("Sec-Fetch-Site", HEADER_SEC_FETCH_SITE)
            .header("User-Agent", HEADER_USER_AGENT)
            .header("sec-ch-ua", HEADER_SEC_CH_UA)
            .header("sec-ch-ua-mobile", HEADER_SEC_CH_UA_MOBILE)
            .header("sec-ch-ua-platform", HEADER_SEC_CH_UA_PLATFORM)
            .header("Authorization", HEADER_AUTHORIZATION)
            .header("USERID", HEADER_USERID)
            .header("Content-Type", HEADER_CONTENT_TYPE)
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .timeout(Duration.ofSeconds(30))
            .build()
    }
}


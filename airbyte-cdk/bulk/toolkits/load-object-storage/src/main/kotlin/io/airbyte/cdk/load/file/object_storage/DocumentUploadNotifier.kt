/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file.object_storage

import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Service responsible for notifying the middleware API about uploaded documents.
 * Notifications are sent asynchronously and failures do not affect the sync.
 * 
 * @param datasourceId Optional datasource ID for middleware integration (injected via DI)
 */
@Singleton
class DocumentUploadNotifier(
    @Named("datasourceId") private val datasourceId: String? = null
) {
    private val log = KotlinLogging.logger {}
    private val apiClient = MiddlewareApiClient(datasourceId)
    private val scope = CoroutineScope(Dispatchers.IO)

    /**
     * Notify the middleware service about a completed document upload.
     * This is non-blocking and failures are logged but do not propagate.
     */
    fun notifyUploadComplete(metadata: DocumentMetadata?) {
        if (metadata == null) {
            log.debug { "Skipping middleware notification - no metadata available" }
            return
        }

        log.debug { 
            "Preparing middleware notification: " +
            "name=${metadata.name}, type=${metadata.type}, " +
            "internalPath=${metadata.internalPath}"
        }

        // Launch async notification - don't block the upload pipeline
        scope.launch {
            try {
                apiClient.notifyDocumentUpload(metadata)
            } catch (e: Exception) {
                log.error(e) { 
                    "Unexpected error in document upload notification. " +
                    "This should have been caught by the API client."
                }
            }
        }
    }
}


/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file.object_storage

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DocumentUploadNotifierTest {

    @Test
    fun `test notifyUploadComplete with null metadata`() {
        val notifier = DocumentUploadNotifier()
        
        // Should not throw exception
        assertDoesNotThrow {
            notifier.notifyUploadComplete(null)
        }
    }

    @Test
    fun `test notifyUploadComplete with valid metadata`() {
        val notifier = DocumentUploadNotifier()
        val metadata = DocumentMetadata(
            name = "test.pdf",
            type = "pdf",
            sourcePath = "gong/test.pdf",
            internalPath = "s3://bucket/path/test.jsonl",
            fileSizeBytes = 1000L,
            docUpdatedAt = "2025-10-31T15:42:30.123456Z"
        )

        // Should not throw exception
        assertDoesNotThrow {
            notifier.notifyUploadComplete(metadata)
        }

        // Give it a moment for async operation to start
        runBlocking {
            delay(100)
        }
    }

    @Test
    fun `test notifyUploadComplete with incomplete metadata`() {
        val notifier = DocumentUploadNotifier()
        val metadata = DocumentMetadata(
            name = "test.pdf",
            type = "pdf",
            // Missing sourcePath - doesn't have essential metadata
            internalPath = "s3://bucket/path/test.jsonl",
            fileSizeBytes = 1000L
        )

        // Should not throw exception even with incomplete metadata
        assertDoesNotThrow {
            notifier.notifyUploadComplete(metadata)
        }
    }

    @Test
    fun `test multiple consecutive notifications`() {
        val notifier = DocumentUploadNotifier()
        val metadata1 = DocumentMetadata(
            name = "test1.pdf",
            type = "pdf",
            sourcePath = "gong/test1.pdf",
            internalPath = "s3://bucket/path/test1.jsonl",
            fileSizeBytes = 1000L
        )
        val metadata2 = DocumentMetadata(
            name = "test2.pdf",
            type = "pdf",
            sourcePath = "gong/test2.pdf",
            internalPath = "s3://bucket/path/test2.jsonl",
            fileSizeBytes = 2000L
        )

        // Should handle multiple notifications without issues
        assertDoesNotThrow {
            notifier.notifyUploadComplete(metadata1)
            notifier.notifyUploadComplete(metadata2)
        }
    }
}


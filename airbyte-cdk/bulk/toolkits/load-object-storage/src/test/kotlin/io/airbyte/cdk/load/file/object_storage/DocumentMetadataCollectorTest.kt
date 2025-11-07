/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file.object_storage

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.cdk.load.message.DestinationRecordSource
import io.airbyte.cdk.load.message.Meta
import io.airbyte.cdk.load.state.CheckpointId
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID

class DocumentMetadataCollectorTest {
    private val objectMapper = ObjectMapper()

    private fun createMockRecord(jsonString: String, sizeBytes: Long = 1000L): DestinationRecordRaw {
        val jsonNode = objectMapper.readTree(jsonString)
        val stream = mockk<DestinationStream> {
            every { schema } returns ObjectType(linkedMapOf())
            every { airbyteValueProxyFieldAccessors } returns emptyArray()
        }
        val rawData = mockk<DestinationRecordSource> {
            every { asJsonRecord(any()) } returns jsonNode
            every { emittedAtMs } returns System.currentTimeMillis()
            every { sourceMeta } returns Meta()
            every { fileReference } returns null
        }
        return DestinationRecordRaw(
            stream = stream,
            rawData = rawData,
            serializedSizeBytes = sizeBytes,
            checkpointId = null,
            airbyteRawId = UUID.randomUUID()
        )
    }

    @Test
    fun `test extract metadata from record with all fields`() {
        val collector = DocumentMetadataCollector()
        val jsonData = """
            {
                "_airbyte_raw_id": "019a1176-9d81-7a34-bf23-e67ef314c1b6",
                "_airbyte_data": {
                    "content": "# Sample Document",
                    "document_key": "PA-Notification-Weight-Loss.pdf",
                    "_ab_source_file_last_modified": "2025-09-02T15:43:49.000000Z",
                    "_ab_source_file_url": "PA-Notification-Weight-Loss.pdf"
                }
            }
        """.trimIndent()

        val record = createMockRecord(jsonData, 500L)
        collector.collectFromRecord(record)

        val metadata = collector.getMetadata()
        assertNotNull(metadata)
        assertEquals("PA-Notification-Weight-Loss.pdf", metadata?.name)
        assertEquals("pdf", metadata?.type)
        assertEquals("PA-Notification-Weight-Loss.pdf", metadata?.sourcePath)
        // Timestamp is normalized by ZonedDateTime.parse() which removes trailing zeros
        assertEquals("2025-09-02T15:43:49Z", metadata?.docUpdatedAt)
        assertEquals(500L, metadata?.fileSizeBytes)
    }

    @Test
    fun `test extract metadata with different file extension`() {
        val collector = DocumentMetadataCollector()
        val jsonData = """
            {
                "_airbyte_data": {
                    "document_key": "test-document.jsonl",
                    "_ab_source_file_url": "gong/test-document.jsonl",
                    "_ab_source_file_last_modified": "2025-10-31T15:42:30.123456Z"
                }
            }
        """.trimIndent()

        val record = createMockRecord(jsonData, 750L)
        collector.collectFromRecord(record)

        val metadata = collector.getMetadata()
        assertNotNull(metadata)
        assertEquals("test-document.jsonl", metadata?.name)
        assertEquals("jsonl", metadata?.type)
        assertEquals("gong/test-document.jsonl", metadata?.sourcePath)
    }

    @Test
    fun `test metadata accumulates file size across records`() {
        val collector = DocumentMetadataCollector()
        val jsonData = """
            {
                "_airbyte_data": {
                    "document_key": "test.pdf",
                    "_ab_source_file_url": "test.pdf"
                }
            }
        """.trimIndent()

        val record1 = createMockRecord(jsonData, 500L)
        val record2 = createMockRecord(jsonData, 300L)
        val record3 = createMockRecord(jsonData, 200L)

        collector.collectFromRecord(record1)
        collector.collectFromRecord(record2)
        collector.collectFromRecord(record3)

        val metadata = collector.getMetadata()
        assertEquals(1000L, metadata?.fileSizeBytes)
    }

    @Test
    fun `test setInternalPath updates metadata`() {
        val collector = DocumentMetadataCollector()
        val jsonData = """
            {
                "_airbyte_data": {
                    "document_key": "test.pdf",
                    "_ab_source_file_url": "test.pdf"
                }
            }
        """.trimIndent()

        val record = createMockRecord(jsonData, 500L)
        collector.collectFromRecord(record)
        collector.setInternalPath("s3://my-bucket/path/to/test.jsonl")

        val metadata = collector.getMetadata()
        assertEquals("s3://my-bucket/path/to/test.jsonl", metadata?.internalPath)
    }

    @Test
    fun `test hasEssentialMetadata returns true when required fields present`() {
        val metadata = DocumentMetadata(
            name = "test.pdf",
            type = "pdf",
            sourcePath = "gong/test.pdf",
            fileSizeBytes = 100L
        )

        assertTrue(metadata.hasEssentialMetadata())
    }

    @Test
    fun `test hasEssentialMetadata returns false when name missing`() {
        val metadata = DocumentMetadata(
            name = null,
            sourcePath = "gong/test.pdf"
        )

        assertFalse(metadata.hasEssentialMetadata())
    }

    @Test
    fun `test hasEssentialMetadata returns false when sourcePath missing`() {
        val metadata = DocumentMetadata(
            name = "test.pdf",
            sourcePath = null
        )

        assertFalse(metadata.hasEssentialMetadata())
    }

    @Test
    fun `test extract metadata when _airbyte_data is missing`() {
        val collector = DocumentMetadataCollector()
        val jsonData = """
            {
                "_airbyte_raw_id": "019a1176-9d81-7a34-bf23-e67ef314c1b6"
            }
        """.trimIndent()

        val record = createMockRecord(jsonData)
        collector.collectFromRecord(record)

        val metadata = collector.getMetadata()
        assertNotNull(metadata)
        assertNull(metadata?.name)
        assertNull(metadata?.type)
        assertNull(metadata?.sourcePath)
    }

    @Test
    fun `test extract file extension from various filenames`() {
        val collector = DocumentMetadataCollector()
        
        // PDF
        val pdfData = """{"_airbyte_data": {"document_key": "file.pdf"}}"""
        val pdfRecord = createMockRecord(pdfData)
        collector.collectFromRecord(pdfRecord)
        assertEquals("pdf", collector.getMetadata()?.type)

        // JSONL with path
        val jsonlCollector = DocumentMetadataCollector()
        val jsonlData = """{"_airbyte_data": {"document_key": "path/to/file.jsonl"}}"""
        val jsonlRecord = createMockRecord(jsonlData)
        jsonlCollector.collectFromRecord(jsonlRecord)
        assertEquals("jsonl", jsonlCollector.getMetadata()?.type)

        // TXT
        val txtCollector = DocumentMetadataCollector()
        val txtData = """{"_airbyte_data": {"document_key": "document.txt"}}"""
        val txtRecord = createMockRecord(txtData)
        txtCollector.collectFromRecord(txtRecord)
        assertEquals("txt", txtCollector.getMetadata()?.type)
    }
}


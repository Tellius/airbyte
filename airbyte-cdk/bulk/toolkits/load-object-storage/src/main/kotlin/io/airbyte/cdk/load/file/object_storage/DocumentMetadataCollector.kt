/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.file.object_storage

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.ZonedDateTime
import java.time.format.DateTimeParseException

/**
 * Data class representing document metadata extracted from source records.
 * Each record represents a separate document (e.g., different PDF files).
 */
data class DocumentMetadata(
    val name: String? = null,
    val type: String? = null,
    val sourcePath: String? = null,
    val docUpdatedAt: String? = null,
    val fileSizeBytes: Long = 0L,
    val internalPath: String? = null
) {
    /**
     * Check if essential metadata has been collected.
     */
    fun hasEssentialMetadata(): Boolean {
        return !name.isNullOrBlank() && !sourcePath.isNullOrBlank()
    }
}

/**
 * Utility to extract document metadata from individual Airbyte records.
 * Note: In a JSONL file, each line/record represents a DIFFERENT source document.
 */
object DocumentMetadataExtractor {
    private val log = KotlinLogging.logger {}

    /**
     * Extract metadata from a single record.
     * Each record contains information about one source document.
     */
    fun extractFromRecord(record: DestinationRecordRaw, internalPath: String? = null): DocumentMetadata? {
        return try {
            val jsonNode = record.asJsonRecord()
            val metadata = extractMetadata(jsonNode, record.serializedSizeBytes)
            
            // Set internal path if provided (the JSONL file path where this record is stored)
            val finalMetadata = if (internalPath != null) {
                metadata.copy(internalPath = internalPath)
            } else {
                metadata
            }
            
            log.info {"!!!!HARSH's " +
                "Extracted document metadata from record: " +
                "name=${finalMetadata.name}, type=${finalMetadata.type}, " +
                "sourcePath=${finalMetadata.sourcePath}, docUpdatedAt=${finalMetadata.docUpdatedAt}, " +
                "fileSizeBytes=${finalMetadata.fileSizeBytes}, " +
                "internalPath=${finalMetadata.internalPath}, " +
                "hasEssential=${finalMetadata.hasEssentialMetadata()}"
            }
            
            finalMetadata
        } catch (e: Exception) {
            log.warn(e) { "Failed to extract document metadata from record" }
            null
        }
    }

    /**
     * Extract metadata fields from the JSON record.
     */
    private fun extractMetadata(jsonNode: JsonNode, recordSizeBytes: Long): DocumentMetadata {
        // Look for _airbyte_data field which contains the actual source document info
        val airbyteData = jsonNode.get("_airbyte_data")
        
        if (airbyteData == null || airbyteData.isNull) {
            log.debug { "No _airbyte_data field found in record, trying root level" }
            // Fallback: try extracting from root level
            return extractFromRootLevel(jsonNode, recordSizeBytes)
        }

        // Extract document_key (file name)
        val documentKey = airbyteData.get("document_key")?.asText()
        
        // Extract source file URL
        val sourceFileUrl = airbyteData.get("_ab_source_file_url")?.asText()
        
        // Determine name and type
        val name = documentKey ?: sourceFileUrl
        val type = name?.let { extractFileExtension(it) }
        
        // Extract source path (prefer _ab_source_file_url)
        val sourcePath = sourceFileUrl ?: documentKey
        
        // Extract last modified timestamp
        val lastModified = airbyteData.get("_ab_source_file_last_modified")?.asText()
        val docUpdatedAt = lastModified?.let { parseAndFormatTimestamp(it) }
        
        log.debug { 
            "Extracted metadata from _airbyte_data: documentKey=$documentKey, " +
            "sourceFileUrl=$sourceFileUrl, lastModified=$lastModified"
        }
        
        return DocumentMetadata(
            name = name,
            type = type,
            sourcePath = sourcePath,
            docUpdatedAt = docUpdatedAt,
            fileSizeBytes = recordSizeBytes
        )
    }
    
    /**
     * Fallback: Extract metadata from root level if _airbyte_data is not present.
     */
    private fun extractFromRootLevel(jsonNode: JsonNode, recordSizeBytes: Long): DocumentMetadata {
        val documentKey = jsonNode.get("document_key")?.asText()
        val sourceFileUrl = jsonNode.get("_ab_source_file_url")?.asText()
        val name = documentKey ?: sourceFileUrl
        val type = name?.let { extractFileExtension(it) }
        val sourcePath = sourceFileUrl ?: documentKey
        val lastModified = jsonNode.get("_ab_source_file_last_modified")?.asText()
        val docUpdatedAt = lastModified?.let { parseAndFormatTimestamp(it) }
        
        return DocumentMetadata(
            name = name,
            type = type,
            sourcePath = sourcePath,
            docUpdatedAt = docUpdatedAt,
            fileSizeBytes = recordSizeBytes
        )
    }

    /**
     * Extract file extension from filename.
     */
    private fun extractFileExtension(filename: String): String? {
        val lastDot = filename.lastIndexOf('.')
        return if (lastDot > 0 && lastDot < filename.length - 1) {
            filename.substring(lastDot + 1).lowercase()
        } else {
            null
        }
    }

    /**
     * Parse and format timestamp to ISO 8601 format.
     * Handles various input formats.
     */
    private fun parseAndFormatTimestamp(timestamp: String): String? {
        return try {
            // Try parsing as ISO 8601 with timezone
            val zonedDateTime = ZonedDateTime.parse(timestamp)
            zonedDateTime.toString()
        } catch (e: DateTimeParseException) {
            try {
                // If parsing fails, try other common formats
                // For now, return the original if it looks valid
                if (timestamp.matches(Regex("\\d{4}-\\d{2}-\\d{2}T.*"))) {
                    timestamp
                } else {
                    log.debug { "Could not parse timestamp: $timestamp" }
                    null
                }
            } catch (e2: Exception) {
                log.debug { "Could not parse timestamp: $timestamp" }
                null
            }
        } catch (e: Exception) {
            log.debug { "Could not parse timestamp: $timestamp" }
            null
        }
    }
}


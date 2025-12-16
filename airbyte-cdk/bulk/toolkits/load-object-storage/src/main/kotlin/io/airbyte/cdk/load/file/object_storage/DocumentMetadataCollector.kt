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
            
            if (finalMetadata.hasEssentialMetadata()) {
                log.info { 
                    "Extracted metadata: name=${finalMetadata.name}, type=${finalMetadata.type}, " +
                    "sourcePath=${finalMetadata.sourcePath}"
                }
            } else {
                log.debug { 
                    "Extracted metadata missing essential fields: name=${finalMetadata.name}, " +
                    "sourcePath=${finalMetadata.sourcePath}"
                }
            }
            
            finalMetadata
        } catch (e: Exception) {
            log.warn(e) { "Failed to extract document metadata from record" }
            null
        }
    }

    /**
     * Extract metadata fields from the JSON record.
     * 
     * Supports both file-based sources (Google Drive, S3) and API-based sources (Gong, Salesforce).
     * 
     * Note: Airbyte destination connectors typically receive data directly at the root level,
     * not wrapped in _airbyte_data. We check both locations for compatibility.
     */
    private fun extractMetadata(jsonNode: JsonNode, recordSizeBytes: Long): DocumentMetadata {
        // Check for _airbyte_data wrapper (used in some contexts)
        val dataNode = jsonNode.get("_airbyte_data")
        
        return if (dataNode != null && !dataNode.isNull) {
            // Data is wrapped in _airbyte_data
            extractFromDataNode(dataNode, recordSizeBytes)
        } else {
            // Data is at root level (most common case for destination connectors)
            extractFromDataNode(jsonNode, recordSizeBytes)
        }
    }
    
    /**
     * Extract metadata from a data node (either root or _airbyte_data).
     * Detects source type and routes to appropriate extraction method.
     */
    private fun extractFromDataNode(dataNode: JsonNode, recordSizeBytes: Long): DocumentMetadata {
        // Check for API-based sources first (Gong, Salesforce, etc.)
        val apiMetadata = tryExtractFromApiSource(dataNode, recordSizeBytes)
        if (apiMetadata != null && apiMetadata.hasEssentialMetadata()) {
            return apiMetadata
        }
        
        // Fall back to file-based extraction (Google Drive, S3, etc.)
        return extractFromFileSource(dataNode, recordSizeBytes)
    }
    
    /**
     * Extract metadata from file-based sources (Google Drive, S3, etc.).
     */
    private fun extractFromFileSource(dataNode: JsonNode, recordSizeBytes: Long): DocumentMetadata {
        val documentKey = dataNode.get("document_key")?.asText()
        val sourceFileUrl = dataNode.get("_ab_source_file_url")?.asText()
        val lastModified = dataNode.get("_ab_source_file_last_modified")?.asText()
        
        // Determine name and type
        val name = documentKey ?: sourceFileUrl
        val type = name?.let { extractFileExtension(it) }
        
        // Extract source path (prefer _ab_source_file_url)
        val sourcePath = sourceFileUrl ?: documentKey
        
        val docUpdatedAt = lastModified?.let { parseAndFormatTimestamp(it) }
        
        log.debug { 
            "File-based extraction: documentKey=$documentKey, sourceFileUrl=$sourceFileUrl"
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
     * Try to extract metadata from API-based sources (Gong, Salesforce, etc.).
     * Returns null if unable to extract essential metadata.
     */
    private fun tryExtractFromApiSource(dataNode: JsonNode, recordSizeBytes: Long): DocumentMetadata? {
        // Check for Gong-specific structure: metaData object with title, url, started
        val metaData = dataNode.get("metaData")
        if (metaData != null && !metaData.isNull) {
            val title = metaData.get("title")?.asText()
            val url = metaData.get("url")?.asText()
            val started = metaData.get("started")?.asText()
            
            // If we have both title and url, this is Gong
            if (!title.isNullOrBlank() && !url.isNullOrBlank()) {
                log.debug { "Detected Gong call: title=$title" }
                val docUpdatedAt = started?.let { parseAndFormatTimestamp(it) }
                
                return DocumentMetadata(
                    name = title,
                    type = "gong_call",
                    sourcePath = url,
                    docUpdatedAt = docUpdatedAt,
                    fileSizeBytes = recordSizeBytes
                )
            }
        }
        
        // Try generic API source extraction (Salesforce, etc.)
        return tryExtractFromGenericApiSource(dataNode, recordSizeBytes)
    }
    
    /**
     * Try generic extraction for API sources that don't match specific patterns.
     * Tries common field names for name, path, and timestamp.
     * Returns null if unable to extract essential metadata.
     */
    private fun tryExtractFromGenericApiSource(dataNode: JsonNode, recordSizeBytes: Long): DocumentMetadata? {
        // Try to find a name field
        val name = dataNode.get("name")?.asText()
            ?: dataNode.get("title")?.asText()
            ?: dataNode.get("id")?.asText()
        
        // Try to find a path/URL field
        val sourcePath = dataNode.get("url")?.asText()
            ?: dataNode.get("uri")?.asText()
            ?: dataNode.get("path")?.asText()
            ?: dataNode.get("link")?.asText()
        
        // Try to find a timestamp field
        val timestamp = dataNode.get("updated_at")?.asText()
            ?: dataNode.get("updatedAt")?.asText()
            ?: dataNode.get("modified_at")?.asText()
            ?: dataNode.get("modifiedAt")?.asText()
            ?: dataNode.get("created_at")?.asText()
            ?: dataNode.get("createdAt")?.asText()
        
        val docUpdatedAt = timestamp?.let { parseAndFormatTimestamp(it) }
        
        // Determine document type based on available data
        val type = determineDocumentType(dataNode, name)
        
        log.debug { "Generic API extraction: name=$name, sourcePath=$sourcePath, type=$type" }
        
        val metadata = DocumentMetadata(
            name = name,
            type = type,
            sourcePath = sourcePath,
            docUpdatedAt = docUpdatedAt,
            fileSizeBytes = recordSizeBytes
        )
        
        // Return null if we couldn't extract essential fields
        return if (metadata.hasEssentialMetadata()) metadata else null
    }
    
    /**
     * Determine document type based on the data structure.
     * Returns source-specific types where possible.
     */
    private fun determineDocumentType(airbyteData: JsonNode, name: String?): String {
        // Check for Gong call recording indicators
        if (airbyteData.has("metaData") && airbyteData.get("metaData")?.has("title") == true) {
            return "gong_call"
        }
        
        // Check for Salesforce object indicators
        if (airbyteData.has("attributes") && airbyteData.get("attributes")?.has("type") == true) {
            val sfType = airbyteData.get("attributes")?.get("type")?.asText()
            if (sfType != null) {
                return "salesforce_${sfType.lowercase()}"
            }
        }
        
        // Try to extract file extension if name has one
        if (name != null) {
            val extension = extractFileExtension(name)
            if (extension != null) {
                return extension
            }
        }
        
        // Default fallback
        return "api_record"
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


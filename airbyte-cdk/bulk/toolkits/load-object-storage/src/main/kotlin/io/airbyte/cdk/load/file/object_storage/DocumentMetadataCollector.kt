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
    val internalPath: String? = null,
    /** 
     * The source file ID (e.g., Google Drive file ID).
     * Available when using file transfer mode from sources like Google Drive.
     */
    val fileId: String? = null,
    /**
     * The full source URI/redirect link (e.g., https://drive.google.com/open?id=FILE_ID).
     * Available when using file transfer mode from sources like Google Drive.
     */
    val sourceUri: String? = null,
    /** MIME type of the file (all sources: _ab_source_file_content_type). */
    val contentType: String? = null,
    /** File creation timestamp (GDrive, GCS, Azure: _ab_source_file_created_at). */
    val createdAt: String? = null,
    /** File owner email/name (GDrive: _ab_source_file_owner). */
    val owner: String? = null,
    /** User who last modified the file (GDrive: _ab_source_file_last_modified_by). */
    val lastModifiedBy: String? = null,
    /** Whether the file is shared (GDrive: _ab_source_file_shared). */
    val shared: Boolean? = null,
    /**
     * Actual file size in bytes from the source system (_ab_source_file_size).
     * Preferred over fileSizeBytes (which is the serialized record size).
     */
    val fileSizeFromSource: Long? = null,
    /** Storage class (S3, GCS: _ab_source_file_storage_class). */
    val storageClass: String? = null,
    /** Azure access tier (Azure: _ab_source_file_access_tier). */
    val accessTier: String? = null,
    /** Virtual folder/prefix path (all sources: _ab_source_file_prefix_path). */
    val prefixPath: String? = null,
    /** S3 user-defined metadata, serialized as JSON string (_ab_source_file_user_metadata). */
    val userMetadata: String? = null,
    /** S3 object tags, serialized as JSON string (_ab_source_file_object_tags). */
    val objectTags: String? = null,
    /** Azure blob metadata, serialized as JSON string (_ab_source_file_blob_metadata). */
    val blobMetadata: String? = null,
    /** Azure container metadata, serialized as JSON string (_ab_source_file_container_metadata). */
    val containerMetadata: String? = null,
    /** GCS custom metadata, serialized as JSON string (_ab_source_file_custom_metadata). */
    val customMetadata: String? = null,
    /** SharePoint site name (_ab_source_file_site_name). */
    val siteName: String? = null,
    /** SharePoint document library name (_ab_source_file_library_name). */
    val libraryName: String? = null,
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
     * Extract metadata from file-based sources (Google Drive, S3, GCS, Azure, etc.).
     */
    private fun extractFromFileSource(dataNode: JsonNode, recordSizeBytes: Long): DocumentMetadata {
        val documentKey = dataNode.get("document_key")?.asText()
        val sourceFileUrl = dataNode.get("_ab_source_file_url")?.asText()
        val lastModified = dataNode.get("_ab_source_file_last_modified")?.asText()

        // File transfer mode fields (available from Google Drive, SharePoint, etc.)
        // In File Transfer Mode: "id" and "source_uri"
        // In Parsing Mode with custom stream: "_ab_source_file_id" and "_ab_source_file_redirect_url"
        val fileIdFromTransfer = dataNode.get("id")?.asText()
        val fileIdFromParsing = dataNode.get("_ab_source_file_id")?.asText()
        val fileId = fileIdFromTransfer ?: fileIdFromParsing

        val sourceUriFromTransfer = dataNode.get("source_uri")?.asText()
        val sourceUriFromParsing = dataNode.get("_ab_source_file_redirect_url")?.asText()
        val sourceUri = sourceUriFromTransfer ?: sourceUriFromParsing

        val fileName = dataNode.get("file_name")?.asText()
        val updatedAt = dataNode.get("updated_at")?.asText()

        // Determine name (prefer file_name from file transfer mode, then document_key, then _ab_source_file_url)
        val name = fileName ?: documentKey ?: sourceFileUrl
        val type = name?.let { extractFileExtension(it) }

        // Extract source path (prefer _ab_source_file_url for backwards compatibility)
        val sourcePath = sourceFileUrl ?: documentKey ?: fileName

        // Use updated_at from file transfer mode if available, otherwise fall back to _ab_source_file_last_modified
        val docUpdatedAt = (updatedAt ?: lastModified)?.let { parseAndFormatTimestamp(it) }

        // --- New metadata fields added by custom source connectors ---

        val contentType = dataNode.get("_ab_source_file_content_type")?.asText()
        val createdAt = dataNode.get("_ab_source_file_created_at")?.asText()
        val owner = dataNode.get("_ab_source_file_owner")?.asText()
        val lastModifiedBy = dataNode.get("_ab_source_file_last_modified_by")?.asText()
        val sharedNode = dataNode.get("_ab_source_file_shared")
        val shared = if (sharedNode != null && !sharedNode.isNull) sharedNode.asBoolean() else null

        // Prefer actual file size from source (_ab_source_file_size) over serialized record size
        val fileSizeFromSourceNode = dataNode.get("_ab_source_file_size")
        val fileSizeFromSource = if (fileSizeFromSourceNode != null && !fileSizeFromSourceNode.isNull && fileSizeFromSourceNode.isNumber) {
            fileSizeFromSourceNode.asLong()
        } else {
            null
        }
        val effectiveFileSizeBytes = fileSizeFromSource ?: recordSizeBytes

        val storageClass = dataNode.get("_ab_source_file_storage_class")?.asText()
        val accessTier = dataNode.get("_ab_source_file_access_tier")?.asText()
        val prefixPath = dataNode.get("_ab_source_file_prefix_path")?.asText()

        // These are JSON strings serialized by the source connector
        val userMetadata = dataNode.get("_ab_source_file_user_metadata")?.asText()
        val objectTags = dataNode.get("_ab_source_file_object_tags")?.asText()
        val blobMetadata = dataNode.get("_ab_source_file_blob_metadata")?.asText()
        val containerMetadata = dataNode.get("_ab_source_file_container_metadata")?.asText()
        val customMetadata = dataNode.get("_ab_source_file_custom_metadata")?.asText()

        // SharePoint-specific fields
        val siteName = dataNode.get("_ab_source_file_site_name")?.asText()
        val libraryName = dataNode.get("_ab_source_file_library_name")?.asText()

        return DocumentMetadata(
            name = name,
            type = type,
            sourcePath = sourcePath,
            docUpdatedAt = docUpdatedAt,
            fileSizeBytes = effectiveFileSizeBytes,
            fileId = fileId,
            sourceUri = sourceUri,
            contentType = contentType,
            createdAt = createdAt,
            owner = owner,
            lastModifiedBy = lastModifiedBy,
            shared = shared,
            fileSizeFromSource = fileSizeFromSource,
            storageClass = storageClass,
            accessTier = accessTier,
            prefixPath = prefixPath,
            userMetadata = userMetadata,
            objectTags = objectTags,
            blobMetadata = blobMetadata,
            containerMetadata = containerMetadata,
            customMetadata = customMetadata,
            siteName = siteName,
            libraryName = libraryName,
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


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
        // Slack first: its records share top-level field names (`name`, `id`) with
        // the generic API extractor, so the generic extractor would otherwise pull
        // the wrong fields for channels/users.
        val slackMetadata = tryExtractFromSlackSource(dataNode, recordSizeBytes)
        if (slackMetadata != null && slackMetadata.hasEssentialMetadata()) {
            return slackMetadata
        }

        // Granola: same as Slack — only `detailed_notes` records become documents;
        // the lighter `notes` index stream is skipped (used by airflow extractor
        // as substream enrichment only).
        val granolaMetadata = tryExtractFromGranolaSource(dataNode, recordSizeBytes)
        if (granolaMetadata != null && granolaMetadata.hasEssentialMetadata()) {
            return granolaMetadata
        }

        // Check for API-based sources first (Gong, Salesforce, etc.)
        val apiMetadata = tryExtractFromApiSource(dataNode, recordSizeBytes)
        if (apiMetadata != null && apiMetadata.hasEssentialMetadata()) {
            return apiMetadata
        }

        // Fall back to file-based extraction (Google Drive, S3, etc.)
        return extractFromFileSource(dataNode, recordSizeBytes)
    }

    /** Slack message subtypes that are pure system events; never useful for retrieval. */
    private val SLACK_SYSTEM_MESSAGE_SUBTYPES = setOf(
        "channel_join", "channel_leave",
        "channel_archive", "channel_unarchive",
        "channel_topic", "channel_purpose", "channel_name",
        "bot_add", "bot_remove",
    )

    /**
     * Extract document metadata from Slack records (source-slack).
     *
     * Only `channel_messages` thread parents notify middleware. `channels` and
     * `users` records ARE still written to S3 as JSONL (Airbyte does that
     * regardless of middleware notification), but they aren't documents in
     * their own right — the airflow SlackExtractor reads those sibling JSONLs
     * as lookup tables to enrich each thread document with channel context
     * (name/topic/purpose) and per-message speaker info (real_name/title).
     *
     * Mirrors the Gong shape: one document per conversational unit (a thread
     * for Slack, a call for Gong), with all participant/context data baked in
     * — no separate user-card or channel-card documents.
     *
     * Filters on the message branch:
     *  - skip thread replies (thread_ts != null && thread_ts != ts) so the
     *    extractor's per-thread aggregation has a single doc_id to attach
     *    replies to
     *  - skip system event subtypes (channel_join, leave, topic, etc.)
     */
    private fun tryExtractFromSlackSource(dataNode: JsonNode, recordSizeBytes: Long): DocumentMetadata? {
        val id = dataNode.get("id")?.asText()

        // Channel records: don't notify middleware — the channel data is used by
        // the airflow extractor as enrichment for thread documents only.
        if (dataNode.get("is_channel")?.asBoolean() == true && id?.startsWith("C") == true) {
            return null
        }

        // User records: same — enrichment-only, never their own document.
        if (id?.startsWith("U") == true && dataNode.has("team_id") && !dataNode.has("channel_id")) {
            return null
        }

        // ─── channel_messages (thread parents and standalone) ────────────────
        val type = dataNode.get("type")?.asText()
        val channelId = dataNode.get("channel_id")?.asText()
        val ts = dataNode.get("ts")?.asText()
        if (type == "message" && !channelId.isNullOrBlank() && !ts.isNullOrBlank()) {
            // Skip replies — the SlackExtractor aggregates them into the parent's document.
            val threadTs = dataNode.get("thread_ts")?.asText()
            if (!threadTs.isNullOrBlank() && threadTs != ts) return null

            // Skip system event subtypes (channel_join, leave, topic changes, etc.)
            val subtype = dataNode.get("subtype")?.asText()
            if (subtype != null && subtype in SLACK_SYSTEM_MESSAGE_SUBTYPES) return null

            val text = dataNode.get("text")?.asText().orEmpty()
            val nameSnippet = if (text.isNotBlank()) text.take(80) else "Message $ts"

            // Canonical Slack message permalink: archives/{channel}/p{ts with dot removed}
            val tsCompact = ts.replace(".", "")
            val sourcePath = "https://app.slack.com/archives/$channelId/p$tsCompact"

            val docUpdatedAt = ts.toDoubleOrNull()?.let {
                java.time.Instant.ofEpochMilli((it * 1000).toLong()).toString()
            }

            // type marker reflects whether this parent has replies (becomes a thread doc)
            val docType = if (!threadTs.isNullOrBlank() && threadTs == ts) "slack_thread" else "slack_message"

            log.debug { "Detected Slack message: ts=$ts channel=$channelId type=$docType" }
            return DocumentMetadata(
                name = nameSnippet,
                type = docType,
                sourcePath = sourcePath,
                docUpdatedAt = docUpdatedAt,
                fileSizeBytes = recordSizeBytes,
            )
        }

        return null
    }
    
    /**
     * Extract document metadata from Granola records (source-granola).
     *
     * Granola emits two streams:
     *   - `notes`         — light index (id, title, owner, created_at). Records flow
     *                       to S3 but DON'T notify middleware; the airflow
     *                       GranolaExtractor uses these as substream parents only.
     *   - `detailed_notes` — rich per-note (summary_text, summary_markdown,
     *                       transcript[], attendees[], calendar_event, ...). One
     *                       document per record.
     *
     * Discrimination: detailed_notes have at least one of `summary_markdown`,
     * `summary_text`, `transcript`, or `attendees`. The `notes` index records
     * never have those. We branch on field presence rather than stream name
     * because Airbyte doesn't propagate the stream name into the data node
     * uniformly across CDK versions.
     *
     * Mirrors the Gong/Slack pattern: one document per conversational unit
     * (a meeting), with all participant/context data baked in.
     */
    private fun tryExtractFromGranolaSource(dataNode: JsonNode, recordSizeBytes: Long): DocumentMetadata? {
        // Granola records always have object="note" (per manifest schema). Combined
        // with the id-format check below, this protects against false positives
        // from other API connectors.
        val objectType = dataNode.get("object")?.asText()
        if (objectType != "note") return null

        val id = dataNode.get("id")?.asText() ?: return null

        // Skip the light `notes` index stream: no rich fields → enrichment-only.
        val hasRichContent = dataNode.has("summary_markdown") ||
            dataNode.has("summary_text") ||
            dataNode.has("transcript") ||
            dataNode.has("attendees") ||
            dataNode.has("updated_at")
        if (!hasRichContent) return null

        // Title may be null on minimal records — fall back to "Meeting <id>".
        val title = dataNode.get("title")?.takeIf { !it.isNull }?.asText()
        val nameValue = if (!title.isNullOrBlank()) title else "Meeting $id"

        // source_path: prefer Granola's per-note permalink. The custom source image
        // declares `web_url` in the manifest schema, so it survives the CDK (which
        // strips undeclared fields). `url`/`share_url`/`permalink` are kept as
        // historical aliases; the constructed URL is a last-resort fallback only
        // (never hit in practice, since `web_url` is present on every detailed_note).
        // Ordering matches the airflow GranolaExtractor's _build_source_path().
        val sourcePath = dataNode.get("web_url")?.takeIf { !it.isNull }?.asText()
            ?: dataNode.get("url")?.takeIf { !it.isNull }?.asText()
            ?: dataNode.get("share_url")?.takeIf { !it.isNull }?.asText()
            ?: dataNode.get("permalink")?.takeIf { !it.isNull }?.asText()
            ?: "https://notes.granola.ai/t/$id"

        // Prefer updated_at, fall back to created_at — both are ISO 8601 strings.
        val timestamp = dataNode.get("updated_at")?.takeIf { !it.isNull }?.asText()
            ?: dataNode.get("created_at")?.takeIf { !it.isNull }?.asText()
        val docUpdatedAt = timestamp?.let { parseAndFormatTimestamp(it) }

        log.debug { "Detected Granola detailed_note: id=$id title=$title" }
        return DocumentMetadata(
            name = nameValue,
            type = "granola_note",
            sourcePath = sourcePath,
            docUpdatedAt = docUpdatedAt,
            fileSizeBytes = recordSizeBytes,
        )
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


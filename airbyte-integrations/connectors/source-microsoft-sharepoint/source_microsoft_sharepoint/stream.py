#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Mapping

from airbyte_cdk.sources.file_based.stream import DefaultFileBasedStream
from source_microsoft_sharepoint.utils import MicrosoftSharePointRemoteFile

logger = logging.getLogger("airbyte")


class SharePointStream(DefaultFileBasedStream):
    """
    Custom stream for Microsoft SharePoint that adds document metadata fields to records.

    Fields added on top of the CDK baseline
    (content, document_key, _ab_source_file_url, _ab_source_file_last_modified):

    - _ab_source_file_content_type: MIME type from Graph API (file.mimeType)
    - _ab_source_file_created_at: File creation timestamp (ISO-8601)
    - _ab_source_file_size: File size in bytes
    - _ab_source_file_owner: Creator email (falls back to displayName for guests/service principals)
    - _ab_source_file_last_modified_by: Last modifier email (same fallback)
    - _ab_source_file_site_name: SharePoint site name (parsed from drive.web_url)
    - _ab_source_file_library_name: Document library name (drive.name); null for shared items
    - _ab_source_file_prefix_path: Folder path relative to library root, filename excluded
      (e.g. 'Regulatory/FDA-submissions/2026'). Empty string for files at the library root.

    CDK version: ^6 — transform_record() is available and works correctly.
    """

    def get_json_schema(self) -> Mapping[str, Any]:
        schema = super().get_json_schema()

        if "properties" not in schema:
            schema["properties"] = {}

        schema["properties"]["_ab_source_file_content_type"] = {
            "type": ["string", "null"],
            "description": "MIME type of the file as returned by the Microsoft Graph API",
        }
        schema["properties"]["_ab_source_file_created_at"] = {
            "type": ["string", "null"],
            "description": "File creation timestamp in ISO-8601 format",
        }
        schema["properties"]["_ab_source_file_size"] = {
            "type": ["integer", "null"],
            "description": "File size in bytes",
        }
        schema["properties"]["_ab_source_file_owner"] = {
            "type": ["string", "null"],
            "description": "Email of the file creator (falls back to displayName for guests and service principals)",
        }
        schema["properties"]["_ab_source_file_last_modified_by"] = {
            "type": ["string", "null"],
            "description": "Email of the last modifier (falls back to displayName for guests and service principals)",
        }
        schema["properties"]["_ab_source_file_site_name"] = {
            "type": ["string", "null"],
            "description": "SharePoint site name extracted from the drive URL (e.g. 'MarketingSite')",
        }
        schema["properties"]["_ab_source_file_library_name"] = {
            "type": ["string", "null"],
            "description": "Document library name (drive.name). Null for files from shared items scope.",
        }
        schema["properties"]["_ab_source_file_prefix_path"] = {
            "type": ["string", "null"],
            "description": (
                "Folder path relative to the library root, excluding the filename "
                "(e.g. 'Regulatory/FDA-submissions/2026'). "
                "Empty string for files directly in the library root."
            ),
        }

        return schema

    def transform_record(
        self, record: dict[str, Any], file: MicrosoftSharePointRemoteFile, last_updated: str
    ) -> dict[str, Any]:
        record = super().transform_record(record, file, last_updated)

        record["_ab_source_file_content_type"] = file.content_type
        record["_ab_source_file_created_at"] = file.created_at.isoformat() if file.created_at else None
        record["_ab_source_file_size"] = file.size
        record["_ab_source_file_owner"] = file.author
        record["_ab_source_file_last_modified_by"] = file.last_modified_by
        record["_ab_source_file_site_name"] = file.site_name
        record["_ab_source_file_library_name"] = file.library_name
        record["_ab_source_file_prefix_path"] = file.prefix_path

        logger.info(
            f"[SharePoint] Emitting metadata for {file.uri!r} — "
            f"content_type={file.content_type}, "
            f"created_at={record['_ab_source_file_created_at']}, "
            f"size={file.size}, "
            f"owner={file.author}, "
            f"last_modified_by={file.last_modified_by}, "
            f"site_name={file.site_name!r}, "
            f"library_name={file.library_name!r}, "
            f"prefix_path={file.prefix_path!r}"
        )
        if file.content_type is None:
            logger.warning(f"[SharePoint] _ab_source_file_content_type is null for {file.uri!r}")

        return record

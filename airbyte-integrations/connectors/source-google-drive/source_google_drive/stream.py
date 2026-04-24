#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Mapping

from airbyte_cdk.sources.file_based.stream import DefaultFileBasedStream
from source_google_drive.stream_reader import GoogleDriveRemoteFile

logger = logging.getLogger("airbyte")


class GoogleDriveStream(DefaultFileBasedStream):
    """
    Custom stream for Google Drive that adds document metadata fields to records.

    Fields added on top of the CDK baseline
    (content, document_key, _ab_source_file_url, _ab_source_file_last_modified):

    Existing (carried over from earlier work):
    - _ab_source_file_id: Google Drive file ID
    - _ab_source_file_redirect_url: Open/view URL

    New (document metadata per PRD):
    - _ab_source_file_content_type: MIME type as returned by the Drive API
    - _ab_source_file_created_at: File creation timestamp (ISO-8601)
    - _ab_source_file_owner: Email address of the primary owner
    - _ab_source_file_last_modified_by: Email address of the last editor
    - _ab_source_file_shared: Whether the file is shared (boolean)
    - _ab_source_file_size: File size in bytes (null for Google-native documents)
    - _ab_source_file_prefix_path: Full folder path relative to the configured root
      (e.g. "Projects/Pharma/Oncology/Reports"). Built from the BFS traversal — zero extra API calls.
    """

    def get_json_schema(self) -> Mapping[str, Any]:
        schema = super().get_json_schema()

        if "properties" not in schema:
            schema["properties"] = {}

        schema["properties"]["_ab_source_file_id"] = {
            "type": ["string", "null"],
            "description": "Google Drive file ID",
        }
        schema["properties"]["_ab_source_file_redirect_url"] = {
            "type": ["string", "null"],
            "description": "Full Google Drive redirect URL (open/view link)",
        }
        schema["properties"]["_ab_source_file_content_type"] = {
            "type": ["string", "null"],
            "description": "MIME type of the file as returned by the Google Drive API",
        }
        schema["properties"]["_ab_source_file_created_at"] = {
            "type": ["string", "null"],
            "description": "File creation timestamp in ISO-8601 format",
        }
        schema["properties"]["_ab_source_file_owner"] = {
            "type": ["string", "null"],
            "description": "Email address of the primary owner of the file",
        }
        schema["properties"]["_ab_source_file_last_modified_by"] = {
            "type": ["string", "null"],
            "description": "Email address of the user who last modified the file",
        }
        schema["properties"]["_ab_source_file_shared"] = {
            "type": ["boolean", "null"],
            "description": "Whether the file has been shared with anyone",
        }
        schema["properties"]["_ab_source_file_size"] = {
            "type": ["integer", "null"],
            "description": "File size in bytes. Null for Google-native documents (Docs, Sheets, Slides, Drawings)",
        }
        schema["properties"]["_ab_source_file_prefix_path"] = {
            "type": ["string", "null"],
            "description": (
                "Full folder path relative to the configured root folder "
                "(e.g. 'Projects/Pharma/Oncology/Reports'). "
                "Empty string for files directly in the root folder."
            ),
        }

        return schema

    def transform_record(
        self, record: dict[str, Any], file: GoogleDriveRemoteFile, last_updated: str
    ) -> dict[str, Any]:
        record = super().transform_record(record, file, last_updated)

        record["_ab_source_file_id"] = file.id
        record["_ab_source_file_redirect_url"] = file.url
        record["_ab_source_file_content_type"] = file.original_mime_type
        record["_ab_source_file_created_at"] = file.created_at.isoformat() if file.created_at else None
        record["_ab_source_file_owner"] = file.owner
        record["_ab_source_file_last_modified_by"] = file.last_modified_by
        record["_ab_source_file_shared"] = file.shared
        record["_ab_source_file_size"] = file.size
        record["_ab_source_file_prefix_path"] = file.folder_path

        logger.info(
            f"[GDrive] Emitting metadata for {file.uri!r} — "
            f"id={file.id}, content_type={file.original_mime_type}, "
            f"created_at={record['_ab_source_file_created_at']}, "
            f"last_modified={file.last_modified}, "
            f"last_modified_by={file.last_modified_by}, "
            f"owner={file.owner}, shared={file.shared}, size={file.size}, "
            f"prefix_path={file.folder_path!r}"
        )
        if file.size is None:
            logger.warning(
                f"[GDrive] _ab_source_file_size is null for {file.uri!r} "
                f"(mime_type={file.original_mime_type}). "
                f"Google-native documents do not expose size via the Drive API."
            )

        return record

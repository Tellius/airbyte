# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import json
import logging
import urllib.parse
from typing import Any, Mapping

from airbyte_cdk.sources.file_based.stream import DefaultFileBasedStream
from source_gcs.helpers import GCSRemoteFile

logger = logging.getLogger("airbyte")


class GCSStream(DefaultFileBasedStream):
    """
    Custom stream for GCS that adds source file identification and document metadata fields.

    Existing fields (stable fingerprinting and file reference):
    - _ab_source_file_id: Stable gs://bucket/path/file.ext path for fingerprinting
    - _ab_source_file_redirect_url: Same stable gs:// path for file reference
    - file_name: Clean filename extracted from the GCS path

    New document metadata fields (per PRD):
    - _ab_source_file_content_type: MIME type from blob.content_type
    - _ab_source_file_created_at: Creation timestamp from blob.time_created (ISO-8601)
    - _ab_source_file_size: Object size in bytes from blob.size
    - _ab_source_file_storage_class: GCS storage class (STANDARD, NEARLINE, COLDLINE, ARCHIVE)
    - _ab_source_file_custom_metadata: User-defined metadata as a JSON string

    Note: Stable gs:// paths are used instead of signed URLs because:
    - Signed URLs expire (within 7 days) and are 700+ chars long
    - Stable paths can be used to generate fresh signed URLs downstream
    """

    def get_json_schema(self) -> Mapping[str, Any]:
        """Override to declare all custom GCS fields so the destination does not strip them."""
        schema = super().get_json_schema()

        if "properties" not in schema:
            schema["properties"] = {}

        schema["properties"]["_ab_source_file_id"] = {
            "type": ["string", "null"],
            "description": "Stable GCS file path (gs://bucket/path/file.ext) for fingerprinting",
        }
        schema["properties"]["_ab_source_file_redirect_url"] = {
            "type": ["string", "null"],
            "description": "Stable GCS file path (gs://bucket/path/file.ext) for file reference",
        }
        schema["properties"]["file_name"] = {
            "type": ["string", "null"],
            "description": "Clean filename extracted from the GCS path (e.g., 'document.pdf')",
        }
        schema["properties"]["_ab_source_file_content_type"] = {
            "type": ["string", "null"],
            "description": "MIME type of the GCS object as stored in metadata",
        }
        schema["properties"]["_ab_source_file_created_at"] = {
            "type": ["string", "null"],
            "description": "Object creation timestamp in ISO-8601 format",
        }
        schema["properties"]["_ab_source_file_size"] = {
            "type": ["integer", "null"],
            "description": "Object size in bytes",
        }
        schema["properties"]["_ab_source_file_storage_class"] = {
            "type": ["string", "null"],
            "description": "GCS storage class (e.g., STANDARD, NEARLINE, COLDLINE, ARCHIVE)",
        }
        schema["properties"]["_ab_source_file_custom_metadata"] = {
            "type": ["string", "null"],
            "description": "User-defined metadata attached to the GCS object, serialized as a JSON string",
        }
        schema["properties"]["_ab_source_file_prefix_path"] = {
            "type": ["string", "null"],
            "description": (
                "Virtual folder path derived from the object name by stripping the filename "
                "(e.g., 'Pharma/Sales/Q1'). Empty string for objects at the bucket root."
            ),
        }

        return schema

    def transform_record(self, record: dict[str, Any], file: GCSRemoteFile, last_updated: str) -> dict[str, Any]:
        record[self.ab_last_mod_col] = last_updated
        # Use stable gs:// path for _ab_source_file_url — avoids 700+ char signed URLs in DB
        record[self.ab_file_name_col] = file.id if file.id else (file.displayed_uri if file.displayed_uri else file.uri)

        if file.id:
            record["_ab_source_file_id"] = file.id
            record["_ab_source_file_redirect_url"] = file.id
            record["document_key"] = file.id
        else:
            logger.warning(f"[GCS] file.id is None or empty for uri={file.uri!r} — cannot set _ab_source_file_id")

        try:
            uri_without_query = file.uri.split("?")[0]
            filename = uri_without_query.split("/")[-1]
            clean_filename = urllib.parse.unquote(filename)
            record["file_name"] = clean_filename
        except Exception as e:
            logger.warning(f"[GCS] Failed to extract file_name from URI {file.uri!r}: {e}")

        record["_ab_source_file_content_type"] = file.content_type
        record["_ab_source_file_created_at"] = file.created_at.isoformat() if file.created_at else None
        record["_ab_source_file_size"] = file.size
        record["_ab_source_file_storage_class"] = file.storage_class
        record["_ab_source_file_custom_metadata"] = json.dumps(file.custom_metadata) if file.custom_metadata is not None else None
        record["_ab_source_file_prefix_path"] = file.prefix_path

        logger.info(
            f"[GCS] Emitting metadata for {file.id or file.uri!r} — "
            f"content_type={file.content_type}, size={file.size}, "
            f"storage_class={file.storage_class}, "
            f"created_at={record['_ab_source_file_created_at']}, "
            f"prefix_path={file.prefix_path!r}, "
            f"custom_metadata_keys={list(file.custom_metadata.keys()) if file.custom_metadata else []}"
        )
        if file.content_type is None:
            logger.warning(f"[GCS] _ab_source_file_content_type is null for {file.id or file.uri!r}")
        if file.custom_metadata is None:
            logger.debug(f"[GCS] No custom metadata on object {file.id or file.uri!r}")
        else:
            logger.debug(f"[GCS] Full custom metadata for {file.id or file.uri!r}: {file.custom_metadata}")

        return record

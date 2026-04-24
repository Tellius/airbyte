#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
import logging
from typing import Any, Mapping

from airbyte_cdk.sources.file_based.stream import DefaultFileBasedStream
from source_s3.v4.remote_file import S3RemoteFile

logger = logging.getLogger("airbyte")


class S3Stream(DefaultFileBasedStream):
    """
    Custom stream for S3 that emits document metadata fields per the PRD.

    Fields populated from list_objects_v2 (at listing time, zero extra API calls):
    - _ab_source_file_size: Object size in bytes
    - _ab_source_file_storage_class: S3 storage class
    - _ab_source_file_content_type: MIME type derived from file extension

    Fields populated lazily in open_file() for each file actually processed:
    - _ab_source_file_user_metadata: User-defined x-amz-meta-* headers (JSON string)
    - _ab_source_file_object_tags: S3 object tags (JSON string)
    """

    def get_json_schema(self) -> Mapping[str, Any]:
        """Declare all custom S3 metadata fields so the destination does not strip them."""
        schema = super().get_json_schema()

        if "properties" not in schema:
            schema["properties"] = {}

        schema["properties"]["_ab_source_file_size"] = {
            "type": ["integer", "null"],
            "description": "Object size in bytes from S3 list_objects_v2",
        }
        schema["properties"]["_ab_source_file_storage_class"] = {
            "type": ["string", "null"],
            "description": "S3 storage class (e.g., STANDARD, STANDARD_IA, INTELLIGENT_TIERING, GLACIER)",
        }
        schema["properties"]["_ab_source_file_content_type"] = {
            "type": ["string", "null"],
            "description": "MIME type derived from the file key extension (e.g., application/pdf)",
        }
        schema["properties"]["_ab_source_file_user_metadata"] = {
            "type": ["string", "null"],
            "description": "User-defined object metadata (x-amz-meta-* headers), serialized as a JSON string",
        }
        schema["properties"]["_ab_source_file_object_tags"] = {
            "type": ["string", "null"],
            "description": "S3 object tags as a JSON string (array of {Key, Value} objects)",
        }
        schema["properties"]["_ab_source_file_prefix_path"] = {
            "type": ["string", "null"],
            "description": (
                "Virtual folder path derived from the S3 key by stripping the filename "
                "(e.g., 'Pharma/Sales/Q1'). Empty string for objects at the bucket root."
            ),
        }

        return schema

    def transform_record(self, record: dict[str, Any], file: S3RemoteFile, last_updated: str) -> dict[str, Any]:
        record = super().transform_record(record, file, last_updated)

        record["_ab_source_file_size"] = file.size
        record["_ab_source_file_storage_class"] = file.storage_class
        record["_ab_source_file_content_type"] = file.content_type
        record["_ab_source_file_user_metadata"] = json.dumps(file.user_metadata) if file.user_metadata is not None else None
        record["_ab_source_file_object_tags"] = json.dumps(file.object_tags) if file.object_tags is not None else None
        record["_ab_source_file_prefix_path"] = file.prefix_path

        logger.info(
            f"[S3] Emitting metadata for {file.uri!r} — "
            f"size={file.size}, storage_class={file.storage_class}, "
            f"content_type={file.content_type}, "
            f"prefix_path={file.prefix_path!r}, "
            f"user_metadata={file.user_metadata if file.user_metadata else {}}, "
            f"object_tags={file.object_tags if file.object_tags else []}"
        )
        if file.content_type is None:
            logger.warning(
                f"[S3] _ab_source_file_content_type could not be derived for {file.uri!r} "
                f"— file extension not recognized by mimetypes"
            )

        return record

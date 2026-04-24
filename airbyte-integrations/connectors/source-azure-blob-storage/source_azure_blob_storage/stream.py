#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
import logging
from typing import Any, Iterable, Mapping

from airbyte_cdk.models import AirbyteMessage
from airbyte_cdk.sources.file_based.stream import DefaultFileBasedStream
from source_azure_blob_storage.remote_file import AzureRemoteFile

logger = logging.getLogger("airbyte")


class AzureBlobStream(DefaultFileBasedStream):
    """
    Custom stream for Azure Blob Storage that emits document metadata fields per the PRD.

    CDK ^4 note: DefaultFileBasedStream in CDK 4.x does NOT define a transform_record() hook.
    Records are yielded directly inside read_records_from_slice(). We override that method,
    calling super() one file at a time so we can inject Azure-specific metadata into each
    RECORD-type AirbyteMessage before it is yielded downstream.

    All fields are populated at listing time from blob properties — no extra API calls:
    - _ab_source_file_content_type: MIME type from blob.content_settings.content_type
    - _ab_source_file_created_at: Creation timestamp from blob.creation_time (ISO-8601)
    - _ab_source_file_size: Blob size in bytes from blob.size
    - _ab_source_file_access_tier: Azure access tier (Hot, Cool, Archive)
    - _ab_source_file_blob_metadata: User-defined blob metadata as a JSON string
    - _ab_source_file_prefix_path: Virtual directory path derived from blob name
    - _ab_source_file_container_metadata: Container-level metadata as a JSON string
    """

    def get_json_schema(self) -> Mapping[str, Any]:
        """Declare all custom Azure metadata fields so the destination does not strip them."""
        schema = super().get_json_schema()

        if "properties" not in schema:
            schema["properties"] = {}

        schema["properties"]["_ab_source_file_content_type"] = {
            "type": ["string", "null"],
            "description": "MIME type of the blob from blob.content_settings.content_type",
        }
        schema["properties"]["_ab_source_file_created_at"] = {
            "type": ["string", "null"],
            "description": "Blob creation timestamp in ISO-8601 format",
        }
        schema["properties"]["_ab_source_file_size"] = {
            "type": ["integer", "null"],
            "description": "Blob size in bytes",
        }
        schema["properties"]["_ab_source_file_access_tier"] = {
            "type": ["string", "null"],
            "description": "Azure Blob access tier (e.g., Hot, Cool, Archive)",
        }
        schema["properties"]["_ab_source_file_blob_metadata"] = {
            "type": ["string", "null"],
            "description": "User-defined metadata attached to the blob, serialized as a JSON string",
        }
        schema["properties"]["_ab_source_file_prefix_path"] = {
            "type": ["string", "null"],
            "description": (
                "Virtual directory path derived from the blob name by stripping the filename "
                "(e.g., 'Pharma/Sales/Q1'). Empty string for blobs at the container root."
            ),
        }
        schema["properties"]["_ab_source_file_container_metadata"] = {
            "type": ["string", "null"],
            "description": (
                "User-defined metadata set at the container level, serialized as a JSON string. "
                "Fetched once per sync via get_container_properties() — shared across all blobs."
            ),
        }

        return schema

    def read_records_from_slice(self, stream_slice: Any) -> Iterable[AirbyteMessage]:
        """
        Override to inject Azure-specific metadata into each record.

        CDK 4.x yields AirbyteMessage objects directly from read_records_from_slice() with no
        transform_record() hook. We process files one at a time — calling super() with a single-
        file sub-slice — so we know which AzureRemoteFile corresponds to each yielded message.
        Non-RECORD messages (logs, state) are passed through unchanged.
        """
        for file in stream_slice["files"]:
            for message in super().read_records_from_slice({"files": [file]}):
                if message.record is not None:
                    d = message.record.data
                    d["_ab_source_file_content_type"] = file.content_type
                    d["_ab_source_file_created_at"] = file.created_at.isoformat() if file.created_at else None
                    d["_ab_source_file_size"] = file.size
                    d["_ab_source_file_access_tier"] = file.access_tier
                    d["_ab_source_file_blob_metadata"] = json.dumps(file.blob_metadata) if file.blob_metadata is not None else None
                    d["_ab_source_file_prefix_path"] = file.prefix_path
                    d["_ab_source_file_container_metadata"] = json.dumps(file.container_metadata) if file.container_metadata is not None else None

                    logger.info(
                        f"[Azure] Emitting metadata for {file.uri!r} — "
                        f"content_type={file.content_type}, size={file.size}, "
                        f"access_tier={file.access_tier}, "
                        f"created_at={d['_ab_source_file_created_at']}, "
                        f"prefix_path={file.prefix_path!r}, "
                        f"blob_metadata_keys={list(file.blob_metadata.keys()) if file.blob_metadata else []}, "
                        f"container_metadata_keys={list(file.container_metadata.keys()) if file.container_metadata else []}"
                    )
                    if file.content_type is None:
                        logger.warning(f"[Azure] _ab_source_file_content_type is null for {file.uri!r}")
                yield message

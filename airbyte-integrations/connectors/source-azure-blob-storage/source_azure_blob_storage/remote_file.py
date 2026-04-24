#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from datetime import datetime
from typing import Dict, Optional

from airbyte_cdk.sources.file_based.remote_file import RemoteFile


class AzureRemoteFile(RemoteFile):
    """
    Extends RemoteFile with Azure Blob Storage metadata attributes.

    All fields are populated from the BlobProperties object returned by
    ContainerClient.list_blobs() at listing time — no extra per-blob API calls needed.

    Attributes:
        content_type: MIME type from blob.content_settings.content_type
            (e.g., "application/pdf"). May be None if not set by the uploader.
        created_at: Blob creation timestamp from blob.creation_time.
        size: Blob size in bytes from blob.size.
        access_tier: Azure access tier from blob.blob_tier
            (e.g., "Hot", "Cool", "Archive"). May be None for non-tiered storage.
        blob_metadata: User-defined metadata dict from blob.metadata.
            Serialized as a JSON string before emitting so the schema stays fixed.
        prefix_path: Virtual directory path derived from blob.name by stripping the filename
            (e.g., blob.name="Pharma/Sales/Q1/report.pdf" → prefix_path="Pharma/Sales/Q1").
            Empty string for blobs at the container root. Zero extra API calls.
        container_metadata: User-defined metadata set at the container level, fetched once
            per sync via get_container_properties(). Shared across all blobs in the container.
            Serialized as a JSON string before emitting so the schema stays fixed.
    """

    content_type: Optional[str] = None
    created_at: Optional[datetime] = None
    size: Optional[int] = None
    access_tier: Optional[str] = None
    blob_metadata: Optional[Dict[str, str]] = None
    prefix_path: Optional[str] = None
    container_metadata: Optional[Dict[str, str]] = None

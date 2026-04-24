#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
from datetime import datetime
from typing import Dict, Optional

from google.cloud import storage
from google.oauth2 import credentials, service_account

from airbyte_cdk.sources.file_based.remote_file import RemoteFile


def get_gcs_client(config):
    if config.credentials.auth_type == "Service":
        creds = service_account.Credentials.from_service_account_info(json.loads(config.credentials.service_account))
    else:
        creds = credentials.Credentials(
            config.credentials.access_token,
            refresh_token=config.credentials.refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=config.credentials.client_id,
            client_secret=config.credentials.client_secret,
        )
    client = storage.Client(credentials=creds)
    return client


def get_gcs_blobs(config):
    client = get_gcs_client(config)
    bucket = client.get_bucket(config.gcs_bucket)
    blobs = bucket.list_blobs(prefix=config.gcs_path)
    # TODO: only support CSV initially. Change this check if implementing other file formats.
    blobs = [blob for blob in blobs if "csv" in blob.name.lower()]
    return blobs


def get_stream_name(blob):
    blob_name = blob.name
    # Remove path from stream name
    blob_name_without_path = blob_name.split("/")[-1]
    # Remove file extension from stream name
    stream_name = blob_name_without_path.replace(".csv", "")
    return stream_name


class GCSRemoteFile(RemoteFile):
    """
    Extends RemoteFile instance with GCS-specific attributes.

    Attributes:
        displayed_uri: Used by Cursor to identify files with temporal local path in their uri attribute.
        id: Stable identifier for the file (always gs://bucket/path/file.ext format).
            Used for fingerprint generation - stays the same across syncs.
        download_url: URL that can be used to download the file.
            - For OAuth: Same as id (gs:// path)
            - For Service Account: Signed URL with expiration

    Document metadata attributes (populated from blob properties at listing time):
        content_type: MIME type of the object as stored in GCS (e.g., "application/pdf").
        created_at: Object creation timestamp from GCS (blob.time_created).
        size: Object size in bytes (blob.size).
        storage_class: GCS storage class (e.g., "STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE").
        custom_metadata: User-defined metadata dict attached to the object (blob.metadata).
        prefix_path: Virtual folder path derived from blob.name by stripping the filename
            (e.g., blob.name="Pharma/Sales/Q1/report.pdf" → prefix_path="Pharma/Sales/Q1").
            Empty string for objects at the bucket root. Zero extra API calls.
    """

    displayed_uri: Optional[str] = None
    id: Optional[str] = None
    download_url: Optional[str] = None
    content_type: Optional[str] = None
    created_at: Optional[datetime] = None
    size: Optional[int] = None
    storage_class: Optional[str] = None
    custom_metadata: Optional[Dict[str, str]] = None
    prefix_path: Optional[str] = None

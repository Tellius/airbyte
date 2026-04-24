#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from typing import Dict, List, Optional

from airbyte_cdk.sources.file_based.remote_file import RemoteFile


class S3RemoteFile(RemoteFile):
    """
    Extends RemoteFile with S3-specific metadata attributes.

    Fields populated at listing time from list_objects_v2 response:
        size: Object size in bytes (file["Size"]).
        storage_class: S3 storage class (file["StorageClass"], e.g. STANDARD, STANDARD_IA, GLACIER).
        content_type: MIME type derived from the file key extension via mimetypes.guess_type().
            S3's native ContentType field is not returned by list_objects_v2, so we derive
            it from the extension to avoid a head_object() call per file during listing.

    Fields populated lazily in open_file() via separate API calls, for files
    that actually pass glob/date filtering and are being processed:
        user_metadata: User-defined metadata headers (x-amz-meta-*) from head_object()["Metadata"].
            Serialized as a dict; the stream will JSON-encode it before emitting.
        object_tags: S3 object tags from get_object_tagging()["TagSet"].
            Stored as a list of {"Key": ..., "Value": ...} dicts; the stream will JSON-encode it.
        prefix_path: Virtual folder path derived from the object Key by stripping the filename
            (e.g., Key="Pharma/Sales/Q1/report.pdf" → prefix_path="Pharma/Sales/Q1").
            Empty string for objects at the bucket root. Zero extra API calls.
    """

    size: Optional[int] = None
    storage_class: Optional[str] = None
    content_type: Optional[str] = None
    user_metadata: Optional[Dict[str, str]] = None
    object_tags: Optional[List[Dict[str, str]]] = None
    prefix_path: Optional[str] = None

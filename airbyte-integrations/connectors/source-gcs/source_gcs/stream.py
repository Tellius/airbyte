# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import logging
import urllib.parse
from typing import Any, Mapping

from airbyte_cdk.sources.file_based.stream import DefaultFileBasedStream
from source_gcs.helpers import GCSRemoteFile

logger = logging.getLogger("airbyte")


class GCSStream(DefaultFileBasedStream):
    """
    Custom stream for GCS that adds source file identification fields.
    
    These fields enable:
    1. Stable document fingerprinting across syncs (using _ab_source_file_id)
    2. File download capability (using _ab_source_file_redirect_url)
    
    In Parsing Mode, the default DefaultFileBasedStream only includes:
    - content, document_key, _ab_source_file_url, _ab_source_file_last_modified
    
    This custom stream adds:
    - _ab_source_file_id: Stable GCS path (gs://bucket/path/file.ext) for fingerprinting
    - _ab_source_file_redirect_url: Stable GCS path (gs://bucket/path/file.ext) for file reference
    
    Note: We use stable gs:// paths instead of signed URLs because:
    1. Signed URLs expire (typically within 7 days)
    2. Signed URLs are very long (700+ chars) causing DB varchar(255) issues
    3. Stable paths are meaningful and can be used to generate fresh signed URLs when needed
    """
    
    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Override to add custom GCS fields to the schema.
        
        This is critical - without adding these fields to the schema,
        Airbyte will strip them when sending records to the destination.
        """
        # Get the base schema from parent
        schema = super().get_json_schema()
        
        # Add our custom fields to the schema
        if 'properties' not in schema:
            schema['properties'] = {}
        
        schema['properties']['_ab_source_file_id'] = {
            'type': 'string',
            'description': 'Stable GCS file path (gs://bucket/path/file.ext) for fingerprinting'
        }
        schema['properties']['_ab_source_file_redirect_url'] = {
            'type': 'string',
            'description': 'Stable GCS file path (gs://bucket/path/file.ext) for file reference'
        }
        schema['properties']['file_name'] = {
            'type': 'string',
            'description': 'Clean filename extracted from the GCS path (e.g., "document.pdf")'
        }
        
        return schema

    def transform_record(self, record: dict[str, Any], file: GCSRemoteFile, last_updated: str) -> dict[str, Any]:
        record[self.ab_last_mod_col] = last_updated
        # Use stable gs:// path instead of signed URL for _ab_source_file_url
        # This ensures source_path in middleware is short and meaningful (not a 700+ char signed URL)
        record[self.ab_file_name_col] = file.id if file.id else (file.displayed_uri if file.displayed_uri else file.uri)
        
        # Add stable file ID for fingerprint generation
        # This is the gs://bucket/path/file.ext format - stable across syncs
        # The destination uses this for generating consistent document fingerprints
        if file.id:
            record["_ab_source_file_id"] = file.id
        else:
            logger.warning(f"file.id is None or empty - cannot set _ab_source_file_id")
        
        # Add stable GCS path for file reference
        # Use gs:// path instead of signed URL because:
        # - Signed URLs expire (typically within 7 days)
        # - Signed URLs are very long (700+ chars) causing DB issues
        # - Stable paths can be used to generate fresh signed URLs when needed
        if file.id:
            record["_ab_source_file_redirect_url"] = file.id
        
        # Add clean filename (extracted from URI, without query params)
        # This is used by the destination for the 'name' field in middleware
        # The URI may be a signed URL with query params, so we extract just the filename
        try:
            uri_without_query = file.uri.split("?")[0]  # Remove query parameters
            filename = uri_without_query.split("/")[-1]  # Get last path segment
            clean_filename = urllib.parse.unquote(filename)  # URL decode (%20 -> space)
            record["file_name"] = clean_filename
        except Exception as e:
            logger.warning(f"Failed to extract file_name from URI: {e}")
        
        # Override document_key to use stable gs:// path instead of signed URL
        # The CDK's unstructured parser sets document_key to the file URI (signed URL for Service Account)
        # We override it to use the stable path for consistency and to avoid long URLs in the output
        if file.id:
            record["document_key"] = file.id
        
        return record

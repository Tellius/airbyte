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
    Custom stream for Google Drive that adds file ID and redirect URL 
    to records in Parsing Mode.
    
    In Parsing Mode, the default DefaultFileBasedStream only includes:
    - content, document_key, _ab_source_file_url, _ab_source_file_last_modified
    
    This custom stream adds:
    - _ab_source_file_id: The Google Drive file ID
    - _ab_source_file_redirect_url: The full redirect URL (e.g., https://drive.google.com/open?id=...)
    
    These fields enable creating redirect links back to the source document.
    """
    
    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Override to add custom Google Drive fields to the schema.
        """
        # Get the base schema from parent
        schema = super().get_json_schema()
        
        # Add our custom fields to the schema
        if 'properties' not in schema:
            schema['properties'] = {}
        
        schema['properties']['_ab_source_file_id'] = {
            'type': 'string',
            'description': 'Google Drive file ID'
        }
        schema['properties']['_ab_source_file_redirect_url'] = {
            'type': 'string',
            'description': 'Full Google Drive redirect URL'
        }
        
        return schema

    def transform_record(
        self, record: dict[str, Any], file: GoogleDriveRemoteFile, last_updated: str
    ) -> dict[str, Any]:
        """
        Transform a record by adding Google Drive specific metadata.
        
        Args:
            record: The parsed record from the file
            file: The GoogleDriveRemoteFile containing file metadata
            last_updated: The last modified timestamp string
            
        Returns:
            The record with additional _ab_source_file_id and _ab_source_file_redirect_url fields
        """
        # Call parent's transform_record first to ensure standard fields are added
        record = super().transform_record(record, file, last_updated)
        
        # Add Google Drive specific fields that are available in GoogleDriveRemoteFile
        record["_ab_source_file_id"] = file.id
        record["_ab_source_file_redirect_url"] = file.url
        
        return record

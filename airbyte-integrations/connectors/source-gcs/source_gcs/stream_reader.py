#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import itertools
import json
import logging
import tempfile
from datetime import datetime, timedelta
from io import IOBase, StringIO
from typing import Iterable, List, Optional

import pytz
import smart_open
from google.cloud import storage
from google.oauth2 import credentials, service_account

from airbyte_cdk.sources.file_based.exceptions import ErrorListingFiles, FileBasedSourceError
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from source_gcs.config import Config
from source_gcs.helpers import GCSRemoteFile
from source_gcs.zip_helper import ZipHelper


# google can raise warnings for end user credentials, wrapping it to Logger
logging.captureWarnings(True)


class NamedFileWrapper(IOBase):
    """
    A wrapper around a file-like object that adds a .name attribute.
    
    This is needed because smart_open's SeekableBufferedInputBase doesn't properly
    support setting the .name attribute, and the unstructured library's detect_filetype()
    function requires file.name to be a string path to determine the file extension.
    
    Without this wrapper, detect_filetype() fails with:
    TypeError: expected str, bytes or os.PathLike object, not NoneType
    
    Note: The name property also has a setter because the CDK's unstructured_parser.py
    sometimes sets file.name = None (e.g., in _get_filetype method).
    """
    
    def __init__(self, file_handle: IOBase, name: str):
        self._file_handle = file_handle
        self._name = name
    
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter
    def name(self, value):
        # IMPORTANT: Ignore attempts to set name to None.
        # The CDK's unstructured_parser.py may try to set file.name = None,
        # but we need to preserve the original filename for detect_filetype() to work.
        # If we allow None, we're back to the original error:
        # TypeError: expected str, bytes or os.PathLike object, not NoneType
        if value is not None:
            self._name = value
        # If value is None, we silently ignore it and keep the original name
    
    def read(self, size: int = -1):
        return self._file_handle.read(size)
    
    def readline(self, size: int = -1):
        return self._file_handle.readline(size)
    
    def readlines(self, hint: int = -1):
        return self._file_handle.readlines(hint)
    
    def write(self, s):
        return self._file_handle.write(s)
    
    def seek(self, offset: int, whence: int = 0):
        return self._file_handle.seek(offset, whence)
    
    def tell(self):
        return self._file_handle.tell()
    
    def close(self):
        return self._file_handle.close()
    
    def flush(self):
        return self._file_handle.flush()
    
    def readable(self):
        return self._file_handle.readable() if hasattr(self._file_handle, 'readable') else True
    
    def writable(self):
        return self._file_handle.writable() if hasattr(self._file_handle, 'writable') else False
    
    def seekable(self):
        return self._file_handle.seekable() if hasattr(self._file_handle, 'seekable') else False
    
    def __iter__(self):
        return iter(self._file_handle)
    
    def __next__(self):
        return next(self._file_handle)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

ERROR_MESSAGE_ACCESS = (
    "We don't have access to {uri}. The file appears to have become unreachable during sync."
    "Check whether key {uri} exists in `{bucket}` bucket and/or has proper ACL permissions"
)


class SourceGCSStreamReader(AbstractFileBasedStreamReader):
    """
    Stream reader for Google Cloud Storage (GCS).
    """

    def __init__(self):
        super().__init__()
        self._gcs_client = None
        self._config = None
        self.tmp_dir = tempfile.TemporaryDirectory()

    @property
    def config(self) -> Config:
        return self._config

    @config.setter
    def config(self, value: Config):
        assert isinstance(value, Config), "Config must be an instance of the expected Config class."
        self._config = value

    def _initialize_gcs_client(self):
        if self.config is None:
            raise ValueError("Source config is missing; cannot create the GCS client.")
        if self._gcs_client is None:
            credentials = self._get_credentials()
            # using default project to avoid getting project from env, applies only for OAuth creds
            project = getattr(credentials, "project_id", "default")
            self._gcs_client = storage.Client(project=project, credentials=credentials)
        return self._gcs_client

    def _get_credentials(self):
        if self.config.credentials.auth_type == "Service":
            # Service Account authorization
            return service_account.Credentials.from_service_account_info(json.loads(self.config.credentials.service_account))
        # Google OAuth
        return credentials.Credentials(
            self.config.credentials.access_token,
            refresh_token=self.config.credentials.refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=self.config.credentials.client_id,
            client_secret=self.config.credentials.client_secret,
        )

    @property
    def gcs_client(self) -> storage.Client:
        return self._initialize_gcs_client()

    def get_matching_files(self, globs: List[str], prefix: Optional[str], logger: logging.Logger) -> Iterable[GCSRemoteFile]:
        """
        Retrieve all files matching the specified glob patterns in GCS.
        """
        try:
            start_date = (
                datetime.strptime(self.config.start_date, self.DATE_TIME_FORMAT) if self.config and self.config.start_date else None
            )
            prefixes = [prefix] if prefix else self.get_prefixes_from_globs(globs or [])
            globs = globs or [None]

            if not prefixes:
                prefixes = [""]

            for prefix, glob in itertools.product(prefixes, globs):
                bucket = self.gcs_client.get_bucket(self.config.bucket)
                blobs = bucket.list_blobs(prefix=prefix, match_glob=glob)
                for blob in blobs:
                    last_modified = blob.updated.astimezone(pytz.utc).replace(tzinfo=None)

                    if not start_date or last_modified >= start_date:
                        # STABLE identifier - always gs:// path (for fingerprint generation)
                        # This stays the same across syncs, unlike signed URLs
                        stable_id = f"gs://{blob.bucket.name}/{blob.name}"
                        
                        if self.config.credentials.auth_type == "Client":
                            # OAuth: gs:// path can be used directly for download
                            download_url = stable_id
                        else:
                            # Service Account: need signed URL for download access
                            download_url = blob.generate_signed_url(expiration=timedelta(days=7), version="v4")

                        file_extension = ".".join(blob.name.split(".")[1:])
                        
                        remote_file = GCSRemoteFile(
                            uri=download_url,           # For open_file() compatibility
                            last_modified=last_modified,
                            mime_type=file_extension,
                            id=stable_id,               # Stable identifier for fingerprinting
                            download_url=download_url   # URL for downloading the file
                        )

                        if file_extension == "zip":
                            yield from ZipHelper(blob, remote_file, self.tmp_dir).get_gcs_remote_files()
                        else:
                            yield remote_file
        except Exception as exc:
            self._handle_file_listing_error(exc, prefix, logger)

    def _handle_file_listing_error(self, exc: Exception, prefix: str, logger: logging.Logger):
        logger.error(f"Error while listing files: {str(exc)}")
        raise ErrorListingFiles(
            FileBasedSourceError.ERROR_LISTING_FILES,
            source="gcs",
            bucket=self.config.bucket,
            prefix=prefix,
        ) from exc

    def _extract_filename_from_uri(self, uri: str) -> str:
        """
        Extract the filename from a URI.
        Handles both gs:// URIs and signed HTTPS URLs.
        
        Examples:
            gs://bucket/path/to/file.pdf -> file.pdf
            https://storage.googleapis.com/.../file.pdf?X-Goog-Signature=... -> file.pdf
        """
        if uri.startswith("gs://"):
            # gs://bucket/path/to/file.pdf -> file.pdf
            return uri.split("/")[-1]
        else:
            # Signed URL: https://.../.../file.pdf?X-Goog-Signature=...
            # Extract the path part before query params, then get the filename
            path_part = uri.split("?")[0]
            return path_part.split("/")[-1]

    def open_file(self, file: GCSRemoteFile, mode: FileReadMode, encoding: Optional[str], logger: logging.Logger) -> IOBase:
        """
        Open and yield a remote file from GCS for reading.
        """
        logger.debug(f"Opening file: {file.uri}")

        # choose correct compression mode
        file_extension = file.mime_type.split(".")[-1]
        if file_extension in ["gz", "bz2"]:
            compression = "." + file_extension
        else:
            compression = "disable"

        try:
            result = smart_open.open(
                file.uri, mode=mode.value, compression=compression, encoding=encoding, transport_params={"client": self.gcs_client}
            )
            if not result.seekable():
                result = StringIO(result.read())
            
            # FIX: Wrap the file handle in NamedFileWrapper to ensure .name attribute is available.
            # The unstructured library's detect_filetype() function calls os.path.splitext(file.name)
            # to determine the file extension. smart_open's SeekableBufferedInputBase doesn't properly
            # support setting .name, so we wrap it in our custom class that exposes .name correctly.
            # Without this fix, detect_filetype() fails with:
            # TypeError: expected str, bytes or os.PathLike object, not NoneType
            filename = self._extract_filename_from_uri(file.uri)
            
            # URL decode the filename to handle %20 -> space, etc.
            from urllib.parse import unquote
            filename = unquote(filename)
            
            # Wrap the result in NamedFileWrapper to ensure .name is properly exposed
            result = NamedFileWrapper(result, filename)
            
        except OSError as oe:
            logger.warning(ERROR_MESSAGE_ACCESS.format(uri=file.uri, bucket=self.config.bucket))
            logger.exception(oe)
            raise oe
        return result

# Copyright (c) 2023 Airbyte, Inc., all rights reserved.


import logging
from io import IOBase
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union

import pytz
from azure.core.credentials import AccessToken, TokenCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient
from smart_open import open

from airbyte_cdk import AirbyteTracedException, FailureType
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator

from .remote_file import AzureRemoteFile
from .spec import SourceAzureBlobStorageSpec


class AzureClientCredentialsAuthenticator(Oauth2Authenticator, TokenCredential):
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, **kwargs):
        super().__init__(
            token_refresh_endpoint=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            grant_type="client_credentials",
            scopes=["https://storage.azure.com/.default"],
            refresh_token=None,
        )

    def build_refresh_request_body(self) -> Mapping[str, Any]:
        """
        Returns the request body to set on the refresh request

        Override to define additional parameters
        """
        payload: MutableMapping[str, Any] = {
            "grant_type": self.get_grant_type(),
            "client_id": self.get_client_id(),
            "client_secret": self.get_client_secret(),
        }

        if self.get_scopes():
            payload["scope"] = " ".join(self.get_scopes())

        if self.get_refresh_request_body():
            for key, val in self.get_refresh_request_body().items():
                # We defer to existing oauth constructs over custom configured fields
                if key not in payload:
                    payload[key] = val

        return payload

    def get_token(self, *args, **kwargs) -> AccessToken:
        """Parent class handles Oauth Refresh token logic."""
        return AccessToken(token=self.get_access_token(), expires_on=int(self.get_token_expiry_date().timestamp()))


class AzureOauth2Authenticator(Oauth2Authenticator, TokenCredential):
    """
    Authenticator for Azure Blob Storage SDK to align with azure.core.credentials.TokenCredential protocol
    """

    def get_token(self, *args, **kwargs) -> AccessToken:
        """Parent class handles Oauth Refresh token logic.
        `expires_on` is ignored and set to year 2222 to align with protocol.
        """
        return AccessToken(token=self.get_access_token(), expires_on=7952342400)


class SourceAzureBlobStorageStreamReader(AbstractFileBasedStreamReader):
    _credentials = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._config = None

    @property
    def config(self) -> SourceAzureBlobStorageSpec:
        return self._config

    @config.setter
    def config(self, value: SourceAzureBlobStorageSpec) -> None:
        self._config = value

    @property
    def account_url(self) -> str:
        if not self.config.azure_blob_storage_endpoint:
            return f"https://{self.config.azure_blob_storage_account_name}.blob.core.windows.net"
        return self.config.azure_blob_storage_endpoint

    @property
    def azure_container_client(self):
        return ContainerClient(
            self.account_url, container_name=self.config.azure_blob_storage_container_name, credential=self.azure_credentials
        )

    @property
    def azure_blob_service_client(self):
        return BlobServiceClient(self.account_url, credential=self._credentials)

    @property
    def azure_credentials(self) -> Union[str, AzureOauth2Authenticator, AzureClientCredentialsAuthenticator]:
        if not self._credentials:
            if self.config.credentials.auth_type == "storage_account_key":
                self._credentials = self.config.credentials.azure_blob_storage_account_key
            elif self.config.credentials.auth_type == "oauth2":
                self._credentials = AzureOauth2Authenticator(
                    token_refresh_endpoint=f"https://login.microsoftonline.com/{self.config.credentials.tenant_id}/oauth2/v2.0/token",
                    client_id=self.config.credentials.client_id,
                    client_secret=self.config.credentials.client_secret,
                    refresh_token=self.config.credentials.refresh_token,
                )
            elif self.config.credentials.auth_type == "client_credentials":
                self._credentials = AzureClientCredentialsAuthenticator(
                    tenant_id=self.config.credentials.app_tenant_id,
                    client_id=self.config.credentials.app_client_id,
                    client_secret=self.config.credentials.app_client_secret,
                )

        return self._credentials

    def get_matching_files(
        self,
        globs: List[str],
        prefix: Optional[str],
        logger: logging.Logger,
    ) -> Iterable[RemoteFile]:
        prefixes = [prefix] if prefix else self.get_prefixes_from_globs(globs)
        prefixes = prefixes or [None]
        try:
            # Fetch container-level metadata once per sync — single API call shared across all blobs.
            container_client = self.azure_container_client
            try:
                container_props = container_client.get_container_properties()
                raw_container_metadata = container_props.get("metadata") or {}
                container_metadata = dict(raw_container_metadata) if raw_container_metadata else None
                logger.info(
                    f"[Azure] Container metadata fetched — "
                    f"container={self.config.azure_blob_storage_container_name!r}, "
                    f"keys={list(container_metadata.keys()) if container_metadata else []}"
                )
            except Exception as e:
                container_metadata = None
                logger.warning(f"[Azure] Failed to fetch container metadata: {e}")

            for prefix in prefixes:
                for blob in container_client.list_blobs(name_starts_with=prefix):
                    last_modified = blob.last_modified.astimezone(pytz.utc).replace(tzinfo=None)

                    content_settings = getattr(blob, "content_settings", None)
                    content_type = content_settings.content_type if content_settings else None

                    created_at_raw = getattr(blob, "creation_time", None)
                    created_at = created_at_raw.astimezone(pytz.utc).replace(tzinfo=None) if created_at_raw else None

                    size = getattr(blob, "size", None)
                    access_tier = getattr(blob, "blob_tier", None)
                    raw_metadata = getattr(blob, "metadata", None)
                    blob_metadata = dict(raw_metadata) if raw_metadata else None
                    # Derive prefix path from blob.name — everything before the last "/"
                    # e.g. "Pharma/Sales/Q1/report.pdf" → "Pharma/Sales/Q1"
                    blob_name_parts = blob.name.split("/")
                    prefix_path = "/".join(blob_name_parts[:-1])

                    remote_file = AzureRemoteFile(
                        uri=blob.name,
                        last_modified=last_modified,
                        content_type=content_type,
                        created_at=created_at,
                        size=size,
                        access_tier=str(access_tier) if access_tier is not None else None,
                        blob_metadata=blob_metadata,
                        prefix_path=prefix_path,
                        container_metadata=container_metadata,
                    )

                    logger.info(
                        f"[Azure] Metadata captured for {blob.name!r} — "
                        f"content_type={content_type}, size={size}, "
                        f"access_tier={access_tier}, created_at={created_at}, "
                        f"prefix_path={prefix_path!r}, "
                        f"blob_metadata_keys={list(blob_metadata.keys()) if blob_metadata else []}, "
                        f"container_metadata_keys={list(container_metadata.keys()) if container_metadata else []}"
                    )

                    yield from self.filter_files_by_globs_and_start_date([remote_file], globs)
        except ResourceNotFoundError as e:
            raise AirbyteTracedException(failure_type=FailureType.config_error, internal_message=e.message, message=e.reason or e.message)

    def open_file(self, file: RemoteFile, mode: FileReadMode, encoding: Optional[str], logger: logging.Logger) -> IOBase:
        try:
            result = open(
                f"azure://{self.config.azure_blob_storage_container_name}/{file.uri}",
                transport_params={"client": self.azure_blob_service_client},
                mode=mode.value,
                encoding=encoding,
            )
        except OSError:
            logger.warning(
                f"We don't have access to {file.uri}. The file appears to have become unreachable during sync."
                f"Check whether key {file.uri} exists in `{self.config.azure_blob_storage_container_name}` container and/or has proper ACL permissions"
            )
        return result

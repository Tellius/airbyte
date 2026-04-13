"""
Custom OCR parser for Airbyte file-based sources.

Routes between Mistral OCR and Airbyte's default UnstructuredParser based on
a config flag fetched from the Tellius middleware service at sync start.
When Mistral is disabled (or middleware is unreachable), delegates to the
default UnstructuredParser (unstructured.io) for full backward compatibility.

Config flow:
  1. Fetch auth token from Kaiya service
  2. GET middleware /runtimeConfig/unstructureMistralOcrConfig
  3. If use_mistral_ocr=true and mistral_api_key is non-empty → use Mistral OCR
  4. Otherwise delegate to UnstructuredParser

All document formats (PDF, DOCX, PPTX) are sent to Mistral OCR when active.
When falling back, UnstructuredParser handles everything.

Replaces the default UnstructuredParser via .pth auto-patch while maintaining
the same output schema so downstream consumers (DocumentMetadataCollector,
MiddlewareApiClient) work unchanged.
"""

import base64
import logging
import os
import time
from io import IOBase
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple
from urllib.parse import urlparse, unquote

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.config.unstructured_format import UnstructuredFormat
from airbyte_cdk.sources.file_based.exceptions import FileBasedSourceError, RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import (
    AbstractFileBasedStreamReader,
    FileReadMode,
)
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import SchemaType

SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".pptx", ".md", ".txt"}

MIME_TYPES = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}

MIDDLEWARE_URL = os.environ.get(
    "TELLIUS_MIDDLEWARE_URL",
    "http://middleware-service:8082/runtimeConfig/unstructureMistralOcrConfig",
)
KAIYA_AUTH_TOKEN_URL = os.environ.get(
    "TELLIUS_KAIYA_AUTH_TOKEN_URL",
    "http://kaiya-unstructured-service:8080/api/unstructured/internal-auth-token",
)
SERVICE_TIMEOUT_SECONDS = 10

logger = logging.getLogger("airbyte.tellius_ocr_parser")


def _get_file_extension(uri: str) -> str:
    path = unquote(urlparse(uri).path)
    _, ext = os.path.splitext(path)
    return ext.lower()


def _extract_format(config: FileBasedStreamConfig) -> UnstructuredFormat:
    if not isinstance(config.format, UnstructuredFormat):
        raise ValueError(f"Expected UnstructuredFormat, got {type(config.format)}")
    return config.format


def _extract_text_mistral(file_bytes: bytes, api_key: str, mime_type: str) -> str:
    """Extract text from a document using Mistral OCR REST API.

    Supports PDF, DOCX, and PPTX via the document_url data URI with the
    appropriate MIME type.
    """
    import requests as _requests

    start_time = time.time()

    encoded = base64.standard_b64encode(file_bytes).decode("utf-8")
    data_uri = f"data:{mime_type};base64,{encoded}"

    response = _requests.post(
        "https://api.mistral.ai/v1/ocr",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": "mistral-ocr-latest",
            "document": {
                "type": "document_url",
                "document_url": data_uri,
            },
        },
        timeout=300,
    )
    response.raise_for_status()
    result = response.json()

    pages_text = []
    for page in result.get("pages", []):
        md = page.get("markdown", "").strip()
        if md:
            pages_text.append(md)

    elapsed = time.time() - start_time
    total_chars = sum(len(p) for p in pages_text)
    logger.info(
        f"Mistral OCR: {len(result.get('pages', []))} pages, "
        f"{elapsed:.2f}s, {total_chars} chars, mime_type={mime_type}"
    )

    return "\n\n".join(pages_text)


class CustomOCRParser(FileTypeParser):
    """
    Tellius custom document parser that routes between Mistral OCR and
    Airbyte's default UnstructuredParser based on middleware config.

    On first parse_records() call, fetches runtime config from the Tellius
    middleware service. If use_mistral_ocr is True and a valid API key is
    available, processes documents with Mistral OCR. Otherwise, delegates
    entirely to the default UnstructuredParser for backward compatibility.

    The config is cached for the pod lifetime (one sync = one pod).
    If the middleware is unreachable, defaults to UnstructuredParser.
    """

    def __init__(self):
        self._use_mistral: Optional[bool] = None
        self._mistral_api_key: Optional[str] = None
        self._fallback_parser: Optional[FileTypeParser] = None

    @property
    def parser_max_n_files_for_schema_inference(self) -> Optional[int]:
        return 1

    @property
    def parser_max_n_files_for_parsability(self) -> Optional[int]:
        return 0

    def get_parser_defined_primary_key(self, config: FileBasedStreamConfig) -> Optional[str]:
        return "document_key"

    def check_config(self, config: FileBasedStreamConfig) -> Tuple[bool, Optional[str]]:
        return True, None

    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> SchemaType:
        return {
            "content": {
                "type": "string",
                "description": "Content of the file as text. Might be null if the file could not be parsed",
            },
            "document_key": {
                "type": "string",
                "description": "Unique identifier of the document, e.g. the file path",
            },
            "_ab_source_file_parse_error": {
                "type": "string",
                "description": "Error message if the file could not be parsed even though the file is supported",
            },
        }

    def _fetch_auth_token(self) -> str:
        """Fetch authorization token from Kaiya service (same pattern as Kotlin MiddlewareApiClient)."""
        import requests as _requests

        response = _requests.get(
            KAIYA_AUTH_TOKEN_URL,
            headers={"USERID": "TELLIUS_SUPERUSER_ID"},
            timeout=SERVICE_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("token", "")

    def _should_use_mistral(self, log: logging.Logger) -> bool:
        """Check middleware runtime config to decide which parser to use. Cached per pod lifetime."""
        if self._use_mistral is not None:
            return self._use_mistral

        try:
            import requests as _requests

            auth_token = self._fetch_auth_token()

            response = _requests.get(
                MIDDLEWARE_URL,
                headers={
                    "Authorization": auth_token,
                    "USERID": "TELLIUS_SUPERUSER_ID",
                    "Content-Type": "application/json",
                },
                timeout=SERVICE_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            config_response = response.json()

            ocr_config = config_response.get("unstructureMistralOcrConfig", {})
            use_flag = bool(ocr_config.get("use_mistral_ocr", False))
            api_key = ocr_config.get("mistral_api_key", "")

            if not use_flag or not api_key:
                log.info(
                    f"Middleware OCR config: use_mistral_ocr={use_flag}, "
                    f"api_key_present={bool(api_key)}. Using default UnstructuredParser."
                )
                self._use_mistral = False
                return self._use_mistral

            self._mistral_api_key = api_key
            self._use_mistral = True
            log.info("Middleware OCR config: Mistral OCR enabled, API key resolved.")

        except Exception as e:
            log.warning(
                f"Failed to fetch/process OCR config ({e}). "
                "Defaulting to UnstructuredParser (unstructured.io)."
            )
            self._use_mistral = False

        return self._use_mistral

    def _get_fallback_parser(self) -> FileTypeParser:
        """Lazy-import and cache Airbyte's default UnstructuredParser."""
        if self._fallback_parser is None:
            from airbyte_cdk.sources.file_based.file_types.unstructured_parser import UnstructuredParser
            self._fallback_parser = UnstructuredParser()
            logger.info("Initialized fallback UnstructuredParser")
        return self._fallback_parser

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        discovered_schema: Optional[Mapping[str, SchemaType]],
    ) -> Iterable[Dict[str, Any]]:
        if not self._should_use_mistral(logger):
            yield from self._get_fallback_parser().parse_records(
                config, file, stream_reader, logger, discovered_schema
            )
            return

        format_config = _extract_format(config)

        with stream_reader.open_file(file, self.file_read_mode, None, logger) as file_handle:
            try:
                content = self._read_file(file_handle, file, logger)
                yield {
                    "content": content,
                    "document_key": file.uri,
                    "_ab_source_file_parse_error": None,
                }
            except RecordParseError as e:
                if format_config.skip_unprocessable_files:
                    logger.warning(f"Skipping unprocessable file {file.uri}: {e}")
                    yield {
                        "content": None,
                        "document_key": file.uri,
                        "_ab_source_file_parse_error": str(e),
                    }
                else:
                    raise
            except Exception as e:
                if format_config.skip_unprocessable_files:
                    logger.warning(f"Skipping file {file.uri} due to error: {e}")
                    yield {
                        "content": None,
                        "document_key": file.uri,
                        "_ab_source_file_parse_error": str(e),
                    }
                else:
                    raise RecordParseError(
                        FileBasedSourceError.ERROR_PARSING_RECORD,
                        filename=file.uri,
                        message=str(e),
                    )

    def _read_file(
        self,
        file_handle: IOBase,
        remote_file: RemoteFile,
        logger: logging.Logger,
    ) -> str:
        extension = _get_file_extension(remote_file.uri)

        if extension in {".md", ".txt"}:
            raw = file_handle.read()
            return raw.decode("utf-8") if isinstance(raw, bytes) else raw

        file_handle.seek(0)
        file_bytes = file_handle.read()
        if isinstance(file_bytes, str):
            file_bytes = file_bytes.encode("utf-8")
        file_handle.seek(0)

        if extension in MIME_TYPES:
            return _extract_text_mistral(file_bytes, self._mistral_api_key, MIME_TYPES[extension])
        else:
            raise RecordParseError(
                FileBasedSourceError.ERROR_PARSING_RECORD,
                filename=remote_file.uri,
                message=f"Unsupported file type: {extension}. Supported: {SUPPORTED_EXTENSIONS}",
            )

    @property
    def file_read_mode(self) -> FileReadMode:
        return FileReadMode.READ_BINARY

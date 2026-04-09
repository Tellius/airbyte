"""
Tellius custom OCR parser for Airbyte file-based sources.

This module auto-patches the CDK's default_parsers registry at import time,
replacing UnstructuredParser with CustomOCRParser. It is triggered by a .pth
file during Python startup, before any connector code runs.

The in-place dict mutation (not reassignment) is critical: FileBasedSource.__init__
captures default_parsers as a default parameter reference at class definition time.
Only mutating the same dict object propagates the change to all consumers.
"""

import logging

logger = logging.getLogger("airbyte.tellius_ocr_parser")

try:
    from airbyte_cdk.sources.file_based.file_types import default_parsers
    from airbyte_cdk.sources.file_based.config.unstructured_format import UnstructuredFormat
    from tellius_ocr_parser.parser import CustomOCRParser

    default_parsers[UnstructuredFormat] = CustomOCRParser()
    logger.info("Tellius CustomOCRParser registered, replacing default UnstructuredParser")
except ImportError:
    pass
except Exception as e:
    logger.warning(f"Failed to register Tellius CustomOCRParser: {e}")

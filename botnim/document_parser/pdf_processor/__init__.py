"""
PDF Document Processor

This module provides functionality for processing PDF documents,
including text extraction, field extraction, and Google Sheets integration.
"""

from .pdf_pipeline import PDFExtractionPipeline
from .google_sheets_service import GoogleSheetsService
from .pdf_extraction_config import PDFExtractionConfig
from .text_extraction import extract_text_from_pdf
from .field_extraction import extract_fields_from_text
from .csv_output import write_csv, read_csv
from .metadata_handler import MetadataHandler
from .metrics import MetricsCollector
from .exceptions import PDFExtractionError, FieldExtractionError

__all__ = [
    'PDFExtractionPipeline',
    'GoogleSheetsService',
    'PDFExtractionConfig',
    'extract_text_from_pdf',
    'extract_fields_from_text',
    'write_csv',
    'read_csv',
    'MetadataHandler',
    'MetricsCollector',
    'PDFExtractionError',
    'FieldExtractionError'
]

__version__ = "1.1.0" 
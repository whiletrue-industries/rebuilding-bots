"""
PDF Extraction and Sync Pipeline

This module provides a comprehensive pipeline for extracting structured data from Hebrew PDFs
and syncing it to Google Spreadsheets.
"""

from .pdf_extraction_config import PDFExtractionConfig, FieldConfig, SourceConfig
from .text_extraction import extract_text_from_pdf
from .field_extraction import extract_fields_from_text
from .csv_output import write_csv, flatten_for_csv
from .google_sheets_sync import GoogleSheetsSync
from .pdf_pipeline import PDFExtractionPipeline

__all__ = [
    'PDFExtractionConfig',
    'FieldConfig', 
    'SourceConfig',
    'extract_text_from_pdf',
    'extract_fields_from_text',
    'write_csv',
    'flatten_for_csv',
    'GoogleSheetsSync',
    'PDFExtractionPipeline'
]

__version__ = "1.1.0" 
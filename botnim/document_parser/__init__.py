"""
Document Parser Package

This package provides document processing capabilities for HTML and PDF documents.
"""

# Import from new organized structure
from .html_processor.process_document import PipelineRunner, PipelineConfig
from .html_processor.extract_structure import extract_structure_from_html, build_nested_structure, get_openai_client
from .html_processor.extract_content import extract_content_from_html
from .html_processor.generate_markdown_files import generate_markdown_from_json
from .html_processor.pipeline_config import Environment
from .pdf_processor.pdf_pipeline import PDFExtractionPipeline
from .pdf_processor.google_sheets_service import GoogleSheetsService

__all__ = [
    'PipelineRunner',
    'PipelineConfig', 
    'extract_structure_from_html',
    'build_nested_structure',
    'get_openai_client',
    'extract_content_from_html',
    'generate_markdown_from_json',
    'Environment',
    'PDFExtractionPipeline',
    'GoogleSheetsService'
] 
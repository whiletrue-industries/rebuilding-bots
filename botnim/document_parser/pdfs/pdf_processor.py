from pathlib import Path

from .pdf_extraction_config import SourceConfig

from ...config import get_logger

from .text_extraction import extract_text_from_pdf, fix_ocr_full_content
from .field_extraction import extract_fields_from_text

logger = get_logger(__name__)


def process_single_pdf(pdf_path: Path, source_config: SourceConfig, openai_client):
    try:
        # Extract text from PDF
        logger.info(f"Extracting text from PDF: {pdf_path}")
        text, is_ocr = extract_text_from_pdf(str(pdf_path))

        # Extract structured fields
        logger.info("Extracting structured fields...")
        extracted_data = extract_fields_from_text(
            text, source_config, openai_client
        )

        # Apply special OCR fix for full content field if OCR was used
        if is_ocr and isinstance(extracted_data, dict) and 'טקסט_מלא' in extracted_data:
            logger.info("Applying OCR full content fix for טקסט_מלא field")
            extracted_data['טקסט_מלא'] = fix_ocr_full_content(extracted_data['טקסט_מלא'])
        elif is_ocr and isinstance(extracted_data, list):
            # Handle case where extracted_data is a list
            for item in extracted_data:
                if isinstance(item, dict) and 'טקסט_מלא' in item:
                    logger.info("Applying OCR full content fix for טקסט_מלא field")
                    item['טקסט_מלא'] = fix_ocr_full_content(item['טקסט_מלא'])

        # Build records with metadata
        for data in extracted_data:
            yield data

        logger.info(f"Successfully extracted {len(extracted_data)} entities from {pdf_path}")

    except Exception as e:
        logger.error(f"Failed to process {pdf_path}: {e}")
        raise    
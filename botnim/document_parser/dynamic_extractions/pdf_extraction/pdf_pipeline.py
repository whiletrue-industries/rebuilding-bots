import logging
import os
import json
from pathlib import Path
from typing import List, Dict, Optional
import argparse
import sys
from datetime import datetime

from botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_extraction_config import PDFExtractionConfig
from botnim.document_parser.dynamic_extractions.pdf_extraction.text_extraction import extract_text_from_pdf
from botnim.document_parser.dynamic_extractions.pdf_extraction.field_extraction import extract_fields_from_text
from botnim.document_parser.dynamic_extractions.pdf_extraction.csv_output import write_csv, flatten_for_csv
from botnim.document_parser.dynamic_extractions.pdf_extraction.google_sheets_sync import GoogleSheetsSync

logger = logging.getLogger(__name__)

class PDFExtractionPipeline:
    def __init__(self, config_path: str, openai_client, output_dir: str = "."):
        """
        Initialize the PDF extraction pipeline.
        
        Args:
            config_path: Path to YAML configuration file
            openai_client: OpenAI client for field extraction
            output_dir: Directory for output files
        """
        self.config = PDFExtractionConfig.from_yaml(config_path)
        self.openai_client = openai_client
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        logger.info(f"Initialized pipeline with {len(self.config.sources)} sources")
    
    def process_pdf_file(self, pdf_path: str, source_config) -> Optional[Dict]:
        """
        Process a single PDF file through the extraction pipeline.
        
        Args:
            pdf_path: Path to PDF file
            source_config: Source configuration from YAML
            
        Returns:
            Dictionary with extracted data or None if failed
        """
        try:
            logger.info(f"Processing PDF: {pdf_path}")
            
            # Step 1: Extract text from PDF
            logger.info("Extracting text from PDF...")
            text = extract_text_from_pdf(pdf_path, self.openai_client)
            if not text.strip():
                logger.error("No text extracted from PDF")
                return None
            
            logger.info(f"Extracted {len(text)} characters from PDF")
            
            # Step 2: Extract structured fields using LLM
            logger.info("Extracting structured fields...")
            extracted_fields = extract_fields_from_text(text, source_config, self.openai_client)
            
            if "error" in extracted_fields:
                logger.error(f"Field extraction failed: {extracted_fields['error']}")
                return None
            
            # Step 3: Prepare output data
            # Add metadata
            metadata = {}
            for key, value in source_config.metadata.items():
                if value == "{pdf_url}":
                    metadata[key] = pdf_path  # For now, use file path as URL
                elif value == "{download_date}":
                    metadata[key] = datetime.now().isoformat()
                else:
                    metadata[key] = value
            
            result = {
                "fields": extracted_fields,
                "metadata": metadata,
                "source_name": source_config.name,
                "pdf_path": pdf_path,
                "processed_at": datetime.now().isoformat()
            }
            
            logger.info(f"Successfully processed PDF: {pdf_path}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to process PDF {pdf_path}: {e}")
            return None
    
    def find_pdf_files(self, source_config) -> List[str]:
        """
        Find PDF files matching the source pattern.
        
        Args:
            source_config: Source configuration from YAML
            
        Returns:
            List of PDF file paths
        """
        pattern = source_config.file_pattern
        # For now, assume pattern is relative to current directory
        # In production, this would be more sophisticated
        pdf_files = []
        
        # Simple glob pattern matching
        if "*" in pattern:
            base_dir = pattern.split("*")[0]
            if base_dir:
                search_dir = Path(base_dir)
                if search_dir.exists():
                    for pdf_file in search_dir.glob("*.pdf"):
                        pdf_files.append(str(pdf_file))
            else:
                # Search current directory
                for pdf_file in Path(".").glob("*.pdf"):
                    pdf_files.append(str(pdf_file))
        else:
            # Exact path
            if Path(pattern).exists():
                pdf_files.append(pattern)
        
        logger.info(f"Found {len(pdf_files)} PDF files for source '{source_config.name}'")
        return pdf_files
    
    def process_source(self, source_name: str, upload_to_sheets: bool = False,
                      sheets_credentials: Optional[str] = None,
                      spreadsheet_id: Optional[str] = None,
                      replace_sheet: bool = False) -> bool:
        """
        Process all PDF files for a specific source.
        
        Args:
            source_name: Name of the source to process
            upload_to_sheets: Whether to upload results to Google Sheets
            sheets_credentials: Path to Google Sheets credentials
            spreadsheet_id: Google Sheets spreadsheet ID
            replace_sheet: Whether to replace existing sheet content
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Find source configuration
            source_config = next((s for s in self.config.sources if s.name == source_name), None)
            if not source_config:
                logger.error(f"Source '{source_name}' not found in configuration")
                return False
            
            logger.info(f"Processing source: {source_name}")
            
            # Find PDF files
            pdf_files = self.find_pdf_files(source_config)
            if not pdf_files:
                logger.warning(f"No PDF files found for source '{source_name}'")
                return True  # Not an error, just no files to process
            
            # Process each PDF file
            results = []
            for pdf_file in pdf_files:
                result = self.process_pdf_file(pdf_file, source_config)
                if result:
                    results.append(result)
            
            if not results:
                logger.warning(f"No PDF files were successfully processed for source '{source_name}'")
                return True
            
            logger.info(f"Successfully processed {len(results)} PDF files for source '{source_name}'")
            
            # Generate CSV output
            fieldnames = [f.name for f in source_config.fields]
            flat_data = []
            for result in results:
                row = flatten_for_csv(result, fieldnames)
                flat_data.append(row)
            
            # Write CSV file
            csv_path = write_csv(flat_data, fieldnames, source_name, str(self.output_dir))
            logger.info(f"CSV output written to: {csv_path}")
            
            # Upload to Google Sheets if requested
            if upload_to_sheets and sheets_credentials and spreadsheet_id:
                try:
                    sheets_sync = GoogleSheetsSync(sheets_credentials)
                    sheet_name = source_name.replace(" ", "_").replace("-", "_")
                    success = sheets_sync.upload_csv_to_sheet(
                        csv_path, spreadsheet_id, sheet_name, replace_sheet
                    )
                    if success:
                        logger.info(f"Successfully uploaded to Google Sheets: {sheet_name}")
                    else:
                        logger.error(f"Failed to upload to Google Sheets: {sheet_name}")
                        return False
                except Exception as e:
                    logger.error(f"Google Sheets upload failed: {e}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process source '{source_name}': {e}")
            return False
    
    def process_all_sources(self, upload_to_sheets: bool = False,
                           sheets_credentials: Optional[str] = None,
                           spreadsheet_id: Optional[str] = None,
                           replace_sheet: bool = False) -> bool:
        """
        Process all sources defined in the configuration.
        
        Args:
            upload_to_sheets: Whether to upload results to Google Sheets
            sheets_credentials: Path to Google Sheets credentials
            spreadsheet_id: Google Sheets spreadsheet ID
            replace_sheet: Whether to replace existing sheet content
            
        Returns:
            True if all sources processed successfully, False otherwise
        """
        success = True
        for source in self.config.sources:
            if not self.process_source(
                source.name, upload_to_sheets, sheets_credentials, 
                spreadsheet_id, replace_sheet
            ):
                success = False
        
        return success

def main():
    parser = argparse.ArgumentParser(description="PDF Extraction and Sync Pipeline")
    parser.add_argument("--config", required=True, help="Path to YAML configuration file")
    parser.add_argument("--source", help="Process specific source (default: process all)")
    parser.add_argument("--output-dir", default=".", help="Output directory for CSV files")
    parser.add_argument("--upload-sheets", action="store_true", help="Upload results to Google Sheets")
    parser.add_argument("--sheets-credentials", help="Path to Google Sheets credentials JSON")
    parser.add_argument("--spreadsheet-id", help="Google Sheets spreadsheet ID")
    parser.add_argument("--replace-sheet", action="store_true", help="Replace existing sheet content")
    parser.add_argument("--environment", default="staging", choices=["staging", "production"], help="API environment (default: staging)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize OpenAI client using existing pattern
        from botnim.document_parser.dynamic_extractions.extract_structure import get_openai_client
        openai_client = get_openai_client(args.environment if hasattr(args, 'environment') else 'staging')
        
        # Initialize pipeline
        pipeline = PDFExtractionPipeline(args.config, openai_client, args.output_dir)
        
        # Process sources
        if args.source:
            success = pipeline.process_source(
                args.source, args.upload_sheets, args.sheets_credentials,
                args.spreadsheet_id, args.replace_sheet
            )
        else:
            success = pipeline.process_all_sources(
                args.upload_sheets, args.sheets_credentials,
                args.spreadsheet_id, args.replace_sheet
            )
        
        if success:
            print("Pipeline completed successfully")
        else:
            print("Pipeline completed with errors")
            sys.exit(1)
            
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
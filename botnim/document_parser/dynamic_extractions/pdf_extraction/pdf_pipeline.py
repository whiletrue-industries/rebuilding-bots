import logging
import os
import json
import glob
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import time

from .pdf_extraction_config import PDFExtractionConfig
from .text_extraction import extract_text_from_pdf
from .field_extraction import extract_fields_from_text
from .csv_output import write_csv, flatten_for_csv
from .metrics import MetricsCollector, ExtractionMetrics
from .metrics import ExtractionMetrics
from .google_sheets_sync import GoogleSheetsSync
from botnim.config import get_logger

logger = get_logger(__name__)

class PDFExtractionPipeline:
    def __init__(self, config_path: str, openai_client, enable_metrics: bool = True, 
                 google_sheets_config: Optional[Dict] = None):
        """
        Initialize the PDF extraction pipeline.
        
        Args:
            config_path: Path to YAML configuration file
            openai_client: OpenAI client for field extraction
            enable_metrics: Whether to enable performance metrics collection
            google_sheets_config: Optional Google Sheets configuration for automatic upload
        """
        self.config_path = config_path
        self.config = PDFExtractionConfig.from_yaml(config_path)
        self.openai_client = openai_client
        self.google_sheets_config = google_sheets_config
        
        # Initialize metrics collector
        self.metrics = MetricsCollector() if enable_metrics else None
        
        # Initialize Google Sheets sync if configured
        self.google_sheets_sync = None
        if google_sheets_config:
            try:
                self.google_sheets_sync = GoogleSheetsSync(
                    credentials_path=google_sheets_config.get('credentials_path'),
                    use_adc=google_sheets_config.get('use_adc', False)
                )
                logger.info("Google Sheets integration initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Google Sheets integration: {e}")
        
        logger.info(f"Initialized pipeline with {len(self.config.sources)} sources")
    
    def _extract_text_from_pdf(self, pdf_path: str) -> Optional[str]:
        """Extract text from PDF file."""
        logger.info("Extracting text from PDF...")
        try:
            text = extract_text_from_pdf(pdf_path, self.openai_client)
            if not text.strip():
                logger.error("No text extracted from PDF")
                return None
            
            logger.info(f"Extracted {len(text)} characters from PDF")
            return text
        except Exception as e:
            logger.error(f"Text extraction failed for {pdf_path}: {e}")
            return None

    def _extract_fields_from_text(self, text: str, source_config) -> Optional[List[Dict]]:
        """Extract structured fields from text using LLM."""
        logger.info("Extracting structured fields...")
        try:
            extracted_fields_list = extract_fields_from_text(text, source_config, self.openai_client)
            return extracted_fields_list
        except Exception as e:
            logger.error(f"Field extraction failed: {e}")
            return None

    def _prepare_metadata(self, source_config, pdf_path: str, entity_index: int, total_entities: int) -> Dict:
        """Prepare metadata for an entity."""
        metadata = {}
        for key, value in source_config.metadata.items():
            if value == "{pdf_url}":
                metadata[key] = pdf_path  # For now, use file path as URL
            elif value == "{download_date}":
                metadata[key] = datetime.now().isoformat()
            else:
                metadata[key] = value
        
        # Add entity-specific metadata
        if total_entities > 1:
            metadata["entity_number"] = entity_index + 1
            metadata["total_entities"] = total_entities
        
        return metadata

    def _create_result_object(self, extracted_fields: Dict, metadata: Dict, source_config, pdf_path: str, entity_index: int) -> Dict:
        """Create a result object for an entity."""
        return {
            "fields": extracted_fields,
            "metadata": metadata,
            "source_name": source_config.name,
            "pdf_path": pdf_path,
            "entity_index": entity_index,
            "processed_at": datetime.now().isoformat()
        }

    def process_pdf_file(self, pdf_path: str, source_config) -> Optional[List[Dict]]:
        """
        Process a single PDF file and extract structured data.
        
        Args:
            pdf_path: Path to the PDF file
            source_config: Source configuration for extraction rules
            
        Returns:
            List of extracted entities (documents) or None if processing failed
        """
        logger.info(f"Processing PDF file: {pdf_path}")
        
        # Start timing for metrics
        start_time = time.time()
        
        # Extract text from PDF
        text = self._extract_text_from_pdf(pdf_path)
        if not text:
            return None
        
        # Extract structured fields
        extracted_fields_list = self._extract_fields_from_text(text, source_config)
        if not extracted_fields_list:
            return None
        
        # Process each extracted entity
        results = []
        for entity_index, extracted_fields in enumerate(extracted_fields_list):
            # Prepare metadata
            metadata = self._prepare_metadata(source_config, pdf_path, entity_index, len(extracted_fields_list))
            
            # Create result object
            result = self._create_result_object(extracted_fields, metadata, source_config, pdf_path, entity_index)
            results.append(result)
            
            # Record metrics
            if self.metrics:
                processing_time = time.time() - start_time
                metrics = ExtractionMetrics(
                    pdf_path=pdf_path,
                    source_name=source_config.name,
                    text_extraction_time=0.0,  # We don't track this separately anymore
                    field_extraction_time=processing_time,
                    total_processing_time=processing_time,
                    text_length=len(text),
                    entities_extracted=len(extracted_fields_list),
                    success=True
                )
                self.metrics.record_extraction(metrics)
        
        logger.info(f"Successfully extracted {len(results)} entities from {pdf_path}")
        return results

    def find_pdf_files(self, source_config, input_dir: str) -> List[str]:
        """
        Find PDF files matching the source pattern in the input directory.
        
        Args:
            source_config: Source configuration with file pattern
            input_dir: Directory to search for PDF files
            
        Returns:
            List of matching PDF file paths
        """
        input_path = Path(input_dir)
        if not input_path.exists():
            logger.error(f"Input directory does not exist: {input_dir}")
            return []
        
        # Resolve pattern relative to input directory
        pattern = source_config.file_pattern
        if not os.path.isabs(pattern):
            pattern = str(input_path / pattern)
        
        # Find matching files
        pdf_files = []
        try:
            matching_files = glob.glob(pattern)
            
            for file_path in matching_files:
                if os.path.isfile(file_path) and file_path.lower().endswith('.pdf'):
                    pdf_files.append(file_path)
                    logger.info(f"Found PDF file: {file_path}")
            
        except Exception as e:
            logger.error(f"Error finding PDF files with pattern '{pattern}': {e}")
        
        if not pdf_files:
            logger.warning(f"No PDF files found for pattern '{pattern}' in directory '{input_dir}'")
        
        logger.info(f"Found {len(pdf_files)} PDF files for source '{source_config.name}'")
        return pdf_files

    def load_existing_data(self, input_dir: str) -> Dict[str, List[Dict]]:
        """
        Load existing data from input.csv if it exists.
        
        Args:
            input_dir: Directory containing input.csv
            
        Returns:
            Dictionary mapping source names to existing data
        """
        input_csv_path = Path(input_dir) / "input.csv"
        if not input_csv_path.exists():
            logger.info("No existing input.csv found, starting with empty data")
            return {}
        
        try:
            import csv
            existing_data = {}
            with open(input_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    source_name = row.get('source_name', 'unknown')
                    if source_name not in existing_data:
                        existing_data[source_name] = []
                    existing_data[source_name].append(row)
            
            logger.info(f"Loaded {sum(len(data) for data in existing_data.values())} existing records from input.csv")
            return existing_data
        except Exception as e:
            logger.error(f"Error loading existing data from input.csv: {e}")
            return {}

    def process_source(self, source_name: str, input_dir: str) -> bool:
        """
        Process all PDF files for a specific source.
        
        Args:
            source_name: Name of the source to process
            input_dir: Directory containing input files
            
        Returns:
            True if processing was successful, False otherwise
        """
        # Find source configuration
        source_config = None
        for source in self.config.sources:
            if source.name == source_name:
                source_config = source
                break
        
        if not source_config:
            logger.error(f"Source '{source_name}' not found in configuration")
            return False
        
        logger.info(f"Processing source: {source_name}")
        
        # Find PDF files
        pdf_files = self.find_pdf_files(source_config, input_dir)
        if not pdf_files:
            logger.warning(f"No PDF files found for source '{source_name}'")
            return True  # Not an error, just no files to process
        
        # Load existing data
        existing_data = self.load_existing_data(input_dir)
        source_data = existing_data.get(source_name, [])
        
        # Process each PDF file
        all_results = []
        processed_count = 0
        failed_count = 0
        
        for pdf_path in pdf_files:
            results = self.process_pdf_file(pdf_path, source_config)
            if results:
                all_results.extend(results)
                processed_count += 1
            else:
                failed_count += 1
                logger.warning(f"Failed to process PDF: {pdf_path}")
        
        # Consider the source successful if we processed at least some files
        success = processed_count > 0
        if failed_count > 0:
            logger.warning(f"Source '{source_name}': {processed_count} files processed successfully, {failed_count} files failed")
        else:
            logger.info(f"Source '{source_name}': All {processed_count} files processed successfully")
        
        # Combine existing and new data
        combined_data = source_data + all_results
        
        # Write output CSV
        if combined_data:
            fieldnames = [field.name for field in source_config.fields]
            # Add metadata fields
            for key in source_config.metadata.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
            # Add processing metadata
            for key in ['source_name', 'pdf_path', 'entity_index', 'processed_at']:
                if key not in fieldnames:
                    fieldnames.append(key)
            
            # Flatten data for CSV
            flattened_data = [flatten_for_csv(result, fieldnames) for result in combined_data]
            
            # Write to output.csv in the same directory
            output_path = write_csv(flattened_data, fieldnames, source_name, input_dir)
            logger.info(f"Wrote {len(flattened_data)} records to {output_path}")
        
        return success

    def process_all_sources(self, input_dir: str) -> bool:
        """
        Process all sources defined in the configuration.
        
        Args:
            input_dir: Directory containing input files
            
        Returns:
            True if all sources processed successfully, False otherwise
        """
        success = True
        for source in self.config.sources:
            if not self.process_source(source.name, input_dir):
                success = False
        
        return success

    def save_metrics(self, output_path: Optional[str] = None):
        """Save performance metrics to a JSON file."""
        if self.metrics:
            metrics_path = output_path or "pipeline_metrics.json"
            self.metrics.save_metrics(metrics_path)
            logger.info(f"Performance metrics saved to: {metrics_path}")
    
    def print_performance_summary(self):
        """Print a human-readable performance summary."""
        if self.metrics:
            self.metrics.print_summary()
        else:
            logger.info("Metrics collection is disabled")
    
    def get_performance_summary(self) -> Optional[Dict]:
        """Get performance summary as a dictionary."""
        if self.metrics:
            return self.metrics.get_pipeline_summary()
        return None

    def upload_to_google_sheets(self, csv_path: str, spreadsheet_id: str, sheet_name: str, 
                               replace_existing: bool = False) -> bool:
        """
        Upload CSV results to Google Sheets.
        
        Args:
            csv_path: Path to the CSV file to upload
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            replace_existing: If True, replace entire sheet. If False, append new rows.
            
        Returns:
            True if successful, False otherwise
        """
        if not self.google_sheets_sync:
            logger.error("Google Sheets integration not configured")
            return False
        
        try:
            success = self.google_sheets_sync.upload_csv_to_sheet(
                csv_path, spreadsheet_id, sheet_name, replace_existing
            )
            if success:
                logger.info(f"Successfully uploaded CSV to Google Sheets: {sheet_name}")
            else:
                logger.error("Failed to upload CSV to Google Sheets")
            return success
        except Exception as e:
            logger.error(f"Error uploading to Google Sheets: {e}")
            return False

    def process_with_google_sheets_upload(self, input_dir: str, spreadsheet_id: str, 
                                        sheet_name: str, replace_existing: bool = False) -> bool:
        """
        Process all sources and automatically upload results to Google Sheets.
        
        Args:
            input_dir: Directory containing input files
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet to create/update
            replace_existing: If True, replace entire sheet. If False, append new rows.
            
        Returns:
            True if processing and upload were successful, False otherwise
        """
        # Process all sources
        success = self.process_all_sources(input_dir)
        
        if not success:
            logger.error("PDF processing failed, skipping Google Sheets upload")
            return False
        
        # Find the output CSV file
        output_csv = Path(input_dir) / "output.csv"
        if not output_csv.exists():
            logger.error("Output CSV file not found after processing")
            return False
        
        # Upload to Google Sheets
        return self.upload_to_google_sheets(str(output_csv), spreadsheet_id, sheet_name, replace_existing)

 
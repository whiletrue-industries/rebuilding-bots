"""
PDF extraction pipeline with CSV contract pattern.

This module implements a clean separation of concerns:
- PDF pipeline focuses only on PDF processing
- Input: CSV file + PDF files + metadata
- Output: Updated CSV file
- No cloud storage coupling
"""

import logging
import os
import json
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime

from .pdf_extraction_config import PDFExtractionConfig
from .text_extraction import extract_text_from_pdf
from .field_extraction import extract_fields_from_text
from .csv_output import write_csv, read_csv, write_csv_by_source
from .metrics import MetricsCollector
from .metadata_handler import MetadataHandler
from .exceptions import PDFExtractionError, PDFTextExtractionError, FieldExtractionError
from .metrics import ExtractionMetrics

from botnim.config import get_logger
logger = get_logger(__name__)

class PDFExtractionPipeline:
    """
    PDF extraction pipeline following CSV contract pattern.
    
    Input Contract:
    - input.csv: Existing data (optional)
    - *.pdf: PDF files to process
    - *.pdf.metadata.json: Metadata for each PDF (optional)
    
    Output Contract:
    - output.csv: Updated data with extracted fields
        """
    
    def __init__(self, config_path: str, openai_client, enable_metrics: bool = True):
        """
        Initialize PDF extraction pipeline.
        
        Args:
            config_path: Path to YAML configuration file
            openai_client: OpenAI client for field extraction
            enable_metrics: Whether to collect performance metrics
        """
        self.config = PDFExtractionConfig.from_yaml(config_path)
        self.openai_client = openai_client
        self.metrics = MetricsCollector() if enable_metrics else None
        
        logger.info(f"Initialized pipeline with {len(self.config.sources)} sources")
    
    def process_directory(self, input_dir: str) -> bool:
        """
        Process a directory following the CSV contract pattern.
        
        Args:
            input_dir: Directory containing input.csv, PDF files, and metadata
            
        Returns:
            True if processing was successful
        """
        input_path = Path(input_dir)
        
        # Initialize metadata handler
        metadata_handler = MetadataHandler(str(input_path))
        
        # Read existing data if available
        input_csv_path = input_path / "input.csv"
        existing_data = []
        if input_csv_path.exists():
            existing_data = read_csv(str(input_csv_path))
            logger.info(f"Loaded {len(existing_data)} existing records from input.csv")
        
        # Process all sources
        all_results = []
        for source_config in self.config.sources:
            logger.info(f"Processing source: {source_config.name}")
            
            # Find PDF files for this source
            pdf_files = self._find_pdf_files(input_path, source_config.file_pattern)
            if not pdf_files:
                logger.warning(f"No PDF files found for source '{source_config.name}'")
                continue
            
            # Process each PDF file
            source_results = []
            for pdf_file in pdf_files:
                try:
                    result = self._process_single_pdf(
                        pdf_file, source_config, metadata_handler
                    )
                    if result:
                        source_results.extend(result)
                except Exception as e:
                    logger.error(f"Failed to process {pdf_file}: {e}")
                    if self.metrics:
                        metrics = ExtractionMetrics(
                            pdf_path=str(pdf_file),
                            source_name=source_config.name,
                            text_extraction_time=0.0,
                            field_extraction_time=0.0,
                            total_processing_time=0.0,
                            text_length=0,
                            entities_extracted=0,
                            success=False,
                            error_message=str(e)
                        )
                        self.metrics.record_extraction(metrics)
            
            # Merge with existing data
            all_results.extend(source_results)
            logger.info(f"Source '{source_config.name}': {len(source_results)} records processed")
        
        # Write output CSV files by source
        if all_results:
            # Get source configurations for field definitions
            source_configs = []
            for source_config in self.config.sources:
                # Convert FieldConfig objects to dictionaries
                fields_dict = []
                for field in source_config.fields:
                    fields_dict.append({
                        'name': field.name,
                        'description': getattr(field, 'description', ''),
                        'example': getattr(field, 'example', ''),
                        'hint': getattr(field, 'hint', '')
                    })
                
                source_configs.append({
                    'name': source_config.name,
                    'fields': fields_dict
                })
            
            # Write separate CSV files for each source
            csv_files = write_csv_by_source(all_results, str(input_path), source_configs)
            logger.info(f"Wrote {len(csv_files)} source-specific CSV files")
            
            # Also write combined CSV for backward compatibility
            output_csv_path = input_path / "output.csv"
            write_csv(all_results, str(output_csv_path))
            logger.info(f"Wrote {len(all_results)} records to output.csv")
        
        # Save metrics if enabled
        if self.metrics:
            metrics_path = input_path / "pipeline_metrics.json"
            self.metrics.save_metrics(str(metrics_path))
            logger.info(f"Performance metrics saved to: {metrics_path}")
        
        return True
    
    def _find_pdf_files(self, input_path: Path, file_pattern: str) -> List[Path]:
        """Find PDF files matching the pattern in the input directory."""
        import glob
        
        # Resolve pattern relative to input directory
        if not os.path.isabs(file_pattern):
            pattern = str(input_path / file_pattern)
        
        pdf_files = []
        for file_path in glob.glob(pattern):
            if Path(file_path).suffix.lower() == '.pdf':
                pdf_files.append(Path(file_path))
        
        logger.info(f"Found {len(pdf_files)} PDF files for pattern '{file_pattern}'")
        return pdf_files
    
    def _process_single_pdf(self, pdf_path: Path, source_config, metadata_handler) -> List[Dict[str, Any]]:
        """
        Process a single PDF file and extract structured data.
        
        Args:
            pdf_path: Path to PDF file
            source_config: Source configuration
            metadata_handler: Metadata handler
            
        Returns:
            List of extracted records
        """
        start_time = datetime.now()
        
        try:
            # Extract text from PDF
            logger.info(f"Extracting text from PDF: {pdf_path}")
            text = extract_text_from_pdf(str(pdf_path))
            
            # Extract structured fields
            logger.info("Extracting structured fields...")
            extracted_data = extract_fields_from_text(
                text, source_config, self.openai_client
            )
            
            # Load file metadata
            file_metadata = metadata_handler.load_metadata_for_pdf(pdf_path)
            
            # Merge with config metadata if available
            config_metadata = getattr(source_config, 'metadata', {})
            if config_metadata:
                metadata = metadata_handler.merge_config_metadata(file_metadata, config_metadata, pdf_path)
                logger.info(f"Merged config metadata for {pdf_path.name}")
            else:
                metadata = file_metadata
            
            # Build records with metadata
            records = []
            for data in extracted_data:
                record = {
                    'source_name': source_config.name,
                    'source_url': metadata.get('source_url', ''),
                    'extraction_date': datetime.now().isoformat(),
                    'input_file': str(pdf_path),
                    **data
                }
                records.append(record)
            
            # Record metrics
            if self.metrics:
                processing_time = (datetime.now() - start_time).total_seconds()
                metrics = ExtractionMetrics(
                    pdf_path=str(pdf_path),
                    source_name=source_config.name,
                    text_extraction_time=0.0,  # We don't track this separately
                    field_extraction_time=processing_time,
                    total_processing_time=processing_time,
                    text_length=len(text),
                    entities_extracted=len(records),
                    success=True
                )
                self.metrics.record_extraction(metrics)
            
            logger.info(f"Successfully extracted {len(records)} entities from {pdf_path}")
            return records
            
        except Exception as e:
            logger.error(f"Failed to process {pdf_path}: {e}")
            if self.metrics:
                self.metrics.record_failure(str(pdf_path), source_config.name, str(e))
            raise

 
"""
PDF-to-Spreadsheet Pre-processing Pipeline for Automated Sync System.

This module orchestrates the extraction of structured data from multiple PDFs,
compiles it into a single CSV file, and uploads it to a Google Sheet.
This sheet then becomes a new source for the main sync process.
"""

import os
import tempfile
import hashlib
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

from ..document_parser.pdf_processor.csv_output import write_csv

from ..config import get_logger
from .config import ContentSource, PDFPipelineConfig
from .config import SpreadsheetSourceConfig
from .cache import SyncCache
from .pdf_discovery import PDFDiscoveryService, PDFDownloadManager
from ..document_parser.pdf_processor.pdf_pipeline import PDFExtractionPipeline
from ..document_parser.pdf_processor.google_sheets_service import GoogleSheetsService
from ..document_parser.pdf_processor.exceptions import PDFExtractionError
from ..sync.config_adapters import PDFConfigAdapter

logger = get_logger(__name__)


class PDFPipelineProcessor:
    """
    Orchestrates the PDF-to-Spreadsheet pre-processing pipeline.
    """

    def __init__(self, cache: SyncCache, openai_client: Any):
        """
        Initialize the processor.

        Args:
            cache: The main sync cache.
            openai_client: The OpenAI client for processing.
        """
        self.cache = cache
        self.openai_client = openai_client
        self.discovery_service = PDFDiscoveryService(self.cache, vector_store=None) # Vector store not needed for pre-processing
        self.download_manager = PDFDownloadManager()

    def process_pipeline_source(self, source: ContentSource) -> Dict[str, Any]:
        """
        Execute the full PDF-to-Spreadsheet pipeline for a given source.

        Args:
            source: A ContentSource of type PDF_PIPELINE.

        Returns:
            A dictionary with the results of the pipeline execution.
        """
        if source.type != "pdf_pipeline" or not source.pdf_pipeline_config:
            logger.error(f"Source {source.id} is not a valid PDF pipeline source.")
            return {"status": "failed", "error": "Invalid source type"}

        config = source.pdf_pipeline_config
        logger.info(f"Starting PDF-to-Spreadsheet pipeline for source: {source.id}")

        results = {
            "source_id": source.id,
            "status": "running",
            "discovered_pdfs": 0,
            "processed_pdfs": 0,
            "failed_pdfs": 0,
            "output_csv_path": None,
            "upload_status": "pending",
            "created_spreadsheet_source": None,  # New field for the created spreadsheet source
            "errors": [],
        }

        try:
            # 1. Discover PDFs
            pdf_infos = self._discover_pdfs(config)
            results["discovered_pdfs"] = len(pdf_infos)
            if not pdf_infos:
                logger.info(f"No PDFs found for pipeline source {source.id}.")
                results["status"] = "completed_no_data"
                return results

            # 2. Process all discovered PDFs to generate a CSV
            output_csv_path, processed_count, failed_count, errors = self._process_pdfs_to_csv(pdf_infos, config)
            results["processed_pdfs"] = processed_count
            results["failed_pdfs"] = failed_count
            results["output_csv_path"] = str(output_csv_path)
            results["errors"].extend(errors)

            if not output_csv_path or not processed_count:
                raise PDFProcessingError("Failed to generate CSV from any of the discovered PDFs.")

            # 3. Upload the resulting CSV to Google Sheets
            upload_success = self._upload_csv_to_gdrive(output_csv_path, config)
            if upload_success:
                results["upload_status"] = "completed"
                results["status"] = "completed"
                
                # 4. Create a new spreadsheet source for the main sync loop to process
                created_source = self._create_spreadsheet_source_from_pipeline(source, config)
                results["created_spreadsheet_source"] = created_source
                
                logger.info(f"Successfully completed PDF-to-Spreadsheet pipeline for source: {source.id}")
                logger.info(f"Created new spreadsheet source: {created_source.id} for main sync processing")
            else:
                raise PDFProcessingError("Failed to upload the generated CSV to Google Sheets.")

        except Exception as e:
            logger.error(f"Error in PDF pipeline for source {source.id}: {e}", exc_info=True)
            results["status"] = "failed"
            results["errors"].append(str(e))
        finally:
            self.download_manager.cleanup_temp_files()

        return results

    def _discover_pdfs(self, config: PDFPipelineConfig) -> List[Dict[str, Any]]:
        """Discover PDFs based on the input configuration."""
        input_source_config = ContentSource(
            id=f"{config.input_config.url}-discovery",
            name="PDF Discovery for Pipeline",
            type="pdf",
            pdf_config=config.input_config
        )
        if config.input_config.is_index_page:
            return self.discovery_service.discover_pdfs_from_index_page(input_source_config)
        else:
            # Create a mock structure for a single PDF
             url = config.input_config.url
             filename = self.discovery_service._extract_filename(url)
             url_hash = hashlib.sha256(url.encode()).hexdigest()
             return [{'url': url, 'filename': filename, 'url_hash': url_hash}]

    def _process_pdfs_to_csv(self, pdf_infos: List[Dict[str, Any]], config: PDFPipelineConfig) -> (Optional[Path], int, int, List[str]):
        """Download and process multiple PDFs, then combine results into a single CSV."""
        
        all_extracted_data = []
        processed_count = 0
        failed_count = 0
        errors = []

        # Adapt the sync config to the format the PDF processor expects
        processor_config = PDFConfigAdapter.sync_to_processor_config(config.processing_config, "pdf_pipeline_run")
        pdf_pipeline = PDFExtractionPipeline(processor_config, self.openai_client)

        for pdf_info in pdf_infos:
            temp_pdf_path = self.download_manager.download_pdf(pdf_info, headers=config.input_config.headers)
            if not temp_pdf_path:
                failed_count += 1
                errors.append(f"Failed to download {pdf_info.get('url')}")
                continue
            
            try:
                # Use the existing PDF pipeline to process one file
                extracted_data, _ = pdf_pipeline.process_single_pdf(str(temp_pdf_path))
                if extracted_data:
                    all_extracted_data.append(extracted_data)
                    processed_count += 1
                else:
                    failed_count +=1
                    errors.append(f"No data extracted from {pdf_info.get('filename')}")
            except Exception as e:
                failed_count += 1
                errors.append(f"Failed to process {pdf_info.get('filename')}: {e}")
            finally:
                if temp_pdf_path.exists():
                    os.remove(temp_pdf_path)

        if not all_extracted_data:
            return None, processed_count, failed_count, errors

        # Write combined data to a temporary CSV file
        temp_csv_path = Path(tempfile.gettempdir()) / f"{config.output_config.sheet_name.replace(' ', '_')}.csv"
        
        # Flatten the data - each item in all_extracted_data is a list of records
        flattened_data = []
        for pdf_records in all_extracted_data:
            if isinstance(pdf_records, list):
                flattened_data.extend(pdf_records)
            else:
                flattened_data.append(pdf_records)
        
        # Use the existing CSV utility
        write_csv(flattened_data, str(temp_csv_path))
            
        logger.info(f"Successfully created CSV at {temp_csv_path} with {len(flattened_data)} rows.")
        return temp_csv_path, processed_count, failed_count, errors


    def _upload_csv_to_gdrive(self, csv_path: Path, config: PDFPipelineConfig) -> bool:
        """Upload a CSV file to the specified Google Sheet."""
        output_conf = config.output_config
        try:
            g_service = GoogleSheetsService(
                credentials_path=output_conf.credentials_path,
                use_adc=output_conf.use_adc
            )
            
            success = g_service.upload_csv_to_sheet(
                csv_path=str(csv_path),
                spreadsheet_id=output_conf.spreadsheet_id,
                sheet_name=output_conf.sheet_name,
                replace_existing=True  # Always replace the sheet with the new data
            )
            return success
        except Exception as e:
            logger.error(f"Failed to upload {csv_path} to Google Sheets: {e}")
            return False

    def _create_spreadsheet_source_from_pipeline(self, pipeline_source: ContentSource, config: PDFPipelineConfig) -> ContentSource:
        """
        Create a new spreadsheet source from the PDF pipeline results.
        
        This allows the main sync loop to process the newly created Google Sheet
        as a regular spreadsheet source.
        
        Args:
            pipeline_source: The original PDF pipeline source
            config: The PDF pipeline configuration
            
        Returns:
            A new ContentSource of type SPREADSHEET
        """
        
        # Create a unique ID for the new spreadsheet source
        spreadsheet_source_id = f"{pipeline_source.id}-generated-spreadsheet"
        
        # Create spreadsheet configuration
        spreadsheet_config = SpreadsheetSourceConfig(
            url=f"https://docs.google.com/spreadsheets/d/{config.output_config.spreadsheet_id}",
            sheet_name=config.output_config.sheet_name,
            credentials_path=config.output_config.credentials_path,
            use_adc=config.output_config.use_adc
        )
        
        # Create the new spreadsheet source
        spreadsheet_source = ContentSource(
            id=spreadsheet_source_id,
            name=f"Generated Spreadsheet from {pipeline_source.name}",
            description=f"Auto-generated spreadsheet source from PDF pipeline: {pipeline_source.id}",
            type="spreadsheet",
            spreadsheet_config=spreadsheet_config,
            enabled=True,
            priority=pipeline_source.priority + 1,  # Process after the pipeline
            tags=pipeline_source.tags + ["auto-generated", "pdf-pipeline-output"],
            metadata={
                "generated_from_pipeline": pipeline_source.id,
                "generation_timestamp": time.time(),
                "original_pipeline_config": config.model_dump()
            }
        )
        
        return spreadsheet_source


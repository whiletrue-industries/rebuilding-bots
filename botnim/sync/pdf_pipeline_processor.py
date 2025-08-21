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
import yaml
import csv
import shutil
from pathlib import Path
from typing import Dict, List, Any, Optional

from ..document_parser.pdf_processor.csv_output import write_csv
from ..document_parser.pdf_processor.pdf_pipeline import PDFExtractionPipeline
from ..document_parser.pdf_processor.google_sheets_service import GoogleSheetsService

from ..config import get_logger
from .config import ContentSource, PDFSourceConfig
from .config import SpreadsheetSourceConfig, SourceType, VersioningStrategy, FetchStrategy
from .cache import SyncCache
from .pdf_discovery import PDFDiscoveryService, PDFDownloadManager

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
            source: A ContentSource of type PDF_PIPELINE with Open Budget configuration.

        Returns:
            A dictionary with the results of the pipeline execution.
        """
        if source.type != "pdf_pipeline" or not source.pdf_config:
            logger.error(f"Source {source.id} is not a valid PDF pipeline source with Open Budget configuration.")
            return {"status": "failed", "error": "Invalid source type or missing pdf_config"}

        pdf_config = source.pdf_config
        logger.info(f"Starting Open Budget PDF-to-Spreadsheet pipeline for source: {source.id}")

        results = {
            "source_id": source.id,
            "status": "running",
            "discovered_pdfs": 0,
            "processed_pdfs": 0,
            "failed_pdfs": 0,
            "output_csv_path": None,
            "upload_status": "pending",
            "errors": [],
        }

        try:
            # 1. Process PDFs using the existing PDFExtractionPipeline
            output_csv_path, processed_count, failed_count, errors = self._process_open_budget_pdfs(source, pdf_config)
            results["processed_pdfs"] = processed_count
            results["failed_pdfs"] = failed_count
            results["output_csv_path"] = str(output_csv_path) if output_csv_path else None
            results["errors"].extend(errors)

            if not output_csv_path or not processed_count:
                logger.warning(f"No PDFs processed successfully for source {source.id}")
                results["status"] = "completed_no_data"
                return results

            # 2. Upload the resulting CSV to Google Sheets
            upload_success, spreadsheet_id = self._upload_csv_to_gdrive(output_csv_path, source)
            if upload_success:
                results["upload_status"] = "completed"
                
                results["status"] = "completed"
                logger.info(f"✅ PDF pipeline completed for {source.id}: {processed_count} PDFs processed, uploaded to Google Sheets")
            else:
                results["upload_status"] = "failed"
                results["status"] = "failed"
                logger.error(f"❌ Failed to upload CSV to Google Sheets for source {source.id}")

        except Exception as e:
            error_msg = f"PDF pipeline failed for source {source.id}: {e}"
            logger.error(error_msg, exc_info=True)
            results["status"] = "failed"
            results["errors"].append(error_msg)

        return results

    def _process_open_budget_pdfs(self, source: ContentSource, pdf_config) -> tuple:
        """
        Process Open Budget PDFs using the existing PDFExtractionPipeline with proper change detection.
        
        Args:
            source: The PDF source configuration
            pdf_config: The PDF configuration with Open Budget URLs
            
        Returns:
            Tuple of (output_csv_path, processed_count, failed_count, errors)
        """
        try:
            # Create a temporary directory for processing
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Create input directory structure
                input_dir = temp_path / "input"
                input_dir.mkdir(exist_ok=True)
                
                # Create output directory
                output_dir = temp_path / "output"
                output_dir.mkdir(exist_ok=True)
                
                # 1. Try to read existing Google Spreadsheet as input.csv for change detection
                existing_spreadsheet_id = self._get_existing_spreadsheet_id(source)
                if existing_spreadsheet_id:
                    logger.info(f"Found existing spreadsheet {existing_spreadsheet_id} for change detection")
                    # Get GID from output config if available
                    gid = getattr(source.pdf_config.output_config, 'gid', None) if source.pdf_config and source.pdf_config.output_config else None
                    input_csv_path = self._download_spreadsheet_as_csv(existing_spreadsheet_id, source.name, str(input_dir), gid)
                    if input_csv_path:
                        logger.info(f"Downloaded existing data from Google Sheets for change detection")
                    else:
                        logger.warning(f"Failed to download existing data, starting fresh")
                        input_csv_path = None
                else:
                    logger.info(f"No existing spreadsheet found, starting fresh")
                    input_csv_path = None
                
                # 2. Convert the source to PDFExtractionConfig format
                from ..document_parser.pdf_processor.sync_config_adapter import SyncConfigAdapter
                
                # Create a temporary config file with just this source
                temp_config = {
                    "sources": [{
                        "id": source.id,
                        "name": source.name,
                        "description": source.description,
                        "type": "pdf_pipeline",
                        "pdf_config": {
                            "index_csv_url": pdf_config.index_csv_url,
                            "datapackage_url": pdf_config.datapackage_url,
                            "processing": pdf_config.processing.dict() if pdf_config.processing else {}
                        }
                    }]
                }
                
                # Write temporary config
                temp_config_path = temp_path / "temp_config.yaml"
                with open(temp_config_path, 'w', encoding='utf-8') as f:
                    yaml.dump(temp_config, f, default_flow_style=False, allow_unicode=True)
                
                # 3. Initialize the PDF extraction pipeline
                pipeline = PDFExtractionPipeline(
                    config_path=str(temp_config_path),
                    openai_client=self.openai_client,
                    enable_metrics=False
                )
                
                # 4. Process the directory (this will handle change detection internally)
                success = pipeline.process_directory(str(input_dir))
                
                if not success:
                    return None, 0, 0, ["PDF processing failed"]
                
                # 5. Find the output CSV file
                output_csv_path = output_dir / "output.csv"
                if not output_csv_path.exists():
                    return None, 0, 0, ["No output CSV generated"]
                
                # 6. Count processed records
                with open(output_csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    processed_count = sum(1 for _ in reader)
                
                # 7. Copy the output file to a persistent location
                final_output_path = Path(f"/tmp/pdf_pipeline_{source.id}_{int(time.time())}.csv")
                shutil.copy2(output_csv_path, final_output_path)
                
                logger.info(f"✅ PDF processing completed: {processed_count} records processed")
                return final_output_path, processed_count, 0, []
                
        except Exception as e:
            logger.error(f"Error processing Open Budget PDFs for {source.id}: {e}", exc_info=True)
            return None, 0, 0, [str(e)]

    def _get_existing_spreadsheet_id(self, source: ContentSource) -> Optional[str]:
        """
        Get the existing spreadsheet ID for this source if it exists.
        
        Args:
            source: The PDF source configuration
            
        Returns:
            Spreadsheet ID if found, None otherwise
        """
        try:
            # Check if we have a stored spreadsheet ID in source metadata
            if hasattr(source, 'metadata') and source.metadata:
                spreadsheet_id = source.metadata.get('spreadsheet_id')
                if spreadsheet_id:
                    logger.info(f"Found existing spreadsheet ID in metadata: {spreadsheet_id}")
                    return spreadsheet_id
            
            # Check if we have a generated spreadsheet source for this PDF source
            # Look for a spreadsheet source with ID pattern: {source.id}-spreadsheet
            spreadsheet_source_id = f"{source.id}-spreadsheet"
            
            # Try to find this in the cache or configuration
            # For now, we'll use a simple file-based tracking system
            tracking_file = Path(f"/tmp/pdf_pipeline_tracking_{source.id}.txt")
            if tracking_file.exists():
                with open(tracking_file, 'r') as f:
                    stored_spreadsheet_id = f.read().strip()
                    if stored_spreadsheet_id:
                        logger.info(f"Found existing spreadsheet ID from tracking: {stored_spreadsheet_id}")
                        return stored_spreadsheet_id
            
            logger.info(f"No existing spreadsheet found for source {source.id}")
            return None
            
        except Exception as e:
            logger.warning(f"Error checking for existing spreadsheet: {e}")
            return None

    def _store_spreadsheet_id(self, source: ContentSource, spreadsheet_id: str):
        """
        Store the spreadsheet ID for future change detection.
        
        Args:
            source: The PDF source configuration
            spreadsheet_id: The Google Spreadsheet ID to store
        """
        try:
            # Store in a simple file-based tracking system
            tracking_file = Path(f"/tmp/pdf_pipeline_tracking_{source.id}.txt")
            with open(tracking_file, 'w') as f:
                f.write(spreadsheet_id)
            
            logger.info(f"Stored spreadsheet ID {spreadsheet_id} for source {source.id}")
            
        except Exception as e:
            logger.warning(f"Error storing spreadsheet ID: {e}")

    def _download_spreadsheet_as_csv(self, spreadsheet_id: str, sheet_name: str, output_dir: str, gid: str = None) -> Optional[str]:
        """
        Download a Google Spreadsheet as CSV for change detection.
        
        Args:
            spreadsheet_id: The Google Spreadsheet ID
            sheet_name: The sheet name to download
            output_dir: Directory to save the CSV
            gid: The Google Sheets GID (sheet ID) if available
            
        Returns:
            Path to the downloaded CSV file, or None if failed
        """
        try:
            sheets_service = GoogleSheetsService(use_adc=True)
            
            # Download the sheet as CSV using GID if available, otherwise use sheet name
            if gid:
                csv_content = sheets_service.download_sheet_as_csv_by_gid(
                    spreadsheet_id=spreadsheet_id,
                    gid=gid
                )
            else:
                csv_content = sheets_service.download_sheet_as_csv(
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=sheet_name
                )
            
            if csv_content:
                # Save to input.csv for the pipeline
                input_csv_path = os.path.join(output_dir, "input.csv")
                with open(input_csv_path, 'w', encoding='utf-8') as f:
                    f.write(csv_content)
                
                logger.info(f"Downloaded existing data from Google Sheets: {len(csv_content.splitlines())} lines")
                return input_csv_path
            else:
                logger.warning(f"No data found in Google Sheets {spreadsheet_id}")
                return None
                
        except Exception as e:
            logger.warning(f"Failed to download Google Sheets data: {e}")
            return None

    def _upload_csv_to_gdrive(self, csv_path: Path, source: ContentSource) -> tuple:
        """
        Upload CSV to Google Drive/Sheets using configured spreadsheet.
        
        Args:
            csv_path: Path to the CSV file
            source: The source configuration
            
        Returns:
            Tuple of (success, spreadsheet_id)
        """
        try:
            # Get configured spreadsheet details
            output_config = source.pdf_config.output_config
            spreadsheet_id = output_config.spreadsheet_id
            sheet_name = output_config.sheet_name
            gid = getattr(output_config, 'gid', None)  # Get GID if configured
            
            # Validate that spreadsheet_id is not the placeholder
            if spreadsheet_id == "YOUR_SPREADSHEET_ID_HERE":
                error_msg = f"PDF pipeline source '{source.id}' has not been configured with a valid spreadsheet_id. Please update the configuration with your Google Sheets ID."
                logger.error(error_msg)
                return False, None
            
            logger.info(f"Uploading CSV to configured spreadsheet: {spreadsheet_id}, sheet: {sheet_name}, GID: {gid}")
            
            # Upload the CSV using GID if available, otherwise use sheet name
            sheets_service = GoogleSheetsService(use_adc=output_config.use_adc)
            
            if gid:
                # Use GID-based upload
                success = sheets_service.upload_output_csv_by_gid(
                    output_csv_path=str(csv_path),
                    spreadsheet_id=spreadsheet_id,
                    gid=gid,
                    replace_existing=True  # Always replace the entire sheet with new data
                )
            else:
                # Fallback to sheet name-based upload
                success = sheets_service.upload_csv_to_sheet(
                    csv_path=str(csv_path),
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=sheet_name,
                    replace_existing=True  # Always replace the entire sheet with new data
                )
            
            if success:
                logger.info(f"✅ Successfully uploaded CSV to Google Sheets: {spreadsheet_id}")
                return True, spreadsheet_id
            else:
                logger.error(f"❌ Failed to upload CSV to Google Sheets")
                return False, None
                
        except Exception as e:
            logger.error(f"Error uploading CSV to Google Sheets: {e}", exc_info=True)
            return False, None


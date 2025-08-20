"""
PDF extraction pipeline with CSV contract pattern.

This module implements a clean separation of concerns:
- PDF pipeline focuses only on PDF processing
- Input: CSV file + PDF files + metadata
- Output: Updated CSV file
- No cloud storage coupling
"""

import os
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime

from .pdf_extraction_config import PDFExtractionConfig
from .sync_config_adapter import SyncConfigAdapter
from .text_extraction import extract_text_from_pdf, fix_ocr_full_content
from .field_extraction import extract_fields_from_text
from .csv_output import write_csv, read_csv, write_csv_by_source
from .metrics import MetricsCollector
from .metadata_handler import MetadataHandler
from .exceptions import PDFExtractionError, PDFTextExtractionError, FieldExtractionError
from .metrics import ExtractionMetrics
from .open_budget_data_source import OpenBudgetDataSource

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
        # Try to load as sync config first, then as PDF extraction config
        try:
            self.config = SyncConfigAdapter.load_pdf_sources_from_sync_config(config_path)
            logger.info(f"Loaded {len(self.config.sources)} PDF sources from sync config")
        except Exception as e:
            logger.info(f"Failed to load as sync config, trying as PDF extraction config: {e}")
            self.config = PDFExtractionConfig.from_yaml(config_path)
            logger.info(f"Loaded {len(self.config.sources)} sources from PDF extraction config")
        
        self.openai_client = openai_client
        self.metrics = MetricsCollector() if enable_metrics else None
        
        logger.info(f"Initialized pipeline with {len(self.config.sources)} sources")
    
    def process_directory(self, input_dir: str, source_filter: str = None) -> bool:
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
        
        # Process sources (filtered if specified)
        all_results = []
        sources_to_process = self.config.sources
        
        if source_filter:
            sources_to_process = [s for s in self.config.sources if s.name == source_filter]
            if not sources_to_process:
                logger.error(f"Source '{source_filter}' not found in configuration")
                return False
            logger.info(f"Processing only source: {source_filter}")
        
        # Track failures for detailed reporting
        failed_files = []
        failed_sources = []
        sources_with_no_files = []
        
        for source_config in sources_to_process:
            logger.info(f"Processing source: {source_config.name}")
            
            # Process source
            source_results, source_failed_files = self._process_source(
                source_config, existing_data, input_path, metadata_handler
            )
            
            # Track sources with failures
            if source_failed_files:
                failed_sources.append({
                    'source_name': source_config.name,
                    'failed_files': source_failed_files,
                    'total_files': len(source_failed_files) + len(source_results),
                    'successful_files': len(source_results)
                })
            
            # Merge with existing data
            all_results.extend(source_results)
            logger.info(f"Source '{source_config.name}': {len(source_results)} records processed")
        
        # Merge with existing data to preserve unchanged records
        final_results = self._merge_with_existing_data(all_results, existing_data)
        logger.info(f"Final results: {len(final_results)} records (new: {len(all_results)}, existing: {len(existing_data)})")
        
        # Write output CSV files by source
        if final_results:
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
            csv_files = write_csv_by_source(final_results, str(input_path), source_configs)
            logger.info(f"Wrote {len(csv_files)} source-specific CSV files")
            
            # Also write combined CSV for backward compatibility
            output_csv_path = input_path / "output.csv"
            write_csv(final_results, str(output_csv_path))
            logger.info(f"Wrote {len(final_results)} records to output.csv")
        
        # Save metrics if enabled
        if self.metrics:
            metrics_path = input_path / "pipeline_metrics.json"
            self.metrics.save_metrics(str(metrics_path))
            logger.info(f"Performance metrics saved to: {metrics_path}")
        
        # Generate and log final pipeline summary
        self._log_pipeline_summary(final_results, sources_to_process, input_path, failed_files, failed_sources, sources_with_no_files)
        
        return True
    

    
    def _process_single_pdf(self, pdf_path: Path, source_config, metadata_handler, file_info: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Process a single PDF file and extract structured data.
        
        Args:
            pdf_path: Path to PDF file
            source_config: Source configuration
            metadata_handler: Metadata handler
            file_info: Optional file metadata from Open Budget source
            
        Returns:
            List of extracted records
        """
        start_time = datetime.now()
        
        try:
            # Extract text from PDF
            logger.info(f"Extracting text from PDF: {pdf_path}")
            text, is_ocr = extract_text_from_pdf(str(pdf_path))
            
            # Extract structured fields
            logger.info("Extracting structured fields...")
            extracted_data = extract_fields_from_text(
                text, source_config, self.openai_client
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
                
                # Add Open Budget metadata if available
                if file_info:
                    record.update({
                        'url': file_info.get('url', ''),
                        'title': file_info.get('title', ''),
                        'date': file_info.get('date', ''),
                        'revision': file_info.get('revision', '')
                    })
                
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

    def _log_pipeline_summary(self, all_results: List[Dict[str, Any]], sources_processed: List, input_path: Path, failed_files: List[Dict[str, Any]], failed_sources: List[Dict[str, Any]], sources_with_no_files: List[str]):
        """
        Generate and log a comprehensive final pipeline summary.
        
        Args:
            all_results: All extracted records from the pipeline
            sources_processed: List of source configurations that were processed
            input_path: Input directory path
            failed_files: List of failed file processing attempts
            failed_sources: List of sources with processing failures
            sources_with_no_files: List of sources that had no PDF files
        """
        logger.info("=" * 80)
        logger.info("📊 PDF EXTRACTION PIPELINE - FINAL SUMMARY")
        logger.info("=" * 80)
        
        # Basic statistics
        total_records = len(all_results)
        total_sources = len(sources_processed)
        
        logger.info(f"📈 OVERALL STATISTICS:")
        logger.info(f"   • Total records extracted: {total_records}")
        logger.info(f"   • Sources processed: {total_sources}")
        logger.info(f"   • Input directory: {input_path}")
        
        # Source breakdown
        if all_results:
            source_counts = {}
            for record in all_results:
                source_name = record.get('source_name', 'Unknown')
                source_counts[source_name] = source_counts.get(source_name, 0) + 1
            
            logger.info(f"📋 SOURCE BREAKDOWN:")
            for source_name, count in source_counts.items():
                logger.info(f"   • {source_name}: {count} records")
        
        # File output summary
        output_files = []
        if all_results:
            output_files.append("output.csv")
            
            # Check for source-specific files
            for source_config in sources_processed:
                source_name = source_config.name
                # Look for source-specific CSV files (they follow a pattern with timestamp)
                source_files = list(input_path.glob(f"{source_name.replace(' ', '_')}_*.csv"))
                if source_files:
                    output_files.extend([f.name for f in source_files])
        
        logger.info(f"📁 OUTPUT FILES:")
        for file_name in output_files:
            logger.info(f"   • {file_name}")
        
        # FAILURE DETAILS - NEW SECTION
        total_failures = len(failed_files) + len(sources_with_no_files)
        if total_failures > 0:
            logger.info(f"❌ FAILURE DETAILS:")
            logger.info(f"   • Total failures: {total_failures}")
            
            # Sources with no files
            if sources_with_no_files:
                logger.info(f"   📂 Sources with no PDF files:")
                for source_name in sources_with_no_files:
                    logger.info(f"      • {source_name}")
            
            # Failed files by source
            if failed_files:
                logger.info(f"   📄 Failed files by source:")
                failed_by_source = {}
                for failure in failed_files:
                    source_name = failure['source_name']
                    if source_name not in failed_by_source:
                        failed_by_source[source_name] = []
                    failed_by_source[source_name].append(failure)
                
                for source_name, failures in failed_by_source.items():
                    logger.info(f"      • {source_name}: {len(failures)} failed files")
                    for failure in failures[:3]:  # Show first 3 failures per source
                        logger.info(f"        - {failure['file_name']}: {failure['error_message'][:100]}...")
                    if len(failures) > 3:
                        logger.info(f"        - ... and {len(failures) - 3} more failures")
            
            # Detailed failure list for handling later
            logger.info(f"   🔧 DETAILED FAILURE LIST (for manual handling):")
            for i, failure in enumerate(failed_files, 1):
                logger.info(f"      {i}. {failure['source_name']} - {failure['file_name']}")
                logger.info(f"         Path: {failure['file_path']}")
                logger.info(f"         Error: {failure['error_message']}")
            
            # Recommendations for handling failures
            logger.info(f"   💡 RECOMMENDATIONS:")
            if sources_with_no_files:
                logger.info(f"      • Check file patterns for sources: {', '.join(sources_with_no_files)}")
            if failed_files:
                logger.info(f"      • Review {len(failed_files)} failed files above for manual processing")
                logger.info(f"      • Common issues: OCR problems, corrupted PDFs, API rate limits")
        
        # Metrics summary if available
        if self.metrics:
            try:
                summary = self.metrics.get_pipeline_summary()
                logger.info(f"⏱️ PERFORMANCE METRICS:")
                logger.info(f"   • Total PDFs processed: {summary.total_pdfs_processed}")
                logger.info(f"   • Successful extractions: {summary.successful_extractions}")
                logger.info(f"   • Failed extractions: {summary.failed_extractions}")
                
                if summary.total_pdfs_processed > 0:
                    success_rate = (summary.successful_extractions / summary.total_pdfs_processed) * 100
                    logger.info(f"   • Success rate: {success_rate:.1f}%")
                
                logger.info(f"   • Total processing time: {summary.total_processing_time:.2f} seconds")
                if summary.average_processing_time > 0:
                    logger.info(f"   • Average time per PDF: {summary.average_processing_time:.2f} seconds")
                
                # Error summary if any
                if summary.errors:
                    logger.info(f"❌ ERRORS ENCOUNTERED:")
                    for error in summary.errors[:5]:  # Show first 5 errors
                        logger.info(f"   • {error}")
                    if len(summary.errors) > 5:
                        logger.info(f"   • ... and {len(summary.errors) - 5} more errors")
                
            except Exception as e:
                logger.warning(f"⚠️ Could not generate detailed metrics summary: {e}")
        
        # Processing status
        if total_records > 0:
            if total_failures > 0:
                logger.info(f"⚠️ PIPELINE COMPLETED WITH ISSUES")
                logger.info(f"   • Extracted {total_records} records from {total_sources} sources")
                logger.info(f"   • {total_failures} failures need attention (see details above)")
            else:
                logger.info(f"✅ PIPELINE COMPLETED SUCCESSFULLY")
                logger.info(f"   • Extracted {total_records} records from {total_sources} sources")
        else:
            logger.warning(f"❌ PIPELINE COMPLETED WITH NO RECORDS")
            logger.warning(f"   • No records were extracted - check input files and configuration")
            if total_failures > 0:
                logger.warning(f"   • {total_failures} failures prevented successful extraction")
        
        logger.info("=" * 80)

    def _process_source(self, source_config, existing_data, input_path, metadata_handler):
        """
        Process a source using Open Budget index.csv and datapackage.json.
        
        Args:
            source_config: Source configuration with Open Budget URLs
            existing_data: Existing CSV data for change detection
            input_path: Input directory path
            metadata_handler: Metadata handler instance
            
        Returns:
            Tuple of (source_results, source_failed_files)
        """
        logger.info(f"Processing source: {source_config.name}")
        
        # Initialize Open Budget data source
        ob_source = OpenBudgetDataSource(
            index_csv_url=source_config.index_csv_url,
            datapackage_url=source_config.datapackage_url
        )
        
        # Extract existing URLs and revision for change detection
        existing_urls = set()
        existing_revision = None
        
        for record in existing_data:
            if 'url' in record:
                existing_urls.add(record['url'])
            if 'revision' in record and existing_revision is None:
                existing_revision = record['revision']
        
        logger.info(f"Change detection for {source_config.name}:")
        logger.info(f"  - Existing URLs: {len(existing_urls)}")
        logger.info(f"  - Existing revision: {existing_revision or 'none'}")
        
        # Get current revision
        try:
            current_revision = ob_source.get_current_revision()
            logger.info(f"  - Current revision: {current_revision}")
        except Exception as e:
            logger.error(f"Failed to get current revision for {source_config.name}: {e}")
            return [], [{'source_name': source_config.name, 'error_message': str(e)}]
        
        # Get files that need processing
        try:
            files_to_process = ob_source.get_files_to_process(existing_urls, existing_revision or "unknown")
        except Exception as e:
            logger.error(f"Failed to get files to process for {source_config.name}: {e}")
            return [], [{'source_name': source_config.name, 'error_message': str(e)}]
        
        if not files_to_process:
            logger.info(f"  - No files need processing (all up to date)")
            return [], []
        
        logger.info(f"  - Files to process: {len(files_to_process)}")
        for file_info in files_to_process[:3]:  # Show first 3 files
            logger.info(f"    * {file_info['filename']}: {file_info['url']}")
        if len(files_to_process) > 3:
            logger.info(f"    * ... and {len(files_to_process) - 3} more files")
        
        # Process each file
        source_results = []
        source_failed_files = []
        
        for file_info in files_to_process:
            # Add revision to file_info
            file_info['revision'] = current_revision
            try:
                # Download the PDF
                download_dir = input_path / "downloads" / source_config.name
                pdf_path = ob_source.download_pdf(file_info['filename'], str(download_dir))
                
                # Process the PDF
                result = self._process_single_pdf(
                    Path(pdf_path), source_config, metadata_handler, file_info
                )
                if result:
                    source_results.extend(result)
                    
            except Exception as e:
                error_info = {
                    'file_path': file_info.get('filename', 'unknown'),
                    'source_name': source_config.name,
                    'error_message': str(e),
                    'file_name': file_info.get('filename', 'unknown')
                }
                source_failed_files.append(error_info)
                logger.error(f"Failed to process {file_info.get('filename', 'unknown')}: {e}")
                
                if self.metrics:
                    metrics = ExtractionMetrics(
                        pdf_path=file_info.get('filename', 'unknown'),
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
        
        return source_results, source_failed_files
    
    def _merge_with_existing_data(self, new_results: List[Dict[str, Any]], existing_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Merge new results with existing data, preserving unchanged records.
        
        Args:
            new_results: Newly processed records
            existing_data: Existing records from input.csv
            
        Returns:
            Combined list of records with existing data preserved
        """
        if not existing_data:
            logger.info("No existing data to merge")
            return new_results
        
        # Create lookup for new results by URL
        new_results_lookup = {}
        for record in new_results:
            url = record.get('url', '')
            if url:
                new_results_lookup[url] = record
        
        # Create lookup for existing data by URL
        existing_data_lookup = {}
        for record in existing_data:
            url = record.get('url', '')
            if url:
                existing_data_lookup[url] = record
        
        # Track what we're doing
        preserved_count = 0
        updated_count = 0
        new_count = len(new_results)
        
        # Start with new results
        final_results = list(new_results)
        
        # Add existing records that weren't updated
        for url, existing_record in existing_data_lookup.items():
            if url not in new_results_lookup:
                final_results.append(existing_record)
                preserved_count += 1
                logger.debug(f"Preserved existing record: {url}")
        
        logger.info(f"Data merge summary: {new_count} new/updated, {preserved_count} preserved, {len(final_results)} total")
        
        return final_results
    

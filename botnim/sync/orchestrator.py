"""
Comprehensive Sync Orchestration with CI Integration

This module provides the main orchestration logic that coordinates all sync components:
- HTML content fetching and processing
- PDF discovery and processing  
- Spreadsheet processing
- Embedding generation and storage
- Cache management and version control
- CI integration with robust error handling
"""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from openai import OpenAI

from ..config import get_logger, DEFAULT_ENVIRONMENT, get_openai_client
from .config import SyncConfig, ContentSource, SourceType, VersionManager, FetchStrategy
from .cache import SyncCache
from .html_fetcher import HTMLProcessor
from .pdf_discovery import PDFDiscoveryProcessor
from .html_discovery import HTMLDiscoveryProcessor
from .spreadsheet_fetcher import AsyncSpreadsheetProcessor
from .embedding_processor import SyncEmbeddingProcessor
from .pdf_pipeline_processor import PDFPipelineProcessor
from ..vector_store.vector_store_es import VectorStoreES



logger = get_logger(__name__)


@dataclass
class SyncResult:
    """Result of a sync operation."""
    source_id: str
    source_type: str
    status: str  # success, failed, skipped
    processing_time: float
    documents_processed: int
    documents_failed: int
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SyncSummary:
    """Summary of the entire sync operation."""
    total_sources: int
    successful_sources: int
    failed_sources: int
    skipped_sources: int
    total_documents_processed: int
    total_documents_failed: int
    total_processing_time: float
    embedding_cache_downloaded: bool
    embedding_cache_uploaded: bool
    errors: List[str]
    results: List[SyncResult]


class SyncOrchestrator:
    """
    Main orchestration engine that coordinates all sync operations.
    
    This class manages the complete sync workflow:
    1. Download embedding cache from cloud
    2. Process all content sources (HTML, PDF, spreadsheet)
    3. Generate embeddings for new/changed content
    4. Upload embedding cache to cloud
    5. Provide comprehensive logging and error handling
    """
    
    def __init__(self, config: SyncConfig, environment: str = DEFAULT_ENVIRONMENT):
        """
        Initialize the sync orchestrator.
        
        Args:
            config: Sync configuration
            environment: Environment (staging, production, local)
        """
        self.config = config
        self.environment = environment
        self.logger = get_logger(__name__)
        
        # Initialize core components
        self._init_components()
        
        # Track sync state
        self.sync_start_time = None
        self.sync_results: List[SyncResult] = []
        self.sync_errors: List[str] = []
        
        self.logger.info(f"Sync orchestrator initialized for environment: {environment}")
    
    def _init_components(self):
        """Initialize all sync components."""
        try:
            # Initialize cache
            self.cache = SyncCache(cache_directory=self.config.cache_directory)
            
            # Initialize vector store
            self.vector_store = VectorStoreES('', '.', environment=self.environment)
            
            # Initialize OpenAI client
            self.openai_client = get_openai_client()
            
            # Initialize version manager
            self.version_manager = VersionManager(self.config.version_cache_path)
            
            # Initialize processors
            self.html_processor = HTMLProcessor(self.cache, self.environment)
            self.html_discovery_processor = HTMLDiscoveryProcessor(
                cache=self.cache,
                vector_store=self.vector_store,
                openai_client=self.openai_client
            )
            self.pdf_processor = PDFDiscoveryProcessor(
                cache=self.cache,
                vector_store=self.vector_store,
                openai_client=self.openai_client
            )
            self.spreadsheet_processor = AsyncSpreadsheetProcessor(
                cache=self.cache,
                vector_store=self.vector_store,
                max_workers=self.config.max_concurrent_sources
            )
            self.embedding_processor = SyncEmbeddingProcessor(
                vector_store=self.vector_store,
                openai_client=self.openai_client,
                sync_cache=self.cache,
                embedding_cache_path=self.config.embedding_cache_path
            )
            self.pdf_pipeline_processor = PDFPipelineProcessor(
                cache=self.cache,
                openai_client=self.openai_client
            )

            self.logger.info("All sync components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize sync components: {e}")
            raise
    
    async def run_sync(self) -> SyncSummary:
        """
        Run the complete sync operation.
        
        Returns:
            SyncSummary with comprehensive results
        """
        self.sync_start_time = time.time()
        self.logger.info("🚀 Starting comprehensive sync operation")
        
        try:
            # Step 1: Pre-processing pipelines
            self.logger.info("🛠️ Step 1: Running pre-processing pipelines")
            await self._run_preprocessing_pipelines()

            # Step 2: Download embedding cache from cloud
            self.logger.info("📥 Step 2: Downloading embedding cache from cloud")
            cache_downloaded = await self._download_embedding_cache()
            
            # Step 3: Process all content sources
            self.logger.info("📄 Step 3: Processing content sources")
            await self._process_all_sources()
            
            # Step 4: Generate embeddings for new/changed content
            self.logger.info("🔮 Step 4: Generating embeddings")
            await self._process_embeddings()
            
            # Step 5: Upload embedding cache to cloud
            self.logger.info("📤 Step 5: Uploading embedding cache to cloud")
            cache_uploaded = await self._upload_embedding_cache()
            
            # Step 6: Generate summary
            summary = self._generate_summary(cache_downloaded, cache_uploaded)
            
            self.logger.info("✅ Sync operation completed successfully")
            return summary
            
        except Exception as e:
            error_msg = f"Sync operation failed: {e}"
            self.logger.error(error_msg, exc_info=True)
            self.sync_errors.append(error_msg)
            
            # Return partial summary even on failure
            return self._generate_summary(False, False)

    async def _run_preprocessing_pipelines(self):
        """Run all configured pre-processing pipelines."""
        pipeline_sources = self.config.get_sources_by_type(SourceType.PDF_PIPELINE)
        if not pipeline_sources:
            self.logger.info("No pre-processing pipelines to run.")
            return

        self.logger.info(f"Found {len(pipeline_sources)} PDF pre-processing pipelines to run.")
        
        # Track newly created spreadsheet sources
        created_spreadsheet_sources = []
        
        for source in pipeline_sources:
            if not source.enabled:
                self.logger.info(f"Skipping disabled pipeline: {source.id}")
                continue

            try:
                self.logger.info(f"Running pipeline: {source.id}")
                result = self.pdf_pipeline_processor.process_pipeline_source(source)
                
                # Check if the pipeline created a new spreadsheet source
                if result.get("status") == "completed" and result.get("created_spreadsheet_source"):
                    created_source = result["created_spreadsheet_source"]
                    created_spreadsheet_sources.append(created_source)
                    self.logger.info(f"Pipeline {source.id} created new spreadsheet source: {created_source.id}")
                
                # Log the result
                if result.get("status") == "failed":
                    error_message = f"PDF Pipeline '{source.id}' failed: {result.get('errors')}"
                    self.logger.error(error_message)
                    self.sync_errors.append(error_message)
                else:
                    self.logger.info(f"Pipeline {source.id} completed with status: {result.get('status')}")

            except Exception as e:
                error_message = f"An unexpected error occurred in pipeline '{source.id}': {e}"
                self.logger.error(error_message, exc_info=True)
                self.sync_errors.append(error_message)
        
        # Add newly created spreadsheet sources to the config for processing
        if created_spreadsheet_sources:
            self.logger.info(f"Adding {len(created_spreadsheet_sources)} newly created spreadsheet sources to processing queue")
            self.config.sources.extend(created_spreadsheet_sources)

    async def _download_embedding_cache(self) -> bool:
        """Download embedding cache from cloud storage."""
        try:
            download_results = self.embedding_processor.download_embedding_cache()
            
            if download_results['success']:
                self.logger.info(f"✅ Downloaded {download_results['embeddings_downloaded']:,} embeddings from cloud")
                return True
            else:
                self.logger.warning(f"⚠️ Failed to download embedding cache: {download_results.get('error', 'Unknown error')}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error downloading embedding cache: {e}")
            self.sync_errors.append(f"Embedding cache download failed: {e}")
            return False
    
    async def _process_all_sources(self):
        """Process all configured content sources."""
        # Get all enabled sources, excluding PDF_PIPELINE sources (which are processed in pre-processing)
        # Note: PDF pipeline sources that complete successfully create new spreadsheet sources
        # which are automatically added to the config.sources list during pre-processing
        enabled_sources = [s for s in self.config.get_enabled_sources() if s.type != SourceType.PDF_PIPELINE]
        
        if not enabled_sources:
            self.logger.warning("No enabled sources found for main processing loop.")
            return
        
        # Sort sources by priority (lower number = higher priority)
        enabled_sources.sort(key=lambda s: s.priority)
        
        self.logger.info(f"Processing {len(enabled_sources)} enabled sources in main loop")
        
        # Process sources in parallel with thread pool
        with ThreadPoolExecutor(max_workers=self.config.max_concurrent_sources) as executor:
            # Submit all source processing tasks
            future_to_source = {
                executor.submit(self._process_single_source, source): source
                for source in enabled_sources
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    result = future.result()
                    self.sync_results.append(result)
                    
                    if result.status == 'success':
                        self.logger.info(f"✅ {source.name} ({source.type.value}): {result.documents_processed} documents processed")
                    elif result.status == 'skipped':
                        self.logger.info(f"⏭️ {source.name} ({source.type.value}): skipped")
                    else:
                        self.logger.error(f"❌ {source.name} ({source.type.value}): {result.error_message}")
                        
                except Exception as e:
                    error_msg = f"Failed to process source {source.id}: {e}"
                    self.logger.error(error_msg)
                    self.sync_errors.append(error_msg)
                    
                    # Create failed result
                    failed_result = SyncResult(
                        source_id=source.id,
                        source_type=source.type.value,
                        status='failed',
                        processing_time=0.0,
                        documents_processed=0,
                        documents_failed=0,
                        error_message=str(e)
                    )
                    self.sync_results.append(failed_result)
    
    def _process_single_source(self, source: ContentSource) -> SyncResult:
        """Process a single content source."""
        start_time = time.time()
        
        try:
            self.logger.info(f"Processing source: {source.name} ({source.type.value})")
            
            if source.type == SourceType.HTML:
                return self._process_html_source(source, start_time)
            elif source.type == SourceType.PDF:
                return self._process_pdf_source(source, start_time)
            elif source.type == SourceType.SPREADSHEET:
                return self._process_spreadsheet_source(source, start_time)
            else:
                error_msg = f"Unsupported source type for main processing: {source.type.value}"
                self.logger.warning(error_msg)
                return SyncResult(
                    source_id=source.id,
                    source_type=source.type.value,
                    status='skipped',
                    processing_time=time.time() - start_time,
                    documents_processed=0,
                    documents_failed=0,
                    error_message=error_msg
                )
                
        except Exception as e:
            error_msg = f"Failed to process source {source.id}: {e}"
            self.logger.error(error_msg)
            return SyncResult(
                source_id=source.id,
                source_type=source.type.value,
                status='failed',
                processing_time=time.time() - start_time,
                documents_processed=0,
                documents_failed=0,
                error_message=str(e)
            )
    
    def _process_html_source(self, source: ContentSource, start_time: float) -> SyncResult:
        """Process an HTML source."""
        try:
            # Check if this is an index page discovery source
            if source.fetch_strategy == FetchStrategy.INDEX_PAGE:
                # Use HTML discovery processor
                results = self.html_discovery_processor.process_html_source(source)
                
                processing_time = time.time() - start_time
                
                # Extract results
                processed_pages = results.get('processed_pages', 0)
                failed_pages = results.get('failed_pages', 0)
                errors = results.get('errors', [])
                
                if failed_pages > 0 or errors:
                    status = 'failed'
                    error_message = f"{failed_pages} HTML pages failed, {len(errors)} errors"
                elif processed_pages == 0:
                    status = 'skipped'
                    error_message = None
                else:
                    status = 'success'
                    error_message = None
                
                return SyncResult(
                    source_id=source.id,
                    source_type=source.type.value,
                    status=status,
                    processing_time=processing_time,
                    documents_processed=processed_pages,
                    documents_failed=failed_pages,
                    error_message=error_message,
                    metadata={'html_discovery_results': results}
                )
            else:
                # Use regular HTML processor
                results = self.html_processor.process_sources([source])
                
                processing_time = time.time() - start_time
                
                # Extract results
                summary = results['summary']
                processed_count = summary['processed_count']
                error_count = summary['error_count']
                
                if error_count > 0:
                    status = 'failed'
                    error_message = f"{error_count} errors occurred during processing"
                elif processed_count == 0:
                    status = 'skipped'
                    error_message = None
                else:
                    status = 'success'
                    error_message = None
                
                return SyncResult(
                    source_id=source.id,
                    source_type=source.type.value,
                    status=status,
                    processing_time=processing_time,
                    documents_processed=processed_count,
                    documents_failed=error_count,
                    error_message=error_message,
                    metadata={'html_results': results}
                )
            
        except Exception as e:
            return SyncResult(
                source_id=source.id,
                source_type=source.type.value,
                status='failed',
                processing_time=time.time() - start_time,
                documents_processed=0,
                documents_failed=0,
                error_message=str(e)
            )
    
    def _process_pdf_source(self, source: ContentSource, start_time: float) -> SyncResult:
        """Process a PDF source."""
        try:
            # Process PDF source
            results = self.pdf_processor.process_pdf_source(source)
            
            processing_time = time.time() - start_time
            
            # Extract results
            processed_pdfs = results.get('processed_pdfs', 0)
            failed_pdfs = results.get('failed_pdfs', 0)
            errors = results.get('errors', [])
            
            if failed_pdfs > 0 or errors:
                status = 'failed'
                error_message = f"{failed_pdfs} PDFs failed, {len(errors)} errors"
            elif processed_pdfs == 0:
                status = 'skipped'
                error_message = None
            else:
                status = 'success'
                error_message = None
            
            return SyncResult(
                source_id=source.id,
                source_type=source.type.value,
                status=status,
                processing_time=processing_time,
                documents_processed=processed_pdfs,
                documents_failed=failed_pdfs,
                error_message=error_message,
                metadata={'pdf_results': results}
            )
            
        except Exception as e:
            return SyncResult(
                source_id=source.id,
                source_type=source.type.value,
                status='failed',
                processing_time=time.time() - start_time,
                documents_processed=0,
                documents_failed=0,
                error_message=str(e)
            )
    
    def _process_spreadsheet_source(self, source: ContentSource, start_time: float) -> SyncResult:
        """Process a spreadsheet source."""
        try:
            # Process spreadsheet source asynchronously by running the async function
            # in the current event loop, which is managed by the ThreadPoolExecutor.
            results = asyncio.run(self.spreadsheet_processor.process_spreadsheet_source(source))
            
            processing_time = time.time() - start_time
            
            # Translate the async processor's result dictionary into a SyncResult object
            status = results.get('status', 'failed')
            error_message = results.get('error_message')
            
            # Since the async processor handles one source as one task, we can map the status
            documents_processed = 1 if status == 'submitted' or status == 'completed' else 0
            documents_failed = 1 if status == 'failed' or status == 'error' else 0

            if status == 'submitted' or status == 'completed':
                status = 'success' # Remap to the orchestrator's status enum

            return SyncResult(
                source_id=source.id,
                source_type=source.type.value,
                status=status,
                processing_time=processing_time,
                documents_processed=documents_processed,
                documents_failed=documents_failed,
                error_message=error_message,
                metadata={'spreadsheet_results': results}
            )
            
        except Exception as e:
            return SyncResult(
                source_id=source.id,
                source_type=source.type.value,
                status='failed',
                processing_time=time.time() - start_time,
                documents_processed=0,
                documents_failed=1,
                error_message=str(e)
            )
    
    async def _process_embeddings(self):
        """Process embeddings for all processed content."""
        try:
            # Get all processed content from cache
            processed_content = []
            cache_entries = self.cache.get_all_cached_content()
            
            for entry in cache_entries:
                if entry.processed and not entry.error_message:
                    # Extract content from metadata
                    metadata = entry.metadata
                    if 'parsed_content' in metadata:
                        parsed_content = metadata['parsed_content']
                        
                        # Check if document parsing was used and chunks are available
                        if parsed_content.get('parsing_method') == 'document_parser' and 'chunks' in parsed_content:
                            # Use individual chunks for embedding
                            chunks = parsed_content['chunks']
                            for chunk_name, chunk_content in chunks.items():
                                processed_content.append({
                                    'source_id': f"{entry.source_id}_{chunk_name}",
                                    'content': chunk_content,
                                    'version_info': metadata.get('version_info', {}),
                                    'chunk_info': {
                                        'original_source': entry.source_id,
                                        'chunk_name': chunk_name,
                                        'chunk_count': len(chunks)
                                    }
                                })
                        else:
                            # Use full text content (fallback)
                            text_content = parsed_content.get('text_content', '')
                            if text_content:
                                processed_content.append({
                                    'source_id': entry.source_id,
                                    'content': text_content,
                                    'version_info': metadata.get('version_info', {})
                                })
            
            if not processed_content:
                self.logger.info("No processed content found for embedding generation")
                return
            
            self.logger.info(f"Generating embeddings for {len(processed_content)} documents")
            
            # Process embeddings
            embedding_results = self.embedding_processor.process_sync_content(processed_content)
            
            self.logger.info(f"Embedding processing completed: {embedding_results['processed_documents']} documents processed")
            
        except Exception as e:
            error_msg = f"Failed to process embeddings: {e}"
            self.logger.error(error_msg)
            self.sync_errors.append(error_msg)
    
    async def _upload_embedding_cache(self) -> bool:
        """Upload embedding cache to cloud storage."""
        try:
            upload_results = self.embedding_processor.upload_embedding_cache()
            
            if upload_results['success']:
                self.logger.info(f"✅ Uploaded {upload_results['embeddings_uploaded']:,} embeddings to cloud")
                return True
            else:
                self.logger.warning(f"⚠️ Failed to upload embedding cache: {upload_results.get('error', 'Unknown error')}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error uploading embedding cache: {e}")
            self.sync_errors.append(f"Embedding cache upload failed: {e}")
            return False
    
    def _generate_summary(self, cache_downloaded: bool, cache_uploaded: bool) -> SyncSummary:
        """Generate comprehensive sync summary."""
        total_processing_time = time.time() - self.sync_start_time if self.sync_start_time else 0.0
        
        # Count results by status
        successful_sources = sum(1 for r in self.sync_results if r.status == 'success')
        failed_sources = sum(1 for r in self.sync_results if r.status == 'failed')
        skipped_sources = sum(1 for r in self.sync_results if r.status == 'skipped')
        
        # Sum document counts
        total_documents_processed = sum(r.documents_processed for r in self.sync_results)
        total_documents_failed = sum(r.documents_failed for r in self.sync_results)
        
        return SyncSummary(
            total_sources=len(self.sync_results),
            successful_sources=successful_sources,
            failed_sources=failed_sources,
            skipped_sources=skipped_sources,
            total_documents_processed=total_documents_processed,
            total_documents_failed=total_documents_failed,
            total_processing_time=total_processing_time,
            embedding_cache_downloaded=cache_downloaded,
            embedding_cache_uploaded=cache_uploaded,
            errors=self.sync_errors,
            results=self.sync_results
        )
    
    def get_sync_statistics(self) -> Dict[str, Any]:
        """Get comprehensive sync statistics."""
        try:
            # Get cache statistics
            cache_stats = self.cache.get_cache_statistics()
            
            # Get embedding statistics
            embedding_stats = self.embedding_processor.get_embedding_statistics()
            
            # Get version statistics
            version_count = len(self.version_manager.versions)
            
            return {
                'cache_statistics': cache_stats,
                'embedding_statistics': embedding_stats,
                'version_count': version_count,
                'environment': self.environment,
                'config_name': self.config.name,
                'enabled_sources': len(self.config.get_enabled_sources()),
                'total_sources': len(self.config.sources)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get sync statistics: {e}")
            return {'error': str(e)}
    
    def cleanup(self):
        """Clean up resources."""
        try:
            # Clean up processors
            if hasattr(self, 'html_processor'):
                self.html_processor.close()
            
            if hasattr(self, 'html_discovery_processor'):
                self.html_discovery_processor.cleanup()
            
            if hasattr(self, 'pdf_processor'):
                self.pdf_processor.cleanup()
            
            if hasattr(self, 'spreadsheet_processor'):
                self.spreadsheet_processor.shutdown()
            
            self.logger.info("Sync orchestrator cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


class SyncOrchestratorCLI:
    """CLI interface for the sync orchestrator."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def run_sync_from_config(self, config_path: str, environment: str = DEFAULT_ENVIRONMENT) -> SyncSummary:
        """
        Run sync from configuration file.
        
        Args:
            config_path: Path to sync configuration file
            environment: Environment to run in
            
        Returns:
            SyncSummary with results
        """
        try:
            # Load configuration
            config = SyncConfig.from_yaml(config_path)
            self.logger.info(f"Loaded sync configuration: {config.name}")
            
            # Create orchestrator
            orchestrator = SyncOrchestrator(config, environment)
            
            try:
                # Run sync
                summary = asyncio.run(orchestrator.run_sync())
                return summary
                
            finally:
                # Cleanup
                orchestrator.cleanup()
                
        except Exception as e:
            self.logger.error(f"Failed to run sync: {e}")
            raise
    
    def print_summary(self, summary: SyncSummary):
        """Print sync summary in a formatted way."""
        self.logger.info("=" * 60)
        self.logger.info("📊 SYNC OPERATION SUMMARY")
        self.logger.info("=" * 60)
        
        # Overall statistics
        self.logger.info(f"Total Sources: {summary.total_sources}")
        self.logger.info(f"Successful: {summary.successful_sources} ✅")
        self.logger.info(f"Failed: {summary.failed_sources} ❌")
        self.logger.info(f"Skipped: {summary.skipped_sources} ⏭️")
        
        self.logger.info(f"Documents Processed: {summary.total_documents_processed:,}")
        self.logger.info(f"Documents Failed: {summary.total_documents_failed:,}")
        self.logger.info(f"Total Processing Time: {summary.total_processing_time:.2f}s")
        
        # Cache operations
        self.logger.info(f"Embedding Cache Downloaded: {'✅' if summary.embedding_cache_downloaded else '❌'}")
        self.logger.info(f"Embedding Cache Uploaded: {'✅' if summary.embedding_cache_uploaded else '❌'}")
        
        # Detailed results
        if summary.results:
            self.logger.info("\n📋 DETAILED RESULTS:")
            self.logger.info("-" * 40)
            
            for result in summary.results:
                status_icon = {
                    'success': '✅',
                    'failed': '❌',
                    'skipped': '⏭️'
                }.get(result.status, '❓')
                
                self.logger.info(
                    f"{status_icon} {result.source_id} ({result.source_type}): "
                    f"{result.documents_processed} processed, "
                    f"{result.documents_failed} failed, "
                    f"{result.processing_time:.2f}s"
                )
                
                if result.error_message:
                    self.logger.info(f"   Error: {result.error_message}")
        
        # Errors
        if summary.errors:
            self.logger.info("\n❌ ERRORS:")
            self.logger.info("-" * 20)
            for error in summary.errors:
                self.logger.error(f"  - {error}")
        
        self.logger.info("=" * 60)


# Convenience function for easy integration
async def run_sync_orchestration(config_path: str, environment: str = DEFAULT_ENVIRONMENT) -> SyncSummary:
    """
    Convenience function to run sync orchestration.
    
    Args:
        config_path: Path to sync configuration file
        environment: Environment to run in
        
    Returns:
        SyncSummary with results
    """
    cli = SyncOrchestratorCLI()
    return cli.run_sync_from_config(config_path, environment)


def run_sync_orchestration_sync(config_path: str, environment: str = DEFAULT_ENVIRONMENT) -> SyncSummary:
    """
    Synchronous version of run_sync_orchestration.
    
    Args:
        config_path: Path to sync configuration file
        environment: Environment to run in
        
    Returns:
        SyncSummary with results
    """
    return asyncio.run(run_sync_orchestration(config_path, environment))

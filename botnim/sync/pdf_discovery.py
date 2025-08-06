"""
PDF Discovery and Processing for Automated Sync System

This module provides:
1. Discovery of new PDFs from remote index pages
2. Temporary download and processing of PDFs
3. Integration with existing PDF processing pipeline
4. Tracking of processed files in Elasticsearch
5. Cleanup of temporary files
"""

import os
import re
import hashlib
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

from ..config import get_logger
from .config import ContentSource
from .cache import SyncCache
from botnim.document_parser.pdf_processor.pdf_pipeline import PDFExtractionPipeline
from botnim.vector_store.vector_store_es import VectorStoreES


logger = get_logger(__name__)


class PDFDiscoveryService:
    """
    Service for discovering new PDFs from remote sources.
    """
    
    def __init__(self, cache: SyncCache, vector_store: VectorStoreES):
        """
        Initialize PDF discovery service.
        
        Args:
            cache: Sync cache for tracking processed files
            vector_store: Vector store for storing processed content
        """
        self.cache = cache
        self.vector_store = vector_store
        self.session = requests.Session()
        
        # Set default headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def discover_pdfs_from_index_page(self, source: ContentSource) -> List[Dict[str, Any]]:
        """
        Discover PDF files from an index page.
        
        Args:
            source: PDF source configuration
            
        Returns:
            List of discovered PDF information
        """
        if not source.pdf_config or not source.pdf_config.is_index_page:
            logger.warning(f"Source {source.id} is not configured for index page discovery")
            return []
        
        try:
            logger.info(f"Discovering PDFs from index page: {source.pdf_config.url}")
            
            # Fetch the index page
            response = self.session.get(
                source.pdf_config.url,
                headers=source.pdf_config.headers,
                timeout=source.pdf_config.timeout
            )
            response.raise_for_status()
            
            # Parse HTML content
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find PDF links
            pdf_links = self._extract_pdf_links(soup, source.pdf_config.url, source.pdf_config.file_pattern)
            
            logger.info(f"Discovered {len(pdf_links)} PDF links from {source.id}")
            return pdf_links
            
        except Exception as e:
            logger.error(f"Failed to discover PDFs from {source.id}: {e}")
            return []
    
    def _extract_pdf_links(self, soup: BeautifulSoup, base_url: str, file_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract PDF links from HTML content.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            base_url: Base URL for resolving relative links
            file_pattern: Optional file pattern to filter PDFs
            
        Returns:
            List of PDF link information
        """
        pdf_links = []
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if not href:
                continue
            
            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            
            # Check if it's a PDF link
            if self._is_pdf_link(full_url, file_pattern):
                # Extract link text and metadata
                link_text = link.get_text(strip=True)
                link_title = link.get('title', '')
                
                # Generate unique identifier
                url_hash = hashlib.sha256(full_url.encode()).hexdigest()
                
                pdf_info = {
                    'url': full_url,
                    'filename': self._extract_filename(full_url),
                    'link_text': link_text,
                    'link_title': link_title,
                    'url_hash': url_hash,
                    'discovered_at': datetime.now(timezone.utc).isoformat()
                }
                
                pdf_links.append(pdf_info)
        
        return pdf_links
    
    def _is_pdf_link(self, url: str, file_pattern: Optional[str] = None) -> bool:
        """
        Check if a URL points to a PDF file.
        
        Args:
            url: URL to check
            file_pattern: Optional file pattern to match
            
        Returns:
            True if URL points to a PDF
        """
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        
        # Check if it ends with .pdf
        if path.endswith('.pdf'):
            if file_pattern:
                # Apply file pattern filter
                filename = os.path.basename(path)
                try:
                    return re.match(file_pattern, filename) is not None
                except re.error:
                    # If pattern is invalid, treat as no pattern
                    return True
            return True
        
        return False
    
    def _extract_filename(self, url: str) -> str:
        """
        Extract filename from URL.
        
        Args:
            url: URL to extract filename from
            
        Returns:
            Filename
        """
        parsed_url = urlparse(url)
        path = parsed_url.path
        filename = os.path.basename(path)
        
        if not filename or filename == '/':
            # Generate filename from URL hash
            url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
            filename = f"document_{url_hash}.pdf"
        
        return filename
    
    def close(self):
        """Close the session."""
        self.session.close()


class PDFDownloadManager:
    """
    Manages temporary download and processing of PDF files.
    """
    
    def __init__(self, temp_directory: Optional[str] = None):
        """
        Initialize PDF download manager.
        
        Args:
            temp_directory: Directory for temporary files (auto-created if None)
        """
        self.temp_directory = Path(temp_directory) if temp_directory else Path(tempfile.mkdtemp(prefix="pdf_sync_"))
        self.temp_directory.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"PDF download manager initialized with temp directory: {self.temp_directory}")
    
    def download_pdf(self, pdf_info: Dict[str, Any], headers: Optional[Dict[str, str]] = None, timeout: int = 60) -> Optional[Path]:
        """
        Download a PDF file to temporary location.
        
        Args:
            pdf_info: PDF information from discovery
            headers: Optional HTTP headers
            timeout: Request timeout
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            url = pdf_info['url']
            filename = pdf_info['filename']
            
            logger.info(f"Downloading PDF: {filename} from {url}")
            
            # Create session for this download
            session = requests.Session()
            if headers:
                session.headers.update(headers)
            
            # Download file
            response = session.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            
            # Save to temporary file
            temp_file_path = self.temp_directory / filename
            
            with open(temp_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Successfully downloaded: {temp_file_path}")
            return temp_file_path
            
        except Exception as e:
            logger.error(f"Failed to download PDF {pdf_info.get('filename', 'unknown')}: {e}")
            return None
    
    def cleanup_temp_files(self) -> int:
        """
        Clean up temporary files.
        
        Returns:
            Number of files cleaned up
        """
        try:
            if self.temp_directory.exists():
                shutil.rmtree(self.temp_directory)
                logger.info(f"Cleaned up temporary directory: {self.temp_directory}")
                return 1
        except Exception as e:
            logger.error(f"Failed to cleanup temporary files: {e}")
        
        return 0


class PDFProcessingTracker:
    """
    Tracks PDF processing status in Elasticsearch.
    """
    
    def __init__(self, vector_store: VectorStoreES):
        """
        Initialize PDF processing tracker.
        
        Args:
            vector_store: Vector store for storing tracking information
        """
        self.vector_store = vector_store
        self.index_name = "pdf_processing_tracker"
        
        # Ensure index exists
        self._ensure_index_exists()
    
    def _ensure_index_exists(self):
        """Ensure the tracking index exists."""
        try:
            if not self.vector_store.es_client.indices.exists(index=self.index_name):
                mapping = {
                    "mappings": {
                        "properties": {
                            "source_id": {"type": "keyword"},
                            "pdf_url": {"type": "keyword"},
                            "pdf_filename": {"type": "keyword"},
                            "url_hash": {"type": "keyword"},
                            "download_timestamp": {"type": "date"},
                            "processing_status": {"type": "keyword"},
                            "processing_timestamp": {"type": "date"},
                            "error_message": {"type": "text"},
                            "content_hash": {"type": "keyword"},
                            "vector_store_id": {"type": "keyword"},
                            "metadata": {"type": "object"}
                        }
                    }
                }
                self.vector_store.es_client.indices.create(index=self.index_name, body=mapping)
                logger.info(f"Created tracking index: {self.index_name}")
        except Exception as e:
            logger.error(f"Failed to ensure tracking index exists: {e}")
    
    def track_pdf_processing(self, source_id: str, pdf_info: Dict[str, Any], 
                           status: str, error_message: Optional[str] = None,
                           content_hash: Optional[str] = None, 
                           vector_store_id: Optional[str] = None) -> bool:
        """
        Track PDF processing status.
        
        Args:
            source_id: Source identifier
            pdf_info: PDF information
            status: Processing status (downloaded, processing, completed, failed)
            error_message: Error message if failed
            content_hash: Content hash if processed
            vector_store_id: Vector store document ID if indexed
            
        Returns:
            True if tracking was successful
        """
        try:
            doc = {
                "source_id": source_id,
                "pdf_url": pdf_info['url'],
                "pdf_filename": pdf_info['filename'],
                "url_hash": pdf_info['url_hash'],
                "download_timestamp": pdf_info.get('discovered_at'),
                "processing_status": status,
                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                "error_message": error_message,
                "content_hash": content_hash,
                "vector_store_id": vector_store_id,
                "metadata": {
                    "link_text": pdf_info.get('link_text', ''),
                    "link_title": pdf_info.get('link_title', '')
                }
            }
            
            # Use URL hash as document ID for idempotency
            doc_id = pdf_info['url_hash']
            
            self.vector_store.es_client.index(
                index=self.index_name,
                id=doc_id,
                body=doc
            )
            
            logger.info(f"Tracked PDF processing: {pdf_info['filename']} - {status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to track PDF processing: {e}")
            return False
    
    def is_pdf_processed(self, url_hash: str) -> bool:
        """
        Check if a PDF has already been processed.
        
        Args:
            url_hash: URL hash to check
            
        Returns:
            True if PDF has been processed successfully
        """
        try:
            result = self.vector_store.es_client.get(
                index=self.index_name,
                id=url_hash,
                ignore=[404]
            )
            
            if result.get('found', False):
                status = result['_source'].get('processing_status')
                return status == 'completed'
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to check PDF processing status: {e}")
            return False


class PDFDiscoveryProcessor:
    """
    Main orchestrator for PDF discovery and processing.
    """
    
    def __init__(self, cache: SyncCache, vector_store: VectorStoreES, 
                 openai_client, temp_directory: Optional[str] = None):
        """
        Initialize PDF discovery processor.
        
        Args:
            cache: Sync cache
            vector_store: Vector store for processed content
            openai_client: OpenAI client for PDF processing
            temp_directory: Temporary directory for downloads
        """
        self.cache = cache
        self.vector_store = vector_store
        self.openai_client = openai_client
        
        self.discovery_service = PDFDiscoveryService(cache, vector_store)
        self.download_manager = PDFDownloadManager(temp_directory)
        self.tracker = PDFProcessingTracker(vector_store)
        
        logger.info("PDF discovery processor initialized")
    
    def process_pdf_source(self, source: ContentSource) -> Dict[str, Any]:
        """
        Process a PDF source: discover, download, and process new PDFs.
        
        Args:
            source: PDF source configuration
            
        Returns:
            Processing results summary
        """
        logger.info(f"Processing PDF source: {source.id}")
        
        results = {
            'source_id': source.id,
            'discovered_pdfs': 0,
            'downloaded_pdfs': 0,
            'processed_pdfs': 0,
            'failed_pdfs': 0,
            'errors': []
        }
        
        try:
            # Step 1: Discover PDFs
            pdf_links = self.discovery_service.discover_pdfs_from_index_page(source)
            results['discovered_pdfs'] = len(pdf_links)
            
            if not pdf_links:
                logger.info(f"No PDFs discovered for source {source.id}")
                return results
            
            # Step 2: Filter out already processed PDFs
            new_pdfs = []
            for pdf_info in pdf_links:
                if not self.tracker.is_pdf_processed(pdf_info['url_hash']):
                    new_pdfs.append(pdf_info)
                else:
                    logger.debug(f"PDF already processed: {pdf_info['filename']}")
            
            logger.info(f"Found {len(new_pdfs)} new PDFs to process")
            
            # Step 3: Process each new PDF
            for pdf_info in new_pdfs:
                try:
                    success = self._process_single_pdf(source, pdf_info)
                    if success:
                        results['processed_pdfs'] += 1
                    else:
                        results['failed_pdfs'] += 1
                except Exception as e:
                    error_msg = f"Failed to process {pdf_info['filename']}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['failed_pdfs'] += 1
                    
                    # Track the failure
                    self.tracker.track_pdf_processing(
                        source.id, pdf_info, 'failed', str(e)
                    )
            
            logger.info(f"PDF source processing completed: {results}")
            return results
            
        except Exception as e:
            error_msg = f"Failed to process PDF source {source.id}: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
    
    def _process_single_pdf(self, source: ContentSource, pdf_info: Dict[str, Any]) -> bool:
        """
        Process a single PDF file.
        
        Args:
            source: PDF source configuration
            pdf_info: PDF information
            
        Returns:
            True if processing was successful
        """
        temp_file_path = None
        
        try:
            # Track download start
            self.tracker.track_pdf_processing(source.id, pdf_info, 'downloading')
            
            # Step 1: Download PDF
            temp_file_path = self.download_manager.download_pdf(
                pdf_info, 
                headers=source.pdf_config.headers if source.pdf_config else None,
                timeout=source.pdf_config.timeout if source.pdf_config else 60
            )
            
            if not temp_file_path:
                self.tracker.track_pdf_processing(
                    source.id, pdf_info, 'failed', 'Download failed'
                )
                return False
            
            # Track download completion
            self.tracker.track_pdf_processing(source.id, pdf_info, 'downloaded')
            
            # Step 2: Process PDF using existing pipeline
            self.tracker.track_pdf_processing(source.id, pdf_info, 'processing')
            
            success, content_hash, vector_store_id = self._process_pdf_with_pipeline(
                temp_file_path, source, pdf_info
            )
            
            if success:
                # Track successful processing
                self.tracker.track_pdf_processing(
                    source.id, pdf_info, 'completed', 
                    content_hash=content_hash, vector_store_id=vector_store_id
                )
                logger.info(f"Successfully processed PDF: {pdf_info['filename']}")
                return True
            else:
                # Track processing failure
                self.tracker.track_pdf_processing(
                    source.id, pdf_info, 'failed', 'Processing failed'
                )
                return False
                
        except Exception as e:
            error_msg = f"Error processing PDF {pdf_info['filename']}: {e}"
            logger.error(error_msg)
            self.tracker.track_pdf_processing(source.id, pdf_info, 'failed', error_msg)
            return False
            
        finally:
            # Clean up temporary file
            if temp_file_path and temp_file_path.exists():
                try:
                    temp_file_path.unlink()
                    logger.debug(f"Cleaned up temporary file: {temp_file_path}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup temporary file {temp_file_path}: {e}")
    
    def _process_pdf_with_pipeline(self, pdf_path: Path, source: ContentSource, 
                                  pdf_info: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Process PDF using the existing pipeline.
        
        Args:
            pdf_path: Path to PDF file
            source: Source configuration
            pdf_info: PDF information
            
        Returns:
            Tuple of (success, content_hash, vector_store_id)
        """
        try:
            # Get processing configuration from source
            processing_config = source.pdf_config.processing if source.pdf_config else None
            if not processing_config:
                logger.warning(f"No processing config for source {source.id}, skipping advanced processing")
                return False, None, None

            # Initialize the PDF extraction pipeline
            pipeline = PDFExtractionPipeline(
                config_path=None, # Not needed when providing config directly
                model_name=processing_config.model,
                openai_client=self.openai_client,
                vector_store=self.vector_store
            )

            # Create a temporary directory for pipeline outputs
            with tempfile.TemporaryDirectory() as temp_dir:
                output_dir = Path(temp_dir)
                
                # Run the pipeline for the single PDF
                processed_data, _ = pipeline.run_pipeline(
                    input_dir=str(pdf_path.parent),
                    output_dir=str(output_dir),
                    gcs_bucket_name=None, # Not using GCS for this flow
                    metadata_override={
                        'source_id': source.id,
                        'source_name': source.name,
                        'pdf_url': pdf_info['url'],
                        'pdf_filename': pdf_info['filename'],
                    }
                )

                if processed_data:
                    # Assume one document per PDF for now
                    doc = processed_data[0]
                    content_hash = hashlib.sha256(doc.page_content.encode()).hexdigest()
                    
                    # The pipeline already adds to vector store, so we just get the ID
                    # This assumes the pipeline returns the vector store ID in the metadata
                    vector_store_id = doc.metadata.get("vector_store_id")

                    logger.info(f"Successfully processed and indexed PDF: {pdf_path.name}")
                    return True, content_hash, vector_store_id
                else:
                    logger.warning(f"PDF processing returned no data: {pdf_path.name}")
                    return False, None, None
        
        except Exception as e:
            logger.error(f"Pipeline processing failed for {pdf_path.name}: {e}")
            return False, None, None
    
    def cleanup(self):
        """Clean up resources."""
        try:
            self.discovery_service.close()
            self.download_manager.cleanup_temp_files()
            logger.info("PDF discovery processor cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")


# Convenience function for easy integration
def process_pdf_source(source: ContentSource, cache: SyncCache, vector_store: VectorStoreES, 
                      openai_client, temp_directory: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function to process a PDF source.
    
    Args:
        source: PDF source configuration
        cache: Sync cache
        vector_store: Vector store
        openai_client: OpenAI client
        temp_directory: Temporary directory for downloads
        
    Returns:
        Processing results
    """
    processor = PDFDiscoveryProcessor(cache, vector_store, openai_client, temp_directory)
    
    try:
        return processor.process_pdf_source(source)
    finally:
        processor.cleanup() 
"""
HTML Discovery and Processing for Automated Sync System

This module provides:
1. Discovery of HTML pages from remote index pages
2. Processing of discovered HTML pages
3. Integration with existing HTML processing pipeline
4. Tracking of processed pages in Elasticsearch
"""

import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

from ..config import get_logger
from .config import ContentSource
from .cache import SyncCache
from .html_fetcher import HTMLFetcher
from botnim.vector_store.vector_store_es import VectorStoreES
from .transaction_manager import TransactionManager
from .resilience import RetryPolicy, CircuitBreaker, with_retry

logger = get_logger(__name__)


class HTMLDiscoveryService:
    """
    Service for discovering HTML pages from remote index pages.
    """
    
    def __init__(self, cache: SyncCache, vector_store: VectorStoreES):
        """
        Initialize HTML discovery service.
        
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
        # Resilience
        self.retry_policy = RetryPolicy(max_attempts=3, base_delay_seconds=1.0, max_delay_seconds=16.0)
        self.circuit_breaker = CircuitBreaker()
    
    def discover_html_pages_from_index_page(self, source: ContentSource) -> List[Dict[str, Any]]:
        """
        Discover HTML pages from an index page.
        
        Args:
            source: HTML source configuration
            
        Returns:
            List of discovered HTML page information
        """
        if not source.html_config:
            logger.warning(f"Source {source.id} is missing html_config")
            return []
        
        try:
            logger.info(f"Discovering HTML pages from index page: {source.html_config.url}")
            
            # Fetch the index page
            def _do_get():
                return self.session.get(
                    source.html_config.url,
                    headers=source.html_config.headers,
                    timeout=source.html_config.timeout
                )
            response = with_retry(
                _do_get,
                policy=self.retry_policy,
                circuit_breaker=self.circuit_breaker,
                circuit_key=f"http:get:{source.html_config.url}"
            )
            response.raise_for_status()
            
            # Parse HTML content
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find HTML links
            html_links = self._extract_html_links(soup, source.html_config.url, source.html_config.link_pattern)
            
            logger.info(f"Discovered {len(html_links)} HTML links from {source.id}")
            return html_links
            
        except Exception as e:
            logger.error(f"Failed to discover HTML pages from {source.id}: {e}")
            return []
    
    def _extract_html_links(self, soup: BeautifulSoup, base_url: str, link_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract HTML links from HTML content.
        
        Args:
            soup: BeautifulSoup object of the HTML content
            base_url: Base URL for resolving relative links
            link_pattern: Optional pattern to filter HTML links
            
        Returns:
            List of HTML link information
        """
        html_links = []
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if not href:
                continue
            
            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            
            # Check if it's an HTML link
            if self._is_html_link(full_url, link_pattern):
                # Extract link text and metadata
                link_text = link.get_text(strip=True)
                link_title = link.get('title', '')
                
                # Generate unique identifier
                url_hash = hashlib.sha256(full_url.encode()).hexdigest()
                
                html_info = {
                    'url': full_url,
                    'filename': self._extract_filename(full_url),
                    'link_text': link_text,
                    'link_title': link_title,
                    'url_hash': url_hash,
                    'discovered_at': datetime.now(timezone.utc).isoformat()
                }
                
                html_links.append(html_info)
        
        return html_links
    
    def _is_html_link(self, url: str, link_pattern: Optional[str] = None) -> bool:
        """
        Check if a URL is an HTML link.
        
        Args:
            url: URL to check
            link_pattern: Optional pattern to match
            
        Returns:
            True if it's an HTML link
        """
        parsed_url = urlparse(url)
        
        # Check custom pattern first if provided
        if link_pattern:
            pattern = re.compile(link_pattern)
            return bool(pattern.search(url))
        
        # Check if it's an HTML file
        if parsed_url.path.endswith(('.html', '.htm', '.aspx', '.php')):
            return True
        
        # Check if it has no file extension (likely HTML)
        if '.' not in parsed_url.path.split('/')[-1]:
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
        
        # Get the last part of the path
        filename = path.split('/')[-1]
        
        # If no filename, use the path
        if not filename or '.' not in filename:
            filename = path.replace('/', '_').lstrip('_')
            if not filename:
                filename = 'index'
        
        return filename
    
    def close(self):
        """Close the session."""
        self.session.close()


class HTMLProcessingTracker:
    """
    Tracks HTML page processing status in Elasticsearch.
    """
    
    def __init__(self, vector_store: VectorStoreES):
        """
        Initialize HTML processing tracker.
        
        Args:
            vector_store: Vector store for tracking
        """
        self.vector_store = vector_store
        self.index_name = "html_processing_tracker"
        self.retry_policy = RetryPolicy(max_attempts=3, base_delay_seconds=1.0, max_delay_seconds=16.0)
        self.circuit_breaker = CircuitBreaker()
        self._ensure_index_exists()
    
    def _ensure_index_exists(self):
        """Ensure the tracking index exists."""
        try:
            if not self.vector_store.es_client.indices.exists(index=self.index_name):
                mapping = {
                    "mappings": {
                        "properties": {
                            "source_id": {"type": "keyword"},
                            "url_hash": {"type": "keyword"},
                            "url": {"type": "keyword"},
                            "status": {"type": "keyword"},
                            "processed_at": {"type": "date"},
                            "error_message": {"type": "text"},
                            "content_hash": {"type": "keyword"},
                            "vector_store_id": {"type": "keyword"}
                        }
                    }
                }
                self.vector_store.es_client.indices.create(index=self.index_name, body=mapping)
                logger.info(f"Created HTML processing tracker index: {self.index_name}")
        except Exception as e:
            logger.error(f"Failed to ensure HTML processing tracker index exists: {e}")
    
    def track_html_processing(self, source_id: str, html_info: Dict[str, Any], 
                            status: str, error_message: Optional[str] = None,
                            content_hash: Optional[str] = None, 
                            vector_store_id: Optional[str] = None) -> bool:
        """
        Track HTML page processing status.
        
        Args:
            source_id: Source identifier
            html_info: HTML page information
            status: Processing status
            error_message: Error message if failed
            content_hash: Content hash
            vector_store_id: Vector store document ID
            
        Returns:
            True if tracking was successful
        """
        try:
            doc = {
                "source_id": source_id,
                "url_hash": html_info["url_hash"],
                "url": html_info["url"],
                "status": status,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "error_message": error_message,
                "content_hash": content_hash,
                "vector_store_id": vector_store_id
            }
            
            def _op():
                return self.vector_store.es_client.index(
                    index=self.index_name,
                    id=html_info["url_hash"],
                    document=doc
                )
            with_retry(_op, policy=self.retry_policy, circuit_breaker=self.circuit_breaker, circuit_key=f"es:index:{self.index_name}")
            
            logger.debug(f"Tracked HTML processing: {html_info['url']} -> {status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to track HTML processing: {e}")
            return False
    
    def is_html_processed(self, url_hash: str) -> bool:
        """
        Check if an HTML page has been processed.
        
        Args:
            url_hash: URL hash to check
            
        Returns:
            True if already processed
        """
        try:
            def _op():
                return self.vector_store.es_client.get(
                    index=self.index_name,
                    id=url_hash,
                    ignore=[404]
                )
            result = with_retry(_op, policy=self.retry_policy, circuit_breaker=self.circuit_breaker, circuit_key=f"es:get:{self.index_name}")
            
            if result.get("found"):
                status = result["_source"].get("status")
                return status == "completed"
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to check HTML processing status: {e}")
            return False


class HTMLDiscoveryProcessor:
    """
    Main processor for HTML discovery and processing.
    """
    
    def __init__(self, cache: SyncCache, vector_store: VectorStoreES, 
                 openai_client, temp_directory: Optional[str] = None):
        """
        Initialize HTML discovery processor.
        
        Args:
            cache: Sync cache
            vector_store: Vector store
            openai_client: OpenAI client
            temp_directory: Temporary directory
        """
        self.cache = cache
        self.vector_store = vector_store
        self.openai_client = openai_client
        self.discovery_service = HTMLDiscoveryService(cache, vector_store)
        self.tracker = HTMLProcessingTracker(vector_store)
        self.tx = TransactionManager(vector_store)
        self.html_fetcher = HTMLFetcher(cache)
    
    def process_html_source(self, source: ContentSource) -> Dict[str, Any]:
        """
        Process an HTML source: discover and process HTML pages.
        
        Args:
            source: HTML source configuration
            
        Returns:
            Processing results summary
        """
        logger.info(f"Processing HTML source: {source.id}")
        
        results = {
            'source_id': source.id,
            'discovered_pages': 0,
            'processed_pages': 0,
            'failed_pages': 0,
            'errors': []
        }
        
        try:
            # Step 1: Discover HTML pages from index page
            html_links = self.discovery_service.discover_html_pages_from_index_page(source)
            
            results['discovered_pages'] = len(html_links)
            
            if not html_links:
                logger.info(f"No HTML pages discovered for source {source.id}")
                return results
            
            # Step 2: Filter out already processed pages
            new_pages = []
            for html_info in html_links:
                if not self.tracker.is_html_processed(html_info['url_hash']):
                    new_pages.append(html_info)
                else:
                    logger.debug(f"HTML page already processed: {html_info['filename']}")
            
            logger.info(f"Found {len(new_pages)} new HTML pages to process")
            
            # Step 3: Process each HTML page
            newest_timestamp_iso = None
            for html_info in new_pages:
                try:
                    success = self._process_single_html_page(source, html_info)
                    if success:
                        results['processed_pages'] += 1
                        # Track newest timestamp per run using tracker data is heavy; use now
                        newest_timestamp_iso = datetime.now(timezone.utc).isoformat()
                    else:
                        results['failed_pages'] += 1
                except Exception as e:
                    logger.error(f"Failed to process HTML page {html_info['url']}: {e}")
                    results['failed_pages'] += 1
                    results['errors'].append(str(e))
                    self.tracker.track_html_processing(
                        source.id, html_info, "failed", str(e)
                    )
            
            # Step 4: Transactional cleanup - mark and delete outdated for this source
            try:
                if newest_timestamp_iso:
                    index_name = self.vector_store.get_ingestion_index_name()
                    marked = self.tx.mark_outdated(index_name, source.id, newest_timestamp_iso)
                    deleted = self.tx.delete_outdated(index_name, source.id, newest_timestamp_iso)
                    logger.info(f"Cleanup for {source.id}: marked stale={marked}, deleted={deleted}")
                    results['cleanup'] = {'marked': int(marked), 'deleted': int(deleted)}
            except Exception as e:
                logger.warning(f"Cleanup step failed for {source.id}: {e}")

            logger.info(f"HTML processing completed for {source.id}: "
                       f"{results['processed_pages']} processed, "
                       f"{results['failed_pages']} failed")
            
        except Exception as e:
            logger.error(f"Failed to process HTML source {source.id}: {e}")
            results['errors'].append(str(e))
        
        return results
    
    def _process_single_html_page(self, source: ContentSource, html_info: Dict[str, Any]) -> bool:
        """
        Process a single HTML page.
        
        Args:
            source: Source configuration
            html_info: HTML page information
            
        Returns:
            True if processing was successful
        """
        try:
            logger.info(f"Processing HTML page: {html_info['url']}")
            
            # Create a temporary source for this HTML page
            temp_source = ContentSource(
                id=f"{source.id}-{html_info['url_hash']}",
                name=f"{source.name} - {html_info['link_text']}",
                description=f"Discovered HTML page: {html_info['link_text']}",
                type="html",
                html_config=source.html_config.model_copy(update={'url': html_info['url']}),
                versioning_strategy=source.versioning_strategy,
                fetch_strategy="direct",
                enabled=True,
                priority=source.priority,
                tags=source.tags + ["discovered"],
                use_document_parser=getattr(source, 'use_document_parser', False)
            )
            
            # Process the HTML page
            success, parsed_content, version_info = self.html_fetcher.process_html_source(temp_source)
            
            if success and parsed_content:
                # Store in vector store (idempotent by content hash)
                vector_store_id = self._store_html_content(source, html_info, parsed_content, version_info)
                
                # Track successful processing
                self.tracker.track_html_processing(
                    source.id, html_info, "completed",
                    content_hash=version_info.version_hash,
                    vector_store_id=vector_store_id
                )
                
                logger.info(f"Successfully processed HTML page: {html_info['url']}")
                return True
            else:
                # Track failed processing
                self.tracker.track_html_processing(
                    source.id, html_info, "failed", "Failed to fetch or parse content"
                )
                return False
                
        except Exception as e:
            logger.error(f"Failed to process HTML page {html_info['url']}: {e}")
            self.tracker.track_html_processing(
                source.id, html_info, "failed", str(e)
            )
            return False
    
    def _store_html_content(self, source: ContentSource, html_info: Dict[str, Any], 
                          parsed_content: Dict[str, Any], version_info) -> Optional[str]:
        """
        Store HTML content in vector store.
        
        Args:
            source: Source configuration
            html_info: HTML page information
            parsed_content: Parsed content
            version_info: Version information
            
        Returns:
            Vector store document ID
        """
        try:
            # Prepare document for vector store
            document = {
                "source_id": source.id,
                "source_name": source.name,
                "url": html_info["url"],
                "title": html_info["link_text"],
                "content": parsed_content.get("text_content", ""),
                "content_type": "html",
                "content_hash": version_info.version_hash,
                "timestamp": version_info.version_timestamp.isoformat(),
                "metadata": {
                    "discovered_from": source.id,
                    "link_title": html_info["link_title"],
                    "parsing_method": parsed_content.get("parsing_method", "unknown"),
                    "chunk_count": parsed_content.get("chunk_count", 0),
                    **parsed_content.get("metadata", {})
                }
            }
            
            # Add chunks if available
            if "chunks" in parsed_content:
                document["chunks"] = parsed_content["chunks"]
            
            # Store in vector store
            # Use content_hash as doc_id for idempotency
            doc_id = self.vector_store.add_document(document, doc_id=version_info.version_hash)
            
            logger.debug(f"Stored HTML content in vector store: {doc_id}")
            return doc_id
            
        except Exception as e:
            logger.error(f"Failed to store HTML content: {e}")
            return None
    
    def cleanup(self):
        """Clean up resources."""
        self.discovery_service.close()
        self.html_fetcher.close()

    def get_circuit_snapshot(self) -> Dict[str, Any]:
        """Expose circuit breaker state for reporting."""
        try:
            return self.tx.circuit_breaker.get_state_snapshot()
        except Exception:
            return {}


def process_html_source(source: ContentSource, cache: SyncCache, vector_store: VectorStoreES, 
                       openai_client, temp_directory: Optional[str] = None) -> Dict[str, Any]:
    """
    Process an HTML source with discovery capabilities.
    
    Args:
        source: HTML source configuration
        cache: Sync cache
        vector_store: Vector store
        openai_client: OpenAI client
        temp_directory: Temporary directory
        
    Returns:
        Processing results
    """
    processor = HTMLDiscoveryProcessor(cache, vector_store, openai_client, temp_directory)
    try:
        return processor.process_html_source(source)
    finally:
        processor.cleanup() 
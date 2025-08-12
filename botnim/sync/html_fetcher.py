#!/usr/bin/env python3
"""
Simplified HTML Content Fetcher for Automated Sync System.

This module implements minimal HTML fetching and parsing for the sync system.
It focuses only on what's needed: fetching content, basic text extraction,
and version tracking for change detection.
"""

import hashlib
import shutil
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup

from ..config import get_logger
from ..document_parser.html_processor.extract_structure import extract_structure_from_html, get_openai_client, build_nested_structure
from ..document_parser.html_processor.extract_content import extract_content_for_sections
from ..document_parser.html_processor.generate_markdown_files import generate_markdown_dict
from .config import ContentSource, HTMLSourceConfig, VersionInfo
from .cache import SyncCache

logger = get_logger(__name__)


def decode_url(url: str) -> str:
    """
    Decode percent-encoded URLs to make them readable.
    
    Args:
        url: URL that may contain percent-encoded characters
        
    Returns:
        Decoded URL with readable characters
    """
    try:
        return unquote(url)
    except Exception:
        # If decoding fails, return the original URL
        return url


class HTMLFetcher:
    """Fetches HTML content from web sources with minimal processing."""
    
    def __init__(self, cache: SyncCache, environment: str = "staging"):
        """Initialize HTML fetcher with cache."""
        self.cache = cache
        self.session = requests.Session()
        self.environment = environment
        # Set default headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def fetch_html_content(self, source: ContentSource) -> Tuple[bool, Optional[str], Optional[VersionInfo]]:
        """
        Fetch HTML content from a source and compute version information.
        
        Args:
            source: Content source configuration
            
        Returns:
            Tuple of (success, content, version_info)
        """
        if not source.html_config:
            logger.error(f"HTML source {source.id} missing html_config")
            return False, None, None

        retries = source.html_config.retry_attempts
        for attempt in range(retries):
            try:
                logger.info(f"Fetching HTML content from {source.html_config.url} (Attempt {attempt + 1}/{retries})")
                
                # Handle file:// URLs for local files
                if source.html_config.url.startswith('file://'):
                    content = self._fetch_local_file(source.html_config.url)
                    if content is None:
                        raise Exception("Failed to read local file")
                    
                    # Create a mock response for version info
                    class MockResponse:
                        def __init__(self, content):
                            self.content = content.encode('utf-8')
                            self.headers = {}
                            self.status_code = 200
                    
                    response = MockResponse(content)
                    response.encoding = source.html_config.encoding
                else:
                    response = self.session.get(
                        source.html_config.url,
                        headers=source.html_config.headers,
                        timeout=source.html_config.timeout,
                        stream=True
                    )
                    response.raise_for_status()
                    response.encoding = source.html_config.encoding
                    content = response.text
                
                version_info = self._compute_version_info(source, content, response)
                
                logger.info(f"Successfully fetched {len(content)} characters from {source.html_config.url}")
                return True, content, version_info
                
            except Exception as e:
                logger.warning(f"Failed to fetch HTML from {source.html_config.url} on attempt {attempt + 1}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"All {retries} retry attempts failed for {source.html_config.url}")
                    return False, None, None
        return False, None, None
    
    def _fetch_local_file(self, file_url: str) -> Optional[str]:
        """
        Fetch content from a local file using file:// URL.
        
        Args:
            file_url: file:// URL pointing to local file
            
        Returns:
            File content as string, or None if failed
        """
        try:
            # Remove file:// prefix and decode URL
            file_path = file_url.replace('file://', '')
            file_path = decode_url(file_path)
            
            # Convert to Path object and resolve
            path = Path(file_path).resolve()
            
            if not path.exists():
                logger.error(f"Local file does not exist: {path}")
                return None
            
            # Read file content
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            logger.info(f"Successfully read {len(content)} characters from local file: {path}")
            return content
            
        except Exception as e:
            logger.error(f"Failed to read local file {file_url}: {e}")
            return None
    
    def parse_html_content(self, source: ContentSource, content: str) -> Dict[str, Any]:
        """
        Parse HTML content and extract only what we need for sync.
        
        Args:
            source: Content source configuration
            content: Raw HTML content
            
        Returns:
            Parsed content with minimal structure
        """
        try:
            logger.info(f"Parsing HTML content for source {source.id}")
            
            # Check if this source should use advanced document parsing
            if hasattr(source, 'use_document_parser') and source.use_document_parser:
                return self._parse_with_document_parser(source, content)
            
            # Fall back to simple parsing
            return self._parse_simple_html(source, content)
            
        except Exception as e:
            logger.error(f"Failed to parse HTML content for {source.id}: {e}")
            return {
                'raw_html': content,
                'text_content': content,
                'metadata': {},
                'error': str(e)
            }
    
    def _parse_with_document_parser(self, source: ContentSource, content: str) -> Dict[str, Any]:
        """
        Parse HTML content using the advanced document parser for proper chunking.
        
        Args:
            source: Content source configuration
            content: Raw HTML content
            
        Returns:
            Parsed content with structure and chunks
        """
        try:
            logger.info(f"Using advanced document parser for {source.id}")
            
            # Get OpenAI client from the orchestrator's environment
            client = get_openai_client(self.environment)
            
            # Get model and max_tokens from source config if available, else use defaults
            model = getattr(source, 'model', 'gpt-4o-mini')
            max_tokens = getattr(source, 'max_tokens', 10000)

            # Extract document structure
            structure_items = extract_structure_from_html(
                content,
                client,
                model,
                max_tokens,
                "סעיף"  # Default to Hebrew clauses
            )
            
            # Build nested structure
            nested_structure = build_nested_structure(structure_items)
            
            # Extract content for sections
            structure_with_content = extract_content_for_sections(
                content, 
                {"structure": nested_structure}, 
                "סעיף",  # Extract clauses
                mediawiki_mode=True  # For Wikisource
            )
            
            # Generate markdown chunks
            document_name = source.name or source.id
            markdown_chunks = generate_markdown_dict(
                structure_with_content.get("structure", []), 
                document_name
            )
            
            # Prepare result with chunks
            result = {
                'raw_html': content,
                'text_content': self._extract_text_content_from_structure(structure_with_content),
                'metadata': self._extract_basic_metadata_from_structure(structure_with_content, source),
                'chunks': markdown_chunks,
                'structure': structure_with_content,
                'chunk_count': len(markdown_chunks),
                'parsing_method': 'document_parser'
            }
            
            logger.info(f"Advanced parsing completed for {source.id}: {len(markdown_chunks)} chunks generated")
            return result
            
        except Exception as e:
            logger.error(f"Advanced document parsing failed for {source.id}: {e}")
            # Fall back to simple parsing
            return self._parse_simple_html(source, content)
    
    def _parse_simple_html(self, source: ContentSource, content: str) -> Dict[str, Any]:
        """
        Simple HTML parsing (original method).
        
        Args:
            source: Content source configuration
            content: Raw HTML content
            
        Returns:
            Parsed content with minimal structure
        """
        soup = BeautifulSoup(content, 'html.parser')
        
        # Extract content based on selector if specified
        if source.html_config.selector:
            selected_content = soup.select(source.html_config.selector)
            if selected_content:
                # Use the first matching element
                main_content = selected_content[0]
            else:
                logger.warning(f"Selector '{source.html_config.selector}' not found, using full content")
                main_content = soup
        else:
            main_content = soup
        
        # Extract only what we need for sync
        parsed_content = {
            'raw_html': str(main_content),
            'text_content': self._extract_text_content(main_content),
            'metadata': self._extract_basic_metadata(soup, source),
            'parsing_method': 'simple'
        }
        
        logger.info(f"Simple parsing completed for {source.id}")
        return parsed_content
    
    def _extract_text_content_from_structure(self, structure_data: Dict[str, Any]) -> str:
        """Extract text content from structured data."""
        text_parts = []
        
        def extract_from_items(items):
            for item in items:
                if item.get('content'):
                    text_parts.append(item['content'])
                if 'children' in item:
                    extract_from_items(item['children'])
        
        extract_from_items(structure_data.get('structure', []))
        return '\n\n'.join(text_parts)
    
    def _extract_basic_metadata_from_structure(self, structure_data: Dict[str, Any], source: ContentSource) -> Dict[str, Any]:
        """Extract metadata from structured data."""
        metadata = {
            'source_id': source.id,
            'source_name': source.name,
            'source_type': source.type,
            'parsing_method': 'document_parser',
            'structure_items': len(structure_data.get('structure', [])),
            'has_content': bool(structure_data.get('structure'))
        }
        
        # Add structure metadata if available
        if 'metadata' in structure_data:
            metadata.update(structure_data['metadata'])
        
        return metadata
    
    def _compute_version_info(self, source: ContentSource, content: str, response: requests.Response) -> VersionInfo:
        """Compute version information for the content."""
        now = datetime.now(timezone.utc)
        
        # Compute content hash
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        
        # Get timestamp from response headers or use current time
        version_timestamp = self._get_response_timestamp(response) or now
        
        # Get ETag if available
        etag = response.headers.get('ETag')
        
        # Get content size
        content_size = len(content.encode('utf-8'))
        
        return VersionInfo(
            source_id=source.id,
            version_hash=content_hash,
            version_timestamp=version_timestamp,
            version_string=source.version_string,
            etag=etag,
            content_size=content_size,
            last_fetch=now,
            fetch_status="success"
        )
    
    def _get_response_timestamp(self, response: requests.Response) -> Optional[datetime]:
        """Extract timestamp from response headers."""
        # Try Last-Modified header first
        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            try:
                return parsedate_to_datetime(last_modified)
            except Exception:
                pass
        
        # Try Date header as fallback
        date_header = response.headers.get('Date')
        if date_header:
            try:
                return parsedate_to_datetime(date_header)
            except Exception:
                pass
        
        return None
    
    def _extract_text_content(self, element) -> str:
        """Extract clean text content from HTML element."""
        # Remove script and style elements
        for script in element(["script", "style"]):
            script.decompose()
        
        # Get text and clean it up
        text = element.get_text(separator=' ', strip=True)
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def _extract_basic_metadata(self, soup, source: ContentSource) -> Dict[str, Any]:
        """Extract only basic metadata we need for sync."""
        metadata = {
            'title': '',
            'charset': '',
            'language': ''
        }
        
        # Extract title
        title_tag = soup.find('title')
        if title_tag:
            metadata['title'] = title_tag.get_text(strip=True)
        
        # Extract charset
        charset_meta = soup.find('meta', charset=True)
        if charset_meta:
            metadata['charset'] = charset_meta.get('charset', '')
        
        # Extract language
        html_tag = soup.find('html')
        if html_tag:
            metadata['language'] = html_tag.get('lang', '')
        
        return metadata
    
    def process_html_source(self, source: ContentSource) -> Tuple[bool, Optional[Dict[str, Any]], Optional[VersionInfo]]:
        """
        Complete HTML source processing: fetch, parse, and version.
        
        Args:
            source: Content source configuration
            
        Returns:
            Tuple of (success, parsed_content, version_info)
        """
        logger.info(f"Processing HTML source: {source.id}")
        
        # Fetch HTML content
        success, content, version_info = self.fetch_html_content(source)
        if not success or not content:
            return False, None, None
        
        # Parse HTML content
        parsed_content = self.parse_html_content(source, content)
        
        # Add source metadata
        parsed_content['source_metadata'] = {
            'source_id': source.id,
            'source_name': source.name,
            'source_url': decode_url(source.html_config.url),
            'fetch_timestamp': version_info.last_fetch.isoformat(),
            'content_size': version_info.content_size,
            'version_hash': version_info.version_hash
        }
        
        logger.info(f"Successfully processed HTML source: {source.id}")
        return True, parsed_content, version_info
    
    def close(self):
        """Close the session."""
        self.session.close()


class HTMLProcessor:
    """High-level HTML processing orchestrator."""
    
    def __init__(self, cache: SyncCache, environment: str = "staging"):
        """Initialize HTML processor."""
        self.cache = cache
        self.fetcher = HTMLFetcher(cache, environment)
    
    def process_sources(self, sources: List[ContentSource]) -> Dict[str, Any]:
        """
        Process multiple HTML sources.
        
        Args:
            sources: List of HTML content sources
            
        Returns:
            Dictionary with processing results
        """
        results = {
            'processed': [],
            'skipped': [],
            'errors': [],
            'summary': {
                'total_sources': len(sources),
                'processed_count': 0,
                'skipped_count': 0,
                'error_count': 0
            }
        }
        
        for source in sources:
            try:
                # First, fetch the content to get version info
                success, content, version_info = self.fetcher.fetch_html_content(source)

                if not success or not content or not version_info:
                    results['errors'].append({
                        'source_id': source.id,
                        'error': 'Failed to fetch HTML content'
                    })
                    results['summary']['error_count'] += 1
                    continue

                # Then, check if we should process this source
                should_process, reason = self._should_process_source(source, version_info)
                
                if not should_process:
                    results['skipped'].append({
                        'source_id': source.id,
                        'reason': reason
                    })
                    results['summary']['skipped_count'] += 1
                    logger.info(f"Skipped processing {source.id}: {reason}")
                    continue
                
                # Parse the content
                parsed_content = self.fetcher.parse_html_content(source, content)
                
                # Cache the processed content
                self.cache.cache_content(
                    source_id=source.id,
                    content_hash=version_info.version_hash,
                    content_size=version_info.content_size,
                    metadata={
                        'parsed_content': parsed_content,
                        'version_info': version_info.model_dump(mode='json')
                    },
                    processed=True
                )
                
                results['processed'].append({
                    'source_id': source.id,
                    'version_hash': version_info.version_hash,
                    'content_size': version_info.content_size,
                    'parsed_content': parsed_content
                })
                results['summary']['processed_count'] += 1
                
                logger.info(f"Successfully processed {source.id}")
                
            except Exception as e:
                logger.error(f"Error processing HTML source {source.id}: {e}")
                results['errors'].append({
                    'source_id': source.id,
                    'error': str(e)
                })
                results['summary']['error_count'] += 1
        
        return results
    
    def _should_process_source(self, source: ContentSource, version_info: VersionInfo) -> Tuple[bool, str]:
        """Determine if a source should be processed based on caching and versioning."""
        return self.cache.should_process_source(source, version_info.version_hash, version_info.content_size)
    
    def close(self):
        """Close the processor and fetcher."""
        self.fetcher.close()


# Convenience functions for easy integration
def fetch_and_parse_html(url: str, selector: Optional[str] = None, 
                        encoding: str = "utf-8", timeout: int = 30) -> Dict[str, Any]:
    """
    Convenience function to fetch and parse a single HTML URL.
    
    Args:
        url: URL to fetch
        selector: CSS selector for content extraction
        encoding: Content encoding
        timeout: Request timeout
        
    Returns:
        Parsed content dictionary
    """
    # Create a temporary cache for this operation
    temp_cache = SyncCache("./temp_cache")
    
    # Create a temporary source configuration
    temp_source = ContentSource(
        id="temp_source",
        name="Temporary HTML Source",
        type="html",
        html_config=HTMLSourceConfig(
            url=url,
            selector=selector,
            encoding=encoding,
            timeout=timeout
        )
    )
    
    try:
        processor = HTMLProcessor(temp_cache)
        fetcher = processor.fetcher
        
        # Fetch and parse
        success, parsed_content, version_info = fetcher.process_html_source(temp_source)
        
        if success and parsed_content:
            return parsed_content
        else:
            return {"error": "Failed to fetch and parse HTML"}
            
    finally:
        processor.close()
        # Clean up temp cache
        if Path("./temp_cache").exists():
            shutil.rmtree("./temp_cache")


if __name__ == "__main__":
    # Example usage
    result = fetch_and_parse_html(
        "https://main.knesset.gov.il/about/lexicon/pages/default.aspx",
        selector="#content"
    )
    print(f"Title: {result.get('metadata', {}).get('title', 'N/A')}")
    print(f"Content length: {len(result.get('text_content', ''))}") 
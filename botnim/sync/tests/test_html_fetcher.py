#!/usr/bin/env python3
"""
Unit tests for simplified HTML fetcher functionality.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest
from datetime import datetime, timezone
from bs4 import BeautifulSoup

from ..html_fetcher import HTMLFetcher, HTMLProcessor, fetch_and_parse_html
from ..config import ContentSource, HTMLSourceConfig, SourceType, VersioningStrategy
from ..cache import SyncCache


class TestHTMLFetcher:
    """Test simplified HTML fetcher functionality."""
    
    @pytest.fixture
    def temp_cache(self):
        """Create a temporary cache for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = SyncCache(temp_dir)
            yield cache
    
    @pytest.fixture
    def html_fetcher(self, temp_cache):
        """Create an HTML fetcher instance."""
        return HTMLFetcher(temp_cache)
    
    @pytest.fixture
    def sample_html_source(self):
        """Create a sample HTML source configuration."""
        return ContentSource(
            id="test-html-source",
            name="Test HTML Source",
            description="Test HTML source for unit testing",
            type=SourceType.HTML,
            html_config=HTMLSourceConfig(
                url="https://example.com/test",
                selector="#content",
                encoding="utf-8",
                timeout=30
            ),
            versioning_strategy=VersioningStrategy.HASH,
            tags=["test", "html"]
        )
    
    @pytest.fixture
    def sample_html_content(self):
        """Sample HTML content for testing."""
        return """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <title>Test Page</title>
        </head>
        <body>
            <div id="content">
                <h1>Main Title</h1>
                <p>This is a test paragraph with <a href="https://example.com">a link</a>.</p>
                <h2>Subtitle</h2>
                <ul>
                    <li>Item 1</li>
                    <li>Item 2</li>
                </ul>
            </div>
        </body>
        </html>
        """
    
    def test_html_fetcher_initialization(self, temp_cache):
        """Test HTML fetcher initialization."""
        fetcher = HTMLFetcher(temp_cache)
        assert fetcher.cache == temp_cache
        assert fetcher.session is not None
        assert 'User-Agent' in fetcher.session.headers
        
        # Test session headers
        headers = fetcher.session.headers
        assert 'Mozilla' in headers['User-Agent']
        assert 'text/html' in headers['Accept']
    
    @patch('requests.Session.get')
    def test_fetch_html_content_success(self, mock_get, html_fetcher, sample_html_source, sample_html_content):
        """Test successful HTML content fetching."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = sample_html_content
        mock_response.encoding = 'utf-8'
        mock_response.headers = {
            'Last-Modified': 'Wed, 15 Jan 2024 10:30:00 GMT',
            'ETag': '"abc123"'
        }
        mock_get.return_value = mock_response
        
        # Fetch content
        success, content, version_info = html_fetcher.fetch_html_content(sample_html_source)
        
        # Verify results
        assert success is True
        assert content == sample_html_content
        assert version_info is not None
        assert version_info.source_id == "test-html-source"
        assert version_info.version_hash is not None
        assert version_info.etag == '"abc123"'
        assert version_info.fetch_status == "success"
        
        # Verify request was made correctly
        mock_get.assert_called_once_with(
            "https://example.com/test",
            headers={},
            timeout=30,
            stream=True
        )
    
    @patch('requests.Session.get')
    def test_fetch_html_content_http_error(self, mock_get, html_fetcher, sample_html_source):
        """Test HTML fetching with HTTP error."""
        # Mock HTTP error
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")
        mock_get.return_value = mock_response
        
        # Fetch content
        success, content, version_info = html_fetcher.fetch_html_content(sample_html_source)
        
        # Verify results
        assert success is False
        assert content is None
        assert version_info is None
    
    @patch('requests.Session.get')
    def test_fetch_html_content_network_error(self, mock_get, html_fetcher, sample_html_source):
        """Test HTML fetching with network error."""
        # Mock network error
        mock_get.side_effect = Exception("Network error")
        
        # Fetch content
        success, content, version_info = html_fetcher.fetch_html_content(sample_html_source)
        
        # Verify results
        assert success is False
        assert content is None
        assert version_info is None
    
    def test_parse_html_content_with_selector(self, html_fetcher, sample_html_source, sample_html_content):
        """Test HTML parsing with CSS selector."""
        parsed_content = html_fetcher.parse_html_content(sample_html_source, sample_html_content)
        
        # Verify basic structure (simplified)
        assert 'raw_html' in parsed_content
        assert 'text_content' in parsed_content
        assert 'metadata' in parsed_content
        
        # Verify content was extracted correctly
        assert 'Main Title' in parsed_content['text_content']
        assert 'test paragraph' in parsed_content['text_content']
        assert parsed_content['metadata']['title'] == 'Test Page'
        
        # Verify we don't have the removed features
        assert 'markdown' not in parsed_content
        assert 'links' not in parsed_content
        assert 'structure' not in parsed_content
    
    def test_parse_html_content_without_selector(self, html_fetcher, sample_html_source, sample_html_content):
        """Test HTML parsing without CSS selector."""
        # Remove selector from source
        sample_html_source.html_config.selector = None
        
        parsed_content = html_fetcher.parse_html_content(sample_html_source, sample_html_content)
        
        # Should still parse successfully
        assert 'raw_html' in parsed_content
        assert 'text_content' in parsed_content
        assert 'metadata' in parsed_content
    
    def test_parse_html_content_invalid_selector(self, html_fetcher, sample_html_source, sample_html_content):
        """Test HTML parsing with invalid CSS selector."""
        # Set invalid selector
        sample_html_source.html_config.selector = "#nonexistent"
        
        parsed_content = html_fetcher.parse_html_content(sample_html_source, sample_html_content)
        
        # Should fall back to full content
        assert 'raw_html' in parsed_content
        assert 'text_content' in parsed_content
        assert 'metadata' in parsed_content
    
    def test_extract_text_content(self, html_fetcher, sample_html_content):
        """Test text content extraction."""
        soup = BeautifulSoup(sample_html_content, 'html.parser')
        text_content = html_fetcher._extract_text_content(soup)
        
        # Should contain main content but not scripts/styles
        assert 'Main Title' in text_content
        assert 'test paragraph' in text_content
        assert 'Item 1' in text_content
        assert 'Item 2' in text_content
    
    def test_extract_basic_metadata(self, html_fetcher, sample_html_source, sample_html_content):
        """Test basic metadata extraction."""
        soup = BeautifulSoup(sample_html_content, 'html.parser')
        metadata = html_fetcher._extract_basic_metadata(soup, sample_html_source)
        
        # Verify basic metadata extraction
        assert metadata['title'] == 'Test Page'
        assert metadata['charset'] == 'utf-8'
        assert metadata['language'] == 'en'
        
        # Verify we don't have the removed metadata fields
        assert 'description' not in metadata
        assert 'keywords' not in metadata
        assert 'author' not in metadata
    
    def test_compute_version_info(self, html_fetcher, sample_html_source, sample_html_content):
        """Test version information computation."""
        # Create mock response
        mock_response = Mock()
        mock_response.headers = {
            'Last-Modified': 'Wed, 15 Jan 2024 10:30:00 GMT',
            'ETag': '"abc123"'
        }
        
        version_info = html_fetcher._compute_version_info(sample_html_source, sample_html_content, mock_response)
        
        # Verify version info
        assert version_info.source_id == "test-html-source"
        assert version_info.version_hash is not None
        assert version_info.content_size > 0
        assert version_info.etag == '"abc123"'
        assert version_info.fetch_status == "success"
    
    def test_get_response_timestamp(self, html_fetcher):
        """Test timestamp extraction from response headers."""
        # Test with Last-Modified header
        mock_response = Mock()
        mock_response.headers = {'Last-Modified': 'Wed, 15 Jan 2024 10:30:00 GMT'}
        
        timestamp = html_fetcher._get_response_timestamp(mock_response)
        assert timestamp is not None
        assert isinstance(timestamp, datetime)
        
        # Test with Date header
        mock_response.headers = {'Date': 'Wed, 15 Jan 2024 10:30:00 GMT'}
        timestamp = html_fetcher._get_response_timestamp(mock_response)
        assert timestamp is not None
        
        # Test with no timestamp headers
        mock_response.headers = {}
        timestamp = html_fetcher._get_response_timestamp(mock_response)
        assert timestamp is None
    
    def test_process_html_source_complete(self, html_fetcher, sample_html_source, sample_html_content):
        """Test complete HTML source processing."""
        with patch.object(html_fetcher, 'fetch_html_content') as mock_fetch:
            # Mock successful fetch
            mock_fetch.return_value = (True, sample_html_content, Mock())
            
            success, parsed_content, version_info = html_fetcher.process_html_source(sample_html_source)
            
            # Verify results
            assert success is True
            assert parsed_content is not None
            assert version_info is not None
            assert 'source_metadata' in parsed_content
            assert parsed_content['source_metadata']['source_id'] == "test-html-source"
    
    def test_process_html_source_fetch_failure(self, html_fetcher, sample_html_source):
        """Test HTML source processing with fetch failure."""
        with patch.object(html_fetcher, 'fetch_html_content') as mock_fetch:
            # Mock failed fetch
            mock_fetch.return_value = (False, None, None)
            
            success, parsed_content, version_info = html_fetcher.process_html_source(sample_html_source)
            
            # Verify results
            assert success is False
            assert parsed_content is None
            assert version_info is None
    
    def test_html_fetcher_close(self, html_fetcher):
        """Test HTML fetcher cleanup."""
        html_fetcher.close()
        # Session should be closed (though we can't easily test this without mocking)


class TestHTMLProcessor:
    """Test HTML processor functionality."""
    
    @pytest.fixture
    def temp_cache(self):
        """Create a temporary cache for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = SyncCache(temp_dir)
            yield cache
    
    @pytest.fixture
    def html_processor(self, temp_cache):
        """Create an HTML processor instance."""
        return HTMLProcessor(temp_cache)
    
    @pytest.fixture
    def sample_sources(self):
        """Create sample HTML sources."""
        return [
            ContentSource(
                id="source-1",
                name="Source 1",
                type=SourceType.HTML,
                html_config=HTMLSourceConfig(url="https://example.com/1")
            ),
            ContentSource(
                id="source-2",
                name="Source 2",
                type=SourceType.HTML,
                html_config=HTMLSourceConfig(url="https://example.com/2")
            )
        ]
    
    def test_html_processor_initialization(self, temp_cache):
        """Test HTML processor initialization."""
        processor = HTMLProcessor(temp_cache)
        assert processor.cache == temp_cache
        assert processor.fetcher is not None
    
    @patch.object(HTMLFetcher, 'process_html_source')
    def test_process_sources_success(self, mock_process, html_processor, sample_sources):
        """Test processing multiple sources successfully."""
        # Mock successful processing for all sources
        from ..config import VersionInfo
        from datetime import datetime, timezone
        
        mock_version_info = VersionInfo(
            source_id="test",
            version_hash="test_hash",
            version_timestamp=datetime.now(timezone.utc),
            content_size=100,
            last_fetch=datetime.now(timezone.utc),
            fetch_status="success"
        )
        mock_process.return_value = (True, {'test': 'content'}, mock_version_info)
        
        results = html_processor.process_sources(sample_sources)
        
        # Verify results structure
        assert 'processed' in results
        assert 'skipped' in results
        assert 'errors' in results
        assert 'summary' in results
        
        # Verify summary
        summary = results['summary']
        assert summary['total_sources'] == 2
        assert summary['processed_count'] == 2
        assert summary['skipped_count'] == 0
        assert summary['error_count'] == 0
    
    @patch.object(HTMLFetcher, 'process_html_source')
    def test_process_sources_with_errors(self, mock_process, html_processor, sample_sources):
        """Test processing sources with some errors."""
        # Mock mixed results
        from ..config import VersionInfo
        from datetime import datetime, timezone
        
        mock_version_info = VersionInfo(
            source_id="test",
            version_hash="test_hash",
            version_timestamp=datetime.now(timezone.utc),
            content_size=100,
            last_fetch=datetime.now(timezone.utc),
            fetch_status="success"
        )
        
        def mock_process_side_effect(source):
            if source.id == "source-1":
                return (True, {'test': 'content'}, mock_version_info)
            else:
                return (False, None, None)
        
        mock_process.side_effect = mock_process_side_effect
        
        results = html_processor.process_sources(sample_sources)
        
        # Verify results
        summary = results['summary']
        assert summary['total_sources'] == 2
        assert summary['processed_count'] == 1
        assert summary['skipped_count'] == 0
        assert summary['error_count'] == 1
    
    def test_should_process_source(self, html_processor, sample_sources):
        """Test source processing decision logic."""
        should_process, reason = html_processor._should_process_source(sample_sources[0])
        
        # Currently always returns True
        assert should_process is True
        assert "Processing required" in reason
    
    def test_html_processor_close(self, html_processor):
        """Test HTML processor cleanup."""
        html_processor.close()
        # Should close the fetcher


class TestFetchAndParseHTML:
    """Test convenience function."""
    
    @patch('botnim.sync.html_fetcher.HTMLFetcher.process_html_source')
    def test_fetch_and_parse_html_success(self, mock_process):
        """Test successful HTML fetching and parsing."""
        # Mock successful processing
        mock_process.return_value = (True, {'test': 'content'}, Mock())
        
        result = fetch_and_parse_html("https://example.com", "#content")
        
        # Verify result
        assert result == {'test': 'content'}
    
    @patch('botnim.sync.html_fetcher.HTMLFetcher.process_html_source')
    def test_fetch_and_parse_html_failure(self, mock_process):
        """Test failed HTML fetching and parsing."""
        # Mock failed processing
        mock_process.return_value = (False, None, None)
        
        result = fetch_and_parse_html("https://example.com", "#content")
        
        # Verify result
        assert result == {"error": "Failed to fetch and parse HTML"}


# Integration tests
class TestHTMLFetcherIntegration:
    """Integration tests for HTML fetcher."""
    
    @pytest.fixture
    def temp_cache(self):
        """Create a temporary cache for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = SyncCache(temp_dir)
            yield cache
    
    def test_real_html_parsing(self, temp_cache):
        """Test parsing of real HTML content."""
        html_content = """
        <!DOCTYPE html>
        <html>
        <head><title>Test</title></head>
        <body>
            <div id="content">
                <h1>Hello World</h1>
                <p>This is a <strong>test</strong> paragraph.</p>
                <a href="https://example.com">Example Link</a>
            </div>
        </body>
        </html>
        """
        
        source = ContentSource(
            id="test",
            name="Test",
            type=SourceType.HTML,
            html_config=HTMLSourceConfig(
                url="https://example.com",
                selector="#content"
            )
        )
        
        fetcher = HTMLFetcher(temp_cache)
        parsed_content = fetcher.parse_html_content(source, html_content)
        
        # Verify parsing results (simplified)
        assert 'Hello World' in parsed_content['text_content']
        assert 'test paragraph' in parsed_content['text_content']
        assert parsed_content['metadata']['title'] == 'Test'
        
        # Verify we don't have removed features
        assert 'markdown' not in parsed_content
        assert 'links' not in parsed_content
        assert 'structure' not in parsed_content
        
        fetcher.close()


if __name__ == "__main__":
    pytest.main([__file__]) 
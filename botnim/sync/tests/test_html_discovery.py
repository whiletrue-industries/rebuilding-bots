"""
Tests for HTML discovery functionality.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from ..config import ContentSource, HTMLSourceConfig, FetchStrategy
from ..cache import SyncCache
from ..html_discovery import HTMLDiscoveryService, HTMLProcessingTracker, HTMLDiscoveryProcessor
from botnim.vector_store.vector_store_es import VectorStoreES


class TestHTMLDiscoveryService(unittest.TestCase):
    """Test HTML discovery service."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_cache = Mock(spec=SyncCache)
        self.mock_vector_store = Mock(spec=VectorStoreES)
        self.discovery_service = HTMLDiscoveryService(self.mock_cache, self.mock_vector_store)
        
        # Create test source
        self.test_source = ContentSource(
            id="test-html-source",
            name="Test HTML Source",
            description="Test HTML source for discovery",
            type="html",
            html_config=HTMLSourceConfig(
                url="https://example.com/index.html",
                selector="#content",
                link_pattern=".*test.*",
                timeout=30
            ),
            fetch_strategy=FetchStrategy.INDEX_PAGE,
            enabled=True,
            priority=1,
            tags=["test"]
        )
    
    def test_is_html_link(self):
        """Test HTML link detection."""
        # Test HTML files
        self.assertTrue(self.discovery_service._is_html_link("https://example.com/page.html"))
        self.assertTrue(self.discovery_service._is_html_link("https://example.com/page.htm"))
        self.assertTrue(self.discovery_service._is_html_link("https://example.com/page.aspx"))
        self.assertTrue(self.discovery_service._is_html_link("https://example.com/page.php"))
        
        # Test URLs without file extensions (likely HTML)
        self.assertTrue(self.discovery_service._is_html_link("https://example.com/page"))
        self.assertTrue(self.discovery_service._is_html_link("https://example.com/"))
        
        # Test non-HTML files
        self.assertFalse(self.discovery_service._is_html_link("https://example.com/file.pdf"))
        self.assertFalse(self.discovery_service._is_html_link("https://example.com/image.jpg"))
        self.assertFalse(self.discovery_service._is_html_link("https://example.com/data.json"))
        
        # Test with custom pattern
        self.assertTrue(self.discovery_service._is_html_link("https://example.com/test-page", ".*test.*"))
        # URLs that don't match the pattern are not considered HTML when pattern is provided
        self.assertFalse(self.discovery_service._is_html_link("https://example.com/other-page", ".*test.*"))
        
        # Test with pattern on URLs that have file extensions
        self.assertTrue(self.discovery_service._is_html_link("https://example.com/test-page.html", ".*test.*"))
        self.assertFalse(self.discovery_service._is_html_link("https://example.com/other-page.html", ".*test.*"))
    
    def test_extract_filename(self):
        """Test filename extraction."""
        self.assertEqual(self.discovery_service._extract_filename("https://example.com/page.html"), "page.html")
        self.assertEqual(self.discovery_service._extract_filename("https://example.com/subdir/page.htm"), "page.htm")
        self.assertEqual(self.discovery_service._extract_filename("https://example.com/"), "index")
        self.assertEqual(self.discovery_service._extract_filename("https://example.com/page"), "page")
    
    @patch('requests.Session.get')
    def test_discover_html_pages_from_index_page(self, mock_get):
        """Test HTML page discovery from index page."""
        # Mock HTML response with links
        html_content = """
        <html>
            <body>
                <div id="content">
                    <a href="test-page-1.html">Test Page 1</a>
                    <a href="https://example.com/test-page-2.html">Test Page 2</a>
                    <a href="other-page.html">Other Page</a>
                    <a href="document.pdf">PDF Document</a>
                </div>
            </body>
        </html>
        """
        
        mock_response = Mock()
        mock_response.content = html_content.encode()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test discovery
        html_links = self.discovery_service.discover_html_pages_from_index_page(self.test_source)
        
        # Should find 2 HTML links that match the pattern ".*test.*" (excluding PDF and other-page.html)
        self.assertEqual(len(html_links), 2)
        
        # Check that all links have required fields
        for link in html_links:
            self.assertIn('url', link)
            self.assertIn('filename', link)
            self.assertIn('url_hash', link)
            self.assertIn('discovered_at', link)
            self.assertIn('link_text', link)
            self.assertTrue(link['url'].endswith('.html'))
    
    def tearDown(self):
        """Clean up after tests."""
        self.discovery_service.close()


class TestHTMLProcessingTracker(unittest.TestCase):
    """Test HTML processing tracker."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_vector_store = Mock(spec=VectorStoreES)
        self.mock_vector_store.es = Mock()
        self.tracker = HTMLProcessingTracker(self.mock_vector_store)
    
    def test_track_html_processing(self):
        """Test HTML processing tracking."""
        html_info = {
            'url': 'https://example.com/test.html',
            'url_hash': 'test_hash_123',
            'link_text': 'Test Page'
        }
        
        # Mock successful tracking
        self.mock_vector_store.es.index.return_value = {'_id': 'test_id'}
        
        result = self.tracker.track_html_processing(
            source_id="test-source",
            html_info=html_info,
            status="completed",
            content_hash="content_hash_123",
            vector_store_id="vector_id_123"
        )
        
        self.assertTrue(result)
        self.mock_vector_store.es.index.assert_called_once()
    
    def test_is_html_processed(self):
        """Test HTML processing status check."""
        # Mock existing processed document
        mock_result = {
            'found': True,
            '_source': {'status': 'completed'}
        }
        self.mock_vector_store.es.get.return_value = mock_result
        
        result = self.tracker.is_html_processed("test_hash_123")
        self.assertTrue(result)
        
        # Mock non-existent document
        mock_result = {'found': False}
        self.mock_vector_store.es.get.return_value = mock_result
        
        result = self.tracker.is_html_processed("test_hash_456")
        self.assertFalse(result)


class TestHTMLDiscoveryProcessor(unittest.TestCase):
    """Test HTML discovery processor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_cache = Mock(spec=SyncCache)
        self.mock_vector_store = Mock(spec=VectorStoreES)
        self.mock_openai_client = Mock()
        
        self.processor = HTMLDiscoveryProcessor(
            cache=self.mock_cache,
            vector_store=self.mock_vector_store,
            openai_client=self.mock_openai_client
        )
        
        # Create test source
        self.test_source = ContentSource(
            id="test-html-source",
            name="Test HTML Source",
            description="Test HTML source for discovery",
            type="html",
            html_config=HTMLSourceConfig(
                url="https://example.com/index.html",
                selector="#content",
                link_pattern=".*test.*",
                timeout=30
            ),
            fetch_strategy=FetchStrategy.INDEX_PAGE,
            enabled=True,
            priority=1,
            tags=["test"]
        )
    
    @patch.object(HTMLDiscoveryService, 'discover_html_pages_from_index_page')
    @patch.object(HTMLProcessingTracker, 'is_html_processed')
    def test_process_html_source_no_new_pages(self, mock_is_processed, mock_discover):
        """Test processing when no new pages are found."""
        # Mock no HTML pages discovered
        mock_discover.return_value = []
        
        results = self.processor.process_html_source(self.test_source)
        
        self.assertEqual(results['discovered_pages'], 0)
        self.assertEqual(results['processed_pages'], 0)
        self.assertEqual(results['failed_pages'], 0)
        self.assertEqual(len(results['errors']), 0)
    
    def tearDown(self):
        """Clean up after tests."""
        self.processor.cleanup()


if __name__ == '__main__':
    unittest.main() 
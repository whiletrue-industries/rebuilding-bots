#!/usr/bin/env python3
"""
Test script for PDF discovery and processing functionality.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from botnim.sync.pdf_discovery import (
    PDFDiscoveryService, 
    PDFDownloadManager, 
    PDFProcessingTracker,
    PDFDiscoveryProcessor
)
from botnim.sync.config import ContentSource, PDFSourceConfig, SourceType
from botnim.sync.cache import SyncCache


class TestPDFDiscoveryService(unittest.TestCase):
    """Test PDF discovery service functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.cache = Mock()
        self.vector_store = Mock()
        self.discovery_service = PDFDiscoveryService(self.cache, self.vector_store)
        
        # Create a test PDF source
        self.test_source = ContentSource(
            id="test-pdf-source",
            name="Test PDF Source",
            type=SourceType.PDF,
            pdf_config=PDFSourceConfig(
                url="https://example.com/pdfs/",
                is_index_page=True,
                file_pattern="*.pdf",
                timeout=30
            )
        )
    
    def test_extract_filename_from_url(self):
        """Test filename extraction from URLs."""
        test_cases = [
            ("https://example.com/document.pdf", "document.pdf"),
            ("https://example.com/path/to/file.pdf", "file.pdf"),
            ("https://example.com/", "document_"),
            ("https://example.com/path/", "document_"),
        ]
        
        for url, expected_prefix in test_cases:
            filename = self.discovery_service._extract_filename(url)
            if expected_prefix == "document_":
                # Should generate a hash-based filename
                self.assertTrue(filename.startswith("document_"))
                self.assertTrue(filename.endswith(".pdf"))
            else:
                self.assertEqual(filename, expected_prefix)
    
    def test_is_pdf_link(self):
        """Test PDF link detection."""
        # Valid PDF links
        self.assertTrue(self.discovery_service._is_pdf_link("https://example.com/file.pdf"))
        self.assertTrue(self.discovery_service._is_pdf_link("https://example.com/path/document.PDF"))
        
        # Invalid links
        self.assertFalse(self.discovery_service._is_pdf_link("https://example.com/file.txt"))
        self.assertFalse(self.discovery_service._is_pdf_link("https://example.com/"))
        self.assertFalse(self.discovery_service._is_pdf_link("https://example.com/file.pdf.txt"))
    
    def test_is_pdf_link_with_pattern(self):
        """Test PDF link detection with file pattern."""
        # Test with pattern
        self.assertTrue(self.discovery_service._is_pdf_link("https://example.com/decision_2024.pdf", "decision_.*"))
        self.assertFalse(self.discovery_service._is_pdf_link("https://example.com/other.pdf", "decision_.*"))
    
    @patch('requests.Session.get')
    def test_discover_pdfs_from_index_page(self, mock_get):
        """Test PDF discovery from index page."""
        # Mock HTML response with PDF links
        html_content = """
        <html>
            <body>
                <a href="document1.pdf">Document 1</a>
                <a href="https://example.com/document2.pdf">Document 2</a>
                <a href="file.txt">Not a PDF</a>
                <a href="subdir/document3.pdf">Document 3</a>
            </body>
        </html>
        """
        
        mock_response = Mock()
        mock_response.content = html_content.encode()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test discovery
        pdf_links = self.discovery_service.discover_pdfs_from_index_page(self.test_source)
        
        # Should find 3 PDF links
        self.assertEqual(len(pdf_links), 3)
        
        # Check that all links have required fields
        for link in pdf_links:
            self.assertIn('url', link)
            self.assertIn('filename', link)
            self.assertIn('url_hash', link)
            self.assertIn('discovered_at', link)
            self.assertTrue(link['url'].endswith('.pdf'))
    
    def tearDown(self):
        """Clean up after tests."""
        self.discovery_service.close()


class TestPDFDownloadManager(unittest.TestCase):
    """Test PDF download manager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.download_manager = PDFDownloadManager(self.temp_dir)
        
        self.test_pdf_info = {
            'url': 'https://example.com/test.pdf',
            'filename': 'test.pdf',
            'url_hash': 'abc123'
        }
    
    def test_download_manager_initialization(self):
        """Test download manager initialization."""
        self.assertTrue(self.download_manager.temp_directory.exists())
        self.assertEqual(self.download_manager.temp_directory, Path(self.temp_dir))
    
    @patch('requests.Session.get')
    def test_download_pdf_success(self, mock_get):
        """Test successful PDF download."""
        # Mock successful response
        mock_response = Mock()
        mock_response.iter_content.return_value = [b'fake pdf content']
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Test download
        result = self.download_manager.download_pdf(self.test_pdf_info)
        
        # Should return path to downloaded file
        self.assertIsNotNone(result)
        self.assertTrue(result.exists())
        self.assertEqual(result.name, 'test.pdf')
        
        # Check file content
        with open(result, 'rb') as f:
            content = f.read()
        self.assertEqual(content, b'fake pdf content')
    
    @patch('requests.Session.get')
    def test_download_pdf_failure(self, mock_get):
        """Test PDF download failure."""
        # Mock failed response
        mock_get.side_effect = Exception("Download failed")
        
        # Test download
        result = self.download_manager.download_pdf(self.test_pdf_info)
        
        # Should return None on failure
        self.assertIsNone(result)
    
    def test_cleanup_temp_files(self):
        """Test temporary file cleanup."""
        # Create a test file
        test_file = self.download_manager.temp_directory / "test.txt"
        test_file.write_text("test content")
        
        # Test cleanup
        cleaned = self.download_manager.cleanup_temp_files()
        
        # Should return 1 (one directory cleaned)
        self.assertEqual(cleaned, 1)
        
        # Directory should be removed
        self.assertFalse(self.download_manager.temp_directory.exists())
    
    def tearDown(self):
        """Clean up after tests."""
        # Ensure temp directory is cleaned up
        if self.download_manager.temp_directory.exists():
            import shutil
            shutil.rmtree(self.download_manager.temp_directory)


class TestPDFProcessingTracker(unittest.TestCase):
    """Test PDF processing tracker functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.vector_store = Mock()
        self.vector_store.es_client = Mock()
        self.tracker = PDFProcessingTracker(self.vector_store)
        
        self.test_pdf_info = {
            'url': 'https://example.com/test.pdf',
            'filename': 'test.pdf',
            'url_hash': 'abc123',
            'discovered_at': '2024-01-01T00:00:00Z'
        }
    
    def test_track_pdf_processing(self):
        """Test PDF processing tracking."""
        # Mock successful tracking
        self.vector_store.es_client.index.return_value = {'result': 'created'}
        
        # Test tracking
        result = self.tracker.track_pdf_processing(
            source_id="test-source",
            pdf_info=self.test_pdf_info,
            status="completed",
            content_hash="hash123",
            vector_store_id="doc123"
        )
        
        # Should return True
        self.assertTrue(result)
        
        # Should call Elasticsearch index method
        self.vector_store.es_client.index.assert_called_once()
        call_args = self.vector_store.es_client.index.call_args
        self.assertEqual(call_args[1]['index'], 'pdf_processing_tracker')
        self.assertEqual(call_args[1]['id'], 'abc123')
    
    def test_is_pdf_processed(self):
        """Test checking if PDF is already processed."""
        # Mock found document
        mock_result = {
            'found': True,
            '_source': {
                'processing_status': 'completed'
            }
        }
        self.vector_store.es_client.get.return_value = mock_result
        
        # Test check
        result = self.tracker.is_pdf_processed('abc123')
        
        # Should return True for completed status
        self.assertTrue(result)
        
        # Mock not found document
        mock_result_not_found = {'found': False}
        self.vector_store.es_client.get.return_value = mock_result_not_found
        
        # Test check
        result = self.tracker.is_pdf_processed('abc123')
        
        # Should return False for not found
        self.assertFalse(result)


class TestPDFDiscoveryProcessor(unittest.TestCase):
    """Test PDF discovery processor integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.cache = Mock()
        self.vector_store = Mock()
        self.openai_client = Mock()
        
        self.processor = PDFDiscoveryProcessor(
            cache=self.cache,
            vector_store=self.vector_store,
            openai_client=self.openai_client
        )
        
        self.test_source = ContentSource(
            id="test-pdf-source",
            name="Test PDF Source",
            type=SourceType.PDF,
            pdf_config=PDFSourceConfig(
                url="https://example.com/pdfs/",
                is_index_page=True,
                file_pattern="*.pdf",
                timeout=30
            )
        )
    
    def test_processor_initialization(self):
        """Test processor initialization."""
        self.assertIsNotNone(self.processor.discovery_service)
        self.assertIsNotNone(self.processor.download_manager)
        self.assertIsNotNone(self.processor.tracker)
    
    @patch('botnim.sync.pdf_discovery.PDFDiscoveryService.discover_pdfs_from_index_page')
    @patch('botnim.sync.pdf_discovery.PDFProcessingTracker.is_pdf_processed')
    def test_process_pdf_source_no_new_pdfs(self, mock_is_processed, mock_discover):
        """Test processing when no new PDFs are found."""
        # Mock no PDFs discovered
        mock_discover.return_value = []
        
        # Test processing
        results = self.processor.process_pdf_source(self.test_source)
        
        # Should return results with zero counts
        self.assertEqual(results['discovered_pdfs'], 0)
        self.assertEqual(results['processed_pdfs'], 0)
        self.assertEqual(results['failed_pdfs'], 0)
        self.assertEqual(len(results['errors']), 0)
    
    def tearDown(self):
        """Clean up after tests."""
        self.processor.cleanup()


if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2) 
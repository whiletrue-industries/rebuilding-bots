"""
Tests for the sync orchestrator functionality.

This test file contains several types of tests:

1. **Unit Tests** (TestSyncResult, TestSyncSummary, TestSyncOrchestrator):
   - Test individual components in isolation
   - Use mocks to isolate the component under test
   - Verify data structures, method calls, and basic functionality

2. **Mocked Integration Tests** (TestIntegration):
   - Test component interactions using mocks
   - Verify that components are properly initialized and connected
   - Test the orchestration flow without external dependencies

3. **Real Integration Tests** (TestRealIntegration):
   - Test actual component interactions with real dependencies
   - Verify data flows between real components
   - Test error handling and edge cases with real implementations
   - These are the most valuable tests as they verify the system works end-to-end

The key difference is:
- Mocked tests verify "does the code call the right methods?"
- Real integration tests verify "does the system actually work?"
"""

import pytest
import asyncio
import tempfile
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, AsyncMock

from ..orchestrator import (
    SyncOrchestrator, SyncOrchestratorCLI, SyncResult, SyncSummary,
    run_sync_orchestration, run_sync_orchestration_sync
)
from ..config import SyncConfig, ContentSource, SourceType, VersioningStrategy, FetchStrategy, HTMLSourceConfig


class TestSyncResult:
    """Test the SyncResult dataclass."""
    
    def test_sync_result_creation(self):
        """Test creating a SyncResult instance."""
        result = SyncResult(
            source_id="test-source",
            source_type="html",
            status="success",
            processing_time=1.5,
            documents_processed=10,
            documents_failed=0,
            error_message=None,
            metadata={"test": "data"}
        )
        
        assert result.source_id == "test-source"
        assert result.source_type == "html"
        assert result.status == "success"
        assert result.processing_time == 1.5
        assert result.documents_processed == 10
        assert result.documents_failed == 0
        assert result.error_message is None
        assert result.metadata == {"test": "data"}


class TestSyncSummary:
    """Test the SyncSummary dataclass."""
    
    def test_sync_summary_creation(self):
        """Test creating a SyncSummary instance."""
        results = [
            SyncResult(
                source_id="source-1",
                source_type="html",
                status="success",
                processing_time=1.0,
                documents_processed=5,
                documents_failed=0
            ),
            SyncResult(
                source_id="source-2",
                source_type="pdf",
                status="failed",
                processing_time=2.0,
                documents_processed=0,
                documents_failed=1,
                error_message="Test error"
            )
        ]
        
        summary = SyncSummary(
            total_sources=2,
            successful_sources=1,
            failed_sources=1,
            skipped_sources=0,
            total_documents_processed=5,
            total_documents_failed=1,
            total_processing_time=3.0,
            embedding_cache_downloaded=True,
            embedding_cache_uploaded=True,
            errors=["Test error"],
            results=results
        )
        
        assert summary.total_sources == 2
        assert summary.successful_sources == 1
        assert summary.failed_sources == 1
        assert summary.skipped_sources == 0
        assert summary.total_documents_processed == 5
        assert summary.total_documents_failed == 1
        assert summary.total_processing_time == 3.0
        assert summary.embedding_cache_downloaded is True
        assert summary.embedding_cache_uploaded is True
        assert len(summary.errors) == 1
        assert len(summary.results) == 2


class TestSyncOrchestrator:
    """Test the SyncOrchestrator class."""
    
    @pytest.fixture
    def sample_config(self):
        """Create a sample sync configuration."""
        from ..config import HTMLSourceConfig, PDFSourceConfig
        
        return SyncConfig(
            name="Test Sync Config",
            sources=[
                ContentSource(
                    id="test-html",
                    name="Test HTML Source",
                    type=SourceType.HTML,
                    html_config=HTMLSourceConfig(
                        url="https://example.com/test.html",
                        selector="#content",
                        encoding="utf-8"
                    ),
                    versioning_strategy=VersioningStrategy.HASH,
                    fetch_strategy=FetchStrategy.DIRECT,
                    enabled=True,
                    priority=1,
                    use_document_parser=False
                ),
                ContentSource(
                    id="test-pdf",
                    name="Test PDF Source",
                    type=SourceType.PDF,
                    pdf_config=PDFSourceConfig(
                        url="https://example.com/test.pdf",
                        is_index_page=False,
                        file_pattern="*.pdf"
                    ),
                    versioning_strategy=VersioningStrategy.HASH,
                    fetch_strategy=FetchStrategy.DIRECT,
                    enabled=True,
                    priority=2,
                    use_document_parser=False
                )
            ]
        )
    
    @pytest.fixture
    def mock_orchestrator(self, sample_config):
        """Create a mock orchestrator with mocked components."""
        with patch('botnim.sync.orchestrator.SyncCache') as mock_cache_class, \
             patch('botnim.sync.orchestrator.VectorStoreES') as mock_vector_store_class, \
             patch('botnim.sync.orchestrator.get_openai_client') as mock_openai_client, \
             patch('botnim.sync.orchestrator.VersionManager') as mock_version_manager_class, \
             patch('botnim.sync.orchestrator.HTMLProcessor') as mock_html_processor_class, \
             patch('botnim.sync.orchestrator.PDFDiscoveryProcessor') as mock_pdf_processor_class, \
             patch('botnim.sync.orchestrator.AsyncSpreadsheetProcessor') as mock_spreadsheet_processor_class, \
             patch('botnim.sync.orchestrator.SyncEmbeddingProcessor') as mock_embedding_processor_class:
            
            # Configure mock return values
            mock_cache = Mock()
            mock_vector_store = Mock()
            mock_version_manager = Mock()
            mock_html_processor = Mock()
            mock_pdf_processor = Mock()
            mock_spreadsheet_processor = Mock()
            mock_embedding_processor = Mock()
            
            mock_cache_class.return_value = mock_cache
            mock_vector_store_class.return_value = mock_vector_store
            mock_openai_client.return_value = Mock()
            mock_version_manager_class.return_value = mock_version_manager
            mock_html_processor_class.return_value = mock_html_processor
            mock_pdf_processor_class.return_value = mock_pdf_processor
            mock_spreadsheet_processor_class.return_value = mock_spreadsheet_processor
            mock_embedding_processor_class.return_value = mock_embedding_processor
            
            orchestrator = SyncOrchestrator(sample_config, "staging")
            
            return orchestrator
    
    def test_orchestrator_initialization(self, sample_config):
        """Test orchestrator initialization."""
        with patch('botnim.sync.orchestrator.SyncCache') as mock_cache_class, \
             patch('botnim.sync.orchestrator.VectorStoreES') as mock_vector_store_class, \
             patch('botnim.sync.orchestrator.get_openai_client') as mock_openai_client, \
             patch('botnim.sync.orchestrator.VersionManager') as mock_version_manager_class, \
             patch('botnim.sync.orchestrator.HTMLProcessor') as mock_html_processor_class, \
             patch('botnim.sync.orchestrator.PDFDiscoveryProcessor') as mock_pdf_processor_class, \
             patch('botnim.sync.orchestrator.AsyncSpreadsheetProcessor') as mock_spreadsheet_processor_class, \
             patch('botnim.sync.orchestrator.SyncEmbeddingProcessor') as mock_embedding_processor_class:
            
            # Configure mock return values
            mock_cache = Mock()
            mock_vector_store = Mock()
            mock_version_manager = Mock()
            mock_html_processor = Mock()
            mock_pdf_processor = Mock()
            mock_spreadsheet_processor = Mock()
            mock_embedding_processor = Mock()
            
            mock_cache_class.return_value = mock_cache
            mock_vector_store_class.return_value = mock_vector_store
            mock_openai_client.return_value = Mock()
            mock_version_manager_class.return_value = mock_version_manager
            mock_html_processor_class.return_value = mock_html_processor
            mock_pdf_processor_class.return_value = mock_pdf_processor
            mock_spreadsheet_processor_class.return_value = mock_spreadsheet_processor
            mock_embedding_processor_class.return_value = mock_embedding_processor
            
            orchestrator = SyncOrchestrator(sample_config, "staging")
            
            assert orchestrator.config == sample_config
            assert orchestrator.environment == "staging"
            assert orchestrator.sync_start_time is None
            assert len(orchestrator.sync_results) == 0
            assert len(orchestrator.sync_errors) == 0
    
    @pytest.mark.asyncio
    async def test_download_embedding_cache_success(self, mock_orchestrator):
        """Test successful embedding cache download."""
        mock_orchestrator.embedding_processor.download_embedding_cache.return_value = {
            'success': True,
            'embeddings_downloaded': 100
        }
        
        result = await mock_orchestrator._download_embedding_cache()
        
        assert result is True
        mock_orchestrator.embedding_processor.download_embedding_cache.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_download_embedding_cache_failure(self, mock_orchestrator):
        """Test failed embedding cache download."""
        mock_orchestrator.embedding_processor.download_embedding_cache.return_value = {
            'success': False,
            'error': 'Download failed'
        }
        
        result = await mock_orchestrator._download_embedding_cache()
        
        assert result is False
        assert len(mock_orchestrator.sync_errors) == 0  # Warning, not error
    
    @pytest.mark.asyncio
    async def test_download_embedding_cache_exception(self, mock_orchestrator):
        """Test embedding cache download with exception."""
        mock_orchestrator.embedding_processor.download_embedding_cache.side_effect = Exception("Test error")
        
        result = await mock_orchestrator._download_embedding_cache()
        
        assert result is False
        assert len(mock_orchestrator.sync_errors) == 1
        assert "Embedding cache download failed" in mock_orchestrator.sync_errors[0]
    
    def test_process_html_source_success(self, mock_orchestrator, sample_config):
        """Test successful HTML source processing."""
        source = sample_config.sources[0]
        start_time = datetime.now().timestamp()
        
        mock_orchestrator.html_processor.process_sources.return_value = {
            'summary': {
                'processed_count': 5,
                'error_count': 0
            }
        }
        
        result = mock_orchestrator._process_html_source(source, start_time)
        
        assert result.source_id == source.id
        assert result.source_type == source.type.value
        assert result.status == "success"
        assert result.documents_processed == 5
        assert result.documents_failed == 0
        assert result.error_message is None
    
    def test_process_html_source_failure(self, mock_orchestrator, sample_config):
        """Test failed HTML source processing."""
        source = sample_config.sources[0]
        start_time = datetime.now().timestamp()
        
        mock_orchestrator.html_processor.process_sources.return_value = {
            'summary': {
                'processed_count': 0,
                'error_count': 2
            }
        }
        
        result = mock_orchestrator._process_html_source(source, start_time)
        
        assert result.status == "failed"
        assert result.documents_processed == 0
        assert result.documents_failed == 2
        assert "errors occurred during processing" in result.error_message
    
    def test_process_pdf_source_success(self, mock_orchestrator, sample_config):
        """Test successful PDF source processing."""
        source = sample_config.sources[1]  # PDF source
        start_time = datetime.now().timestamp()
        
        mock_orchestrator.pdf_processor.process_pdf_source.return_value = {
            'processed_pdfs': 3,
            'failed_pdfs': 0,
            'errors': []
        }
        
        result = mock_orchestrator._process_pdf_source(source, start_time)
        
        assert result.source_id == source.id
        assert result.source_type == source.type.value
        assert result.status == "success"
        assert result.documents_processed == 3
        assert result.documents_failed == 0
        assert result.error_message is None
    
    def test_process_pdf_source_failure(self, mock_orchestrator, sample_config):
        """Test failed PDF source processing."""
        source = sample_config.sources[1]  # PDF source
        start_time = datetime.now().timestamp()
        
        mock_orchestrator.pdf_processor.process_pdf_source.return_value = {
            'processed_pdfs': 0,
            'failed_pdfs': 2,
            'errors': ['Error 1', 'Error 2']
        }
        
        result = mock_orchestrator._process_pdf_source(source, start_time)
        
        assert result.status == "failed"
        assert result.documents_processed == 0
        assert result.documents_failed == 2
        assert "PDFs failed" in result.error_message
    
    @pytest.mark.asyncio
    async def test_process_embeddings(self, mock_orchestrator):
        """Test embedding processing."""
        # Mock cache entries
        mock_entry = Mock()
        mock_entry.processed = True
        mock_entry.error_message = None
        mock_entry.source_id = "test-source"
        mock_entry.metadata = {
            'parsed_content': {
                'text_content': 'Test content'
            },
            'version_info': {'hash': 'test-hash'}
        }
        
        mock_orchestrator.cache.get_all_cached_content.return_value = [mock_entry]
        mock_orchestrator.embedding_processor.process_sync_content.return_value = {
            'processed_documents': 1,
            'total_documents': 1
        }
        
        await mock_orchestrator._process_embeddings()
        
        mock_orchestrator.embedding_processor.process_sync_content.assert_called_once()
        call_args = mock_orchestrator.embedding_processor.process_sync_content.call_args[0][0]
        assert len(call_args) == 1
        assert call_args[0]['source_id'] == "test-source"
        assert call_args[0]['content'] == "Test content"
    
    @pytest.mark.asyncio
    async def test_upload_embedding_cache_success(self, mock_orchestrator):
        """Test successful embedding cache upload."""
        mock_orchestrator.embedding_processor.upload_embedding_cache.return_value = {
            'success': True,
            'embeddings_uploaded': 50
        }
        
        result = await mock_orchestrator._upload_embedding_cache()
        
        assert result is True
        mock_orchestrator.embedding_processor.upload_embedding_cache.assert_called_once()
    
    def test_generate_summary(self, mock_orchestrator):
        """Test summary generation."""
        # Add some test results
        mock_orchestrator.sync_results = [
            SyncResult(
                source_id="source-1",
                source_type="html",
                status="success",
                processing_time=1.0,
                documents_processed=5,
                documents_failed=0
            ),
            SyncResult(
                source_id="source-2",
                source_type="pdf",
                status="failed",
                processing_time=2.0,
                documents_processed=0,
                documents_failed=1,
                error_message="Test error"
            )
        ]
        
        mock_orchestrator.sync_start_time = datetime.now().timestamp() - 3.0
        
        summary = mock_orchestrator._generate_summary(True, True)
        
        assert summary.total_sources == 2
        assert summary.successful_sources == 1
        assert summary.failed_sources == 1
        assert summary.skipped_sources == 0
        assert summary.total_documents_processed == 5
        assert summary.total_documents_failed == 1
        assert summary.embedding_cache_downloaded is True
        assert summary.embedding_cache_uploaded is True
        assert len(summary.results) == 2
    
    def test_get_sync_statistics(self, mock_orchestrator):
        """Test getting sync statistics."""
        mock_orchestrator.cache.get_cache_statistics.return_value = {
            'total_sources': 10,
            'processed_sources': 8
        }
        mock_orchestrator.embedding_processor.get_embedding_statistics.return_value = {
            'total_embeddings': 1000,
            'storage_size_mb': 50.5
        }
        mock_orchestrator.version_manager.versions = {'source1': {}, 'source2': {}}
        
        stats = mock_orchestrator.get_sync_statistics()
        
        assert stats['environment'] == "staging"
        assert stats['config_name'] == "Test Sync Config"
        assert stats['enabled_sources'] == 2
        assert stats['total_sources'] == 2
        assert stats['version_count'] == 2
        assert 'cache_statistics' in stats
        assert 'embedding_statistics' in stats
    
    def test_cleanup(self, mock_orchestrator):
        """Test orchestrator cleanup."""
        # Mock cleanup methods
        mock_orchestrator.html_processor.close = Mock()
        mock_orchestrator.pdf_processor.cleanup = Mock()
        mock_orchestrator.spreadsheet_processor.shutdown = Mock()
        
        mock_orchestrator.cleanup()
        
        mock_orchestrator.html_processor.close.assert_called_once()
        mock_orchestrator.pdf_processor.cleanup.assert_called_once()
        mock_orchestrator.spreadsheet_processor.shutdown.assert_called_once()


@pytest.fixture
def temp_config_file():
    """Create a temporary config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        config_data = {
            'version': '1.0.0',
            'name': 'Test Config',
            'sources': [
                {
                    'id': 'test-source',
                    'name': 'Test Source',
                    'type': 'html',
                    'html_config': {
                        'url': 'https://example.com',
                        'selector': 'body'
                    },
                    'enabled': True,
                    'priority': 1
                }
            ]
        }
        import yaml
        yaml.dump(config_data, f)
        yield f.name
    
    # Cleanup
    Path(f.name).unlink(missing_ok=True)


class TestSyncOrchestratorCLI:
    """Test the SyncOrchestratorCLI class."""
    
    @pytest.fixture
    def cli(self):
        """Create a CLI instance."""
        return SyncOrchestratorCLI()
    
    def test_run_sync_from_config(self, cli, temp_config_file):
        """Test running sync from config file."""
        with patch('botnim.sync.orchestrator.SyncOrchestrator') as mock_orchestrator_class:
            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            
            # Mock the async run_sync method
            mock_summary = SyncSummary(
                total_sources=1,
                successful_sources=1,
                failed_sources=0,
                skipped_sources=0,
                total_documents_processed=5,
                total_documents_failed=0,
                total_processing_time=2.0,
                embedding_cache_downloaded=True,
                embedding_cache_uploaded=True,
                errors=[],
                results=[]
            )
            
            mock_orchestrator.run_sync = AsyncMock(return_value=mock_summary)
            mock_orchestrator.cleanup = Mock()
            
            summary = cli.run_sync_from_config(temp_config_file, "staging")
            
            assert summary == mock_summary
            mock_orchestrator_class.assert_called_once()
            mock_orchestrator.run_sync.assert_called_once()
            mock_orchestrator.cleanup.assert_called_once()
    
    def test_print_summary(self, cli):
        """Test printing sync summary."""
        summary = SyncSummary(
            total_sources=2,
            successful_sources=1,
            failed_sources=1,
            skipped_sources=0,
            total_documents_processed=5,
            total_documents_failed=1,
            total_processing_time=3.0,
            embedding_cache_downloaded=True,
            embedding_cache_uploaded=False,
            errors=["Test error"],
            results=[
                SyncResult(
                    source_id="source-1",
                    source_type="html",
                    status="success",
                    processing_time=1.0,
                    documents_processed=5,
                    documents_failed=0
                ),
                SyncResult(
                    source_id="source-2",
                    source_type="pdf",
                    status="failed",
                    processing_time=2.0,
                    documents_processed=0,
                    documents_failed=1,
                    error_message="Test error"
                )
            ]
        )
        
        # This should not raise any exceptions
        cli.print_summary(summary)


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    @pytest.mark.asyncio
    async def test_run_sync_orchestration(self, temp_config_file):
        """Test async convenience function."""
        with patch('botnim.sync.orchestrator.SyncOrchestratorCLI') as mock_cli_class:
            mock_cli = Mock()
            mock_cli_class.return_value = mock_cli
            
            mock_summary = SyncSummary(
                total_sources=1,
                successful_sources=1,
                failed_sources=0,
                skipped_sources=0,
                total_documents_processed=5,
                total_documents_failed=0,
                total_processing_time=2.0,
                embedding_cache_downloaded=True,
                embedding_cache_uploaded=True,
                errors=[],
                results=[]
            )
            
            mock_cli.run_sync_from_config.return_value = mock_summary
            
            result = await run_sync_orchestration(temp_config_file, "staging")
            
            assert result == mock_summary
            mock_cli.run_sync_from_config.assert_called_once_with(temp_config_file, "staging")
    
    def test_run_sync_orchestration_sync(self, temp_config_file):
        """Test synchronous convenience function."""
        with patch('botnim.sync.orchestrator.run_sync_orchestration') as mock_async_func:
            mock_summary = SyncSummary(
                total_sources=1,
                successful_sources=1,
                failed_sources=0,
                skipped_sources=0,
                total_documents_processed=5,
                total_documents_failed=0,
                total_processing_time=2.0,
                embedding_cache_downloaded=True,
                embedding_cache_uploaded=True,
                errors=[],
                results=[]
            )
            
            mock_async_func.return_value = mock_summary
            
            result = run_sync_orchestration_sync(temp_config_file, "staging")
            
            assert result == mock_summary
            mock_async_func.assert_called_once_with(temp_config_file, "staging")


class TestIntegration:
    """Integration tests for the orchestrator."""
    
    def test_orchestrator_integration(self, temp_config_file):
        """Test orchestrator integration with mocked components."""
        with patch('botnim.sync.orchestrator.SyncCache') as mock_cache_class, \
             patch('botnim.sync.orchestrator.VectorStoreES') as mock_vector_store_class, \
             patch('botnim.sync.orchestrator.get_openai_client') as mock_openai_client, \
             patch('botnim.sync.orchestrator.VersionManager') as mock_version_manager_class, \
             patch('botnim.sync.orchestrator.HTMLProcessor') as mock_html_processor_class, \
             patch('botnim.sync.orchestrator.PDFDiscoveryProcessor') as mock_pdf_processor_class, \
             patch('botnim.sync.orchestrator.AsyncSpreadsheetProcessor') as mock_spreadsheet_processor_class, \
             patch('botnim.sync.orchestrator.SyncEmbeddingProcessor') as mock_embedding_processor_class:
            
            # Configure mock return values
            mock_cache = Mock()
            mock_vector_store = Mock()
            mock_version_manager = Mock()
            mock_html_processor = Mock()
            mock_pdf_processor = Mock()
            mock_spreadsheet_processor = Mock()
            mock_embedding_processor = Mock()
            
            mock_cache_class.return_value = mock_cache
            mock_vector_store_class.return_value = mock_vector_store
            mock_openai_client.return_value = Mock()
            mock_version_manager_class.return_value = mock_version_manager
            mock_html_processor_class.return_value = mock_html_processor
            mock_pdf_processor_class.return_value = mock_pdf_processor
            mock_spreadsheet_processor_class.return_value = mock_spreadsheet_processor
            mock_embedding_processor_class.return_value = mock_embedding_processor
            
            # Load config
            config = SyncConfig.from_yaml(temp_config_file)
            
            # Create orchestrator
            orchestrator = SyncOrchestrator(config, "staging")
            
            # Verify components are initialized
            assert orchestrator.config == config
            assert orchestrator.environment == "staging"
            assert orchestrator.cache is not None
            assert orchestrator.vector_store is not None
            assert orchestrator.openai_client is not None
            assert orchestrator.version_manager is not None
            assert orchestrator.html_processor is not None
            assert orchestrator.pdf_processor is not None
            assert orchestrator.spreadsheet_processor is not None
            assert orchestrator.embedding_processor is not None
            
            # Test cleanup
            orchestrator.cleanup()
    
    @pytest.mark.asyncio
    async def test_full_sync_workflow_mock(self, temp_config_file):
        """Test the full sync workflow with mocked components."""
        with patch('botnim.sync.orchestrator.SyncCache') as mock_cache_class, \
             patch('botnim.sync.orchestrator.VectorStoreES') as mock_vector_store_class, \
             patch('botnim.sync.orchestrator.get_openai_client') as mock_openai_client, \
             patch('botnim.sync.orchestrator.VersionManager') as mock_version_manager_class, \
             patch('botnim.sync.orchestrator.HTMLProcessor') as mock_html_processor_class, \
             patch('botnim.sync.orchestrator.PDFDiscoveryProcessor') as mock_pdf_processor_class, \
             patch('botnim.sync.orchestrator.AsyncSpreadsheetProcessor') as mock_spreadsheet_processor_class, \
             patch('botnim.sync.orchestrator.SyncEmbeddingProcessor') as mock_embedding_processor_class:
            
            # Configure mock return values
            mock_cache = Mock()
            mock_vector_store = Mock()
            mock_version_manager = Mock()
            mock_html_processor = Mock()
            mock_pdf_processor = Mock()
            mock_spreadsheet_processor = Mock()
            mock_embedding_processor = Mock()
            
            mock_cache_class.return_value = mock_cache
            mock_vector_store_class.return_value = mock_vector_store
            mock_openai_client.return_value = Mock()
            mock_version_manager_class.return_value = mock_version_manager
            mock_html_processor_class.return_value = mock_html_processor
            mock_pdf_processor_class.return_value = mock_pdf_processor
            mock_spreadsheet_processor_class.return_value = mock_spreadsheet_processor
            mock_embedding_processor_class.return_value = mock_embedding_processor
            
            # Load config
            config = SyncConfig.from_yaml(temp_config_file)
            
            # Create orchestrator
            orchestrator = SyncOrchestrator(config, "staging")
            
            # Mock all the async methods
            orchestrator._download_embedding_cache = AsyncMock(return_value=True)
            orchestrator._process_all_sources = AsyncMock()
            orchestrator._process_embeddings = AsyncMock()
            orchestrator._upload_embedding_cache = AsyncMock(return_value=True)
            
            # Mock cache entries for embedding processing
            mock_entry = Mock()
            mock_entry.processed = True
            mock_entry.error_message = None
            mock_entry.source_id = "test-source"
            mock_entry.metadata = {
                'parsed_content': {
                    'text_content': 'Test content'
                }
            }
            orchestrator.cache.get_all_cached_content.return_value = [mock_entry]
            
            # Run sync
            summary = await orchestrator.run_sync()
            
            # Verify all steps were called
            orchestrator._download_embedding_cache.assert_called_once()
            orchestrator._process_all_sources.assert_called_once()
            orchestrator._process_embeddings.assert_called_once()
            orchestrator._upload_embedding_cache.assert_called_once()
            
            # Verify summary
            assert summary is not None
            assert summary.embedding_cache_downloaded is True
            assert summary.embedding_cache_uploaded is True
            
            # Cleanup
            orchestrator.cleanup()


class TestRealIntegration:
    """Real integration tests that test actual component interactions."""
    
    @pytest.fixture
    def real_orchestrator(self, tmp_path):
        """Create a real orchestrator with actual components."""
        config_data = {
            'version': '1.0.0',
            'name': 'Real Integration Test',
            'cache_directory': str(tmp_path / 'cache'),
            'embedding_cache_path': str(tmp_path / 'embeddings.json'),
            'version_cache_path': str(tmp_path / 'versions.json'),
            'max_concurrent_sources': 2,
            'sources': [
                {
                    'id': 'test-local-html',
                    'name': 'Test Local HTML',
                    'type': 'html',
                    'html_config': {
                        'url': 'file://botnim/sync/tests/test_index.html',
                        'selector': 'body',
                        'encoding': 'utf-8'
                    },
                    'enabled': True,
                    'priority': 1,
                    'use_document_parser': False
                }
            ]
        }
        
        config = SyncConfig(**config_data)
        return SyncOrchestrator(config, "local")
    
    def test_real_component_initialization(self, real_orchestrator):
        """Test that real components are properly initialized and connected."""
        # Test that cache is properly initialized
        assert real_orchestrator.cache is not None
        assert hasattr(real_orchestrator.cache, 'content_cache_path')
        assert hasattr(real_orchestrator.cache, 'duplicate_cache_path')
        
        # Test that vector store is properly initialized
        assert real_orchestrator.vector_store is not None
        assert hasattr(real_orchestrator.vector_store, 'es_client')
        
        # Test that processors are properly initialized
        assert real_orchestrator.html_processor is not None
        assert real_orchestrator.pdf_processor is not None
        assert real_orchestrator.spreadsheet_processor is not None
        assert real_orchestrator.embedding_processor is not None
        
        # Test that processors have access to shared components
        assert real_orchestrator.html_processor.cache == real_orchestrator.cache
        assert real_orchestrator.pdf_processor.cache == real_orchestrator.cache
        assert real_orchestrator.pdf_processor.vector_store == real_orchestrator.vector_store
        assert real_orchestrator.embedding_processor.sync_cache == real_orchestrator.cache
    
    def test_real_cache_operations(self, real_orchestrator):
        """Test that cache operations work with real components."""
        # Test cache initialization
        stats = real_orchestrator.cache.get_cache_statistics()
        assert 'total_sources' in stats
        assert 'processed_sources' in stats
        
        # Test cache entry operations
        test_entry = {
            'source_id': 'test-source',
            'content_hash': 'test-hash',
            'content_size': 100,
            'timestamp': datetime.now(),
            'metadata': {'test': 'data'},
            'processed': True,
            'error_message': None
        }
        
        # Add entry to cache
        real_orchestrator.cache.cache_content(
            source_id=test_entry['source_id'],
            content_hash=test_entry['content_hash'],
            content_size=test_entry['content_size'],
            metadata=test_entry['metadata'],
            processed=test_entry['processed'],
            error_message=test_entry['error_message']
        )
        
        # Verify entry was added
        entries = real_orchestrator.cache.get_all_cached_content()
        assert len(entries) == 1
        assert entries[0].source_id == 'test-source'
        assert entries[0].content_hash == 'test-hash'
    
    def test_real_config_loading(self, temp_config_file):
        """Test that real config loading works with actual files."""
        config = SyncConfig.from_yaml(temp_config_file)
        
        assert config.name == 'Test Config'
        assert len(config.sources) == 1
        assert config.sources[0].id == 'test-source'
        assert config.sources[0].type == SourceType.HTML
    
    @pytest.mark.asyncio
    async def test_real_embedding_cache_operations(self, real_orchestrator):
        """Test real embedding cache download/upload operations."""
        # Test download (should work even if no cache exists)
        download_result = await real_orchestrator._download_embedding_cache()
        # Should not fail, even if no cache exists
        assert isinstance(download_result, bool)
        
        # Test upload (should work even if no cache to upload)
        upload_result = await real_orchestrator._upload_embedding_cache()
        # Should not fail, even if no cache to upload
        assert isinstance(upload_result, bool)
    
    def test_real_error_handling(self, real_orchestrator):
        """Test that real error handling works properly."""
        # Test with invalid source (missing required config)
        try:
            invalid_source = ContentSource(
                id="invalid-source",
                name="Invalid Source",
                type=SourceType.HTML,
                html_config=None,  # This should cause a validation error
                enabled=True,
                priority=1
            )
            # If we get here, the validation didn't work as expected
            assert False, "Expected validation error for missing html_config"
        except Exception as e:
            # This is expected - Pydantic should catch this
            assert "html_config" in str(e) or "HTML source requires" in str(e)
        
        # Test with a source that has config but points to non-existent URL
        invalid_source = ContentSource(
            id="invalid-source",
            name="Invalid Source",
            type=SourceType.HTML,
            html_config=HTMLSourceConfig(
                url="https://this-url-does-not-exist-12345.com",
                selector="body",
                encoding="utf-8"
            ),
            enabled=True,
            priority=1
        )
        
        start_time = datetime.now().timestamp()
        result = real_orchestrator._process_html_source(invalid_source, start_time)
        
        # Should handle the error gracefully
        assert result.status == "failed"
        assert result.error_message is not None
        assert result.documents_processed == 0
        assert result.documents_failed == 1  # One document failed to fetch
    
    def test_real_data_flow(self, real_orchestrator):
        """Test that data flows correctly between real components."""
        # Test with a simple HTTP URL that should work
        test_source = ContentSource(
            id="test-http-source",
            name="Test HTTP Source",
            type=SourceType.HTML,
            html_config=HTMLSourceConfig(
                url="https://httpbin.org/html",  # Use a reliable test URL
                selector="body",
                encoding="utf-8"
            ),
            enabled=True,
            priority=1,
            use_document_parser=False
        )
        
        # Process the source
        start_time = datetime.now().timestamp()
        result = real_orchestrator._process_html_source(test_source, start_time)
        
        # Verify the result
        assert result.source_id == "test-http-source"
        assert result.source_type == "html"
        assert result.status == "success"
        assert result.documents_processed >= 0  # May be 0 if no content extracted
        assert result.processing_time > 0 
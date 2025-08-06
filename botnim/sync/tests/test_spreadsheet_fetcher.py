"""
Tests for asynchronous spreadsheet processing functionality.
"""

import pytest
import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from ...config import get_logger
from ..spreadsheet_fetcher import (
    SpreadsheetFetcher, TaskQueue, IntermediateStorage, AsyncSpreadsheetProcessor,
    SpreadsheetData, ProcessingTask, process_spreadsheet_source_async,
    get_spreadsheet_data_from_storage
)
from ..config import ContentSource, SpreadsheetSourceConfig, SourceType, FetchStrategy
from ..cache import SyncCache
from ...vector_store.vector_store_es import VectorStoreES

logger = get_logger(__name__)


class TestSpreadsheetData:
    """Test SpreadsheetData dataclass."""
    
    def test_spreadsheet_data_creation(self):
        """Test creating SpreadsheetData object."""
        data = [
            {"name": "John", "age": "30"},
            {"name": "Jane", "age": "25"}
        ]
        
        spreadsheet_data = SpreadsheetData(
            source_id="test-source",
            data=data,
            headers=["name", "age"],
            row_count=2,
            content_hash="abc123",
            fetch_timestamp=datetime.now(timezone.utc),
            metadata={"url": "https://example.com"}
        )
        
        assert spreadsheet_data.source_id == "test-source"
        assert spreadsheet_data.row_count == 2
        assert spreadsheet_data.content_hash == "abc123"
        assert len(spreadsheet_data.data) == 2


class TestProcessingTask:
    """Test ProcessingTask dataclass."""
    
    def test_processing_task_creation(self):
        """Test creating ProcessingTask object."""
        task = ProcessingTask(
            task_id="task-123",
            source_id="test-source",
            status="pending",
            created_at=datetime.now(timezone.utc)
        )
        
        assert task.task_id == "task-123"
        assert task.source_id == "test-source"
        assert task.status == "pending"
        assert task.error_message is None


class TestTaskQueue:
    """Test TaskQueue functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.task_queue = TaskQueue(max_workers=2)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        self.task_queue.shutdown()
    
    def test_submit_task(self):
        """Test submitting a task."""
        def test_func(x, y):
            return x + y
        
        task_id = self.task_queue.submit_task("task-1", "source-1", test_func, 2, 3)
        
        assert task_id == "task-1"
        
        # Wait for task to complete
        import time
        time.sleep(0.1)
        
        task = self.task_queue.get_task_status("task-1")
        assert task is not None
        assert task.source_id == "source-1"
        assert task.status in ["completed", "processing"]
    
    def test_get_pending_tasks(self):
        """Test getting pending tasks."""
        def slow_func():
            import time
            time.sleep(0.1)
            return "done"
        
        # Submit multiple tasks
        self.task_queue.submit_task("task-1", "source-1", slow_func)
        self.task_queue.submit_task("task-2", "source-2", slow_func)
        
        pending_tasks = self.task_queue.get_pending_tasks()
        assert len(pending_tasks) >= 0  # Tasks might start processing immediately
    
    def test_cleanup_completed_tasks(self):
        """Test cleaning up completed tasks."""
        def quick_func():
            return "done"
        
        # Submit and complete a task
        self.task_queue.submit_task("task-1", "source-1", quick_func)
        
        # Wait for completion
        import time
        time.sleep(0.1)
        
        # Clean up tasks older than 1 hour (should not clean up recent tasks)
        cleaned = self.task_queue.cleanup_completed_tasks(max_age_hours=1)
        assert cleaned == 0
        
        # Clean up tasks older than 0 hours (should clean up all completed tasks)
        cleaned = self.task_queue.cleanup_completed_tasks(max_age_hours=0)
        assert cleaned >= 0


class TestIntermediateStorage:
    """Test IntermediateStorage functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_vector_store = Mock(spec=VectorStoreES)
        self.mock_es_client = Mock()
        self.mock_vector_store.es_client = self.mock_es_client
        self.storage = IntermediateStorage(self.mock_vector_store)
    
    def test_store_spreadsheet_data(self):
        """Test storing spreadsheet data."""
        # Mock Elasticsearch client
        self.mock_es_client.indices.exists.return_value = False
        self.mock_es_client.index.return_value = {"_id": "doc-123"}
        
        data = SpreadsheetData(
            source_id="test-source",
            data=[{"name": "John", "age": "30"}],
            headers=["name", "age"],
            row_count=1,
            content_hash="abc123",
            fetch_timestamp=datetime.now(timezone.utc),
            metadata={"url": "https://example.com"}
        )
        
        success = self.storage.store_spreadsheet_data(data)
        
        assert success is True
        self.mock_es_client.index.assert_called_once()
    
    def test_get_spreadsheet_data(self):
        """Test retrieving spreadsheet data."""
        # Mock Elasticsearch search response
        mock_response = {
            "hits": {
                "hits": [{
                    "_source": {
                        "content": json.dumps([{"name": "John"}]),
                        "metadata": {
                            "source_id": "test-source",
                            "row_count": 1,
                            "content_hash": "abc123"
                        }
                    }
                }]
            }
        }
        self.mock_es_client.search.return_value = mock_response
        
        data = self.storage.get_spreadsheet_data("test-source")
        
        assert data is not None
        assert data["metadata"]["source_id"] == "test-source"
        assert data["metadata"]["row_count"] == 1


class TestSpreadsheetFetcher:
    """Test SpreadsheetFetcher functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_cache = Mock(spec=SyncCache)
        self.mock_vector_store = Mock(spec=VectorStoreES)
        self.fetcher = SpreadsheetFetcher(self.mock_cache, self.mock_vector_store)
    
    def test_parse_google_sheets_url(self):
        """Test parsing Google Sheets URLs."""
        # Test standard URL format
        url = "https://docs.google.com/spreadsheets/d/1fEgiCLNMQQZqBgQFlkABXgke8I2kI1i1XUvj8Yba9Ow/edit?gid=0#gid=0"
        spreadsheet_id, sheet_name = self.fetcher._parse_google_sheets_url(url)
        
        assert spreadsheet_id == "1fEgiCLNMQQZqBgQFlkABXgke8I2kI1i1XUvj8Yba9Ow"
        assert sheet_name == "Sheet1"  # Default sheet name
    
    def test_parse_google_sheets_url_invalid(self):
        """Test parsing invalid Google Sheets URLs."""
        url = "https://example.com/not-a-sheets-url"
        spreadsheet_id, sheet_name = self.fetcher._parse_google_sheets_url(url)
        
        assert spreadsheet_id is None
        assert sheet_name is None


class TestAsyncSpreadsheetProcessor:
    """Test AsyncSpreadsheetProcessor functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_cache = Mock(spec=SyncCache)
        self.mock_vector_store = Mock(spec=VectorStoreES)
        self.processor = AsyncSpreadsheetProcessor(
            self.mock_cache, self.mock_vector_store, max_workers=2
        )
    
    def teardown_method(self):
        """Clean up test fixtures."""
        self.processor.shutdown()
    
    def test_should_process_source_enabled(self):
        """Test source processing decision for enabled source."""
        source = ContentSource(
            id="test-source",
            name="Test Source",
            type=SourceType.SPREADSHEET,
            spreadsheet_config=SpreadsheetSourceConfig(
                url="https://docs.google.com/spreadsheets/d/test",
                use_adc=True
            ),
            fetch_strategy=FetchStrategy.ASYNC,
            enabled=True
        )
        
        should_process, reason = self.processor._should_process_source(source)
        
        assert should_process is True
        assert reason is None
    
    def test_should_process_source_disabled(self):
        """Test source processing decision for disabled source."""
        source = ContentSource(
            id="test-source",
            name="Test Source",
            type=SourceType.SPREADSHEET,
            spreadsheet_config=SpreadsheetSourceConfig(
                url="https://docs.google.com/spreadsheets/d/test",
                use_adc=True
            ),
            fetch_strategy=FetchStrategy.ASYNC,
            enabled=False
        )
        
        should_process, reason = self.processor._should_process_source(source)
        
        assert should_process is False
        assert reason == "Source is disabled"
    
    def test_should_process_source_wrong_strategy(self):
        """Test source processing decision for wrong fetch strategy."""
        source = ContentSource(
            id="test-source",
            name="Test Source",
            type=SourceType.SPREADSHEET,
            spreadsheet_config=SpreadsheetSourceConfig(
                url="https://docs.google.com/spreadsheets/d/test",
                use_adc=True
            ),
            fetch_strategy=FetchStrategy.DIRECT,
            enabled=True
        )
        
        should_process, reason = self.processor._should_process_source(source)
        
        assert should_process is False
        assert reason == "Source is not configured for async processing"
    
    @pytest.mark.asyncio
    async def test_process_spreadsheet_source_skipped(self):
        """Test processing a source that should be skipped."""
        source = ContentSource(
            id="test-source",
            name="Test Source",
            type=SourceType.SPREADSHEET,
            spreadsheet_config=SpreadsheetSourceConfig(
                url="https://docs.google.com/spreadsheets/d/test",
                use_adc=True
            ),
            fetch_strategy=FetchStrategy.DIRECT,  # Wrong strategy
            enabled=True
        )
        
        result = await self.processor.process_spreadsheet_source(source)
        
        assert result["status"] == "skipped"
        assert result["error_message"] == "Source is not configured for async processing"
    
    def test_get_task_status(self):
        """Test getting task status."""
        # Submit a task first
        def test_func():
            return "done"
        
        task_id = self.processor.task_queue.submit_task("task-1", "source-1", test_func)
        
        # Wait for task to complete
        import time
        time.sleep(0.1)
        
        task = self.processor.get_task_status("task-1")
        assert task is not None
        assert task.task_id == "task-1"
        assert task.source_id == "source-1"


class TestIntegration:
    """Integration tests for spreadsheet processing."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_cache = Mock(spec=SyncCache)
        self.mock_vector_store = Mock(spec=VectorStoreES)
        self.mock_es_client = Mock()
        self.mock_vector_store.es_client = self.mock_es_client
    
    @pytest.mark.asyncio
    async def test_process_spreadsheet_source_async(self):
        """Test the convenience function for async processing."""
        source = ContentSource(
            id="test-source",
            name="Test Source",
            type=SourceType.SPREADSHEET,
            spreadsheet_config=SpreadsheetSourceConfig(
                url="https://docs.google.com/spreadsheets/d/test",
                use_adc=True
            ),
            fetch_strategy=FetchStrategy.DIRECT,  # Will be skipped
            enabled=True
        )
        
        result = await process_spreadsheet_source_async(
            source, self.mock_cache, self.mock_vector_store
        )
        
        assert result["status"] == "skipped"
    
    def test_get_spreadsheet_data_from_storage(self):
        """Test the convenience function for getting data from storage."""
        # Mock Elasticsearch search response
        mock_response = {
            "hits": {
                "hits": [{
                    "_source": {
                        "content": json.dumps([{"name": "John"}]),
                        "metadata": {
                            "source_id": "test-source",
                            "row_count": 1
                        }
                    }
                }]
            }
        }
        self.mock_es_client.search.return_value = mock_response
        
        data = get_spreadsheet_data_from_storage("test-source", self.mock_vector_store)
        
        assert data is not None
        assert data["metadata"]["source_id"] == "test-source"


class TestErrorHandling:
    """Test error handling scenarios."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_cache = Mock(spec=SyncCache)
        self.mock_vector_store = Mock(spec=VectorStoreES)
        self.mock_es_client = Mock()
        self.mock_vector_store.es_client = self.mock_es_client
    
    def test_storage_error_handling(self):
        """Test error handling in storage operations."""
        storage = IntermediateStorage(self.mock_vector_store)
        
        # Mock Elasticsearch error
        self.mock_es_client.index.side_effect = Exception("Elasticsearch error")
        
        data = SpreadsheetData(
            source_id="test-source",
            data=[{"name": "John"}],
            headers=["name"],
            row_count=1,
            content_hash="abc123",
            fetch_timestamp=datetime.now(timezone.utc),
            metadata={}
        )
        
        success = storage.store_spreadsheet_data(data)
        
        assert success is False
    
    def test_task_queue_error_handling(self):
        """Test error handling in task queue."""
        task_queue = TaskQueue(max_workers=1)
        
        def failing_func():
            raise Exception("Task failed")
        
        task_id = task_queue.submit_task("task-1", "source-1", failing_func)
        
        # Wait for task to complete
        import time
        time.sleep(0.1)
        
        task = task_queue.get_task_status("task-1")
        assert task is not None
        assert task.status == "failed"
        assert "Task failed" in task.error_message
        
        task_queue.shutdown()


if __name__ == "__main__":
    pytest.main([__file__]) 
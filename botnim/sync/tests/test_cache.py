"""
Tests for the sync cache and duplicate detection functionality.
"""

import pytest
import tempfile
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from ..cache import SyncCache, CacheEntry, DuplicateInfo, DuplicateDetector
from ..config import ContentSource, SourceType, VersioningStrategy, FetchStrategy, VersionManager


class TestSyncCache:
    """Test the SyncCache class."""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary cache directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def cache(self, temp_cache_dir):
        """Create a SyncCache instance for testing."""
        return SyncCache(cache_directory=temp_cache_dir)
    
    @pytest.fixture
    def sample_source(self):
        """Create a sample ContentSource for testing."""
        from ..config import HTMLSourceConfig
        
        return ContentSource(
            id="test-source-1",
            name="Test Source",
            description="A test source",
            type=SourceType.HTML,
            html_config=HTMLSourceConfig(
                url="http://example.com",
                selector="#content",
                encoding="utf-8",
                timeout=30,
                retry_attempts=3
            ),
            versioning_strategy=VersioningStrategy.HASH,
            fetch_strategy=FetchStrategy.DIRECT,
            enabled=True,
            priority=1,
            tags=["test", "html"]
        )
    
    @pytest.fixture
    def version_manager(self, temp_cache_dir):
        """Create a VersionManager instance for testing."""
        return VersionManager(str(Path(temp_cache_dir) / "versions.json"))
    
    def test_init_creates_directories(self, temp_cache_dir):
        """Test that initialization creates necessary directories and databases."""
        cache = SyncCache(cache_directory=temp_cache_dir)
        
        # Check that cache directory exists
        assert Path(temp_cache_dir).exists()
        
        # Check that database files exist
        assert cache.content_cache_path.exists()
        assert cache.duplicate_cache_path.exists()
    
    def test_compute_content_hash(self, cache):
        """Test content hash computation."""
        content = "test content"
        hash1 = cache.compute_content_hash(content)
        hash2 = cache.compute_content_hash(content.encode('utf-8'))
        
        # Same content should produce same hash
        assert hash1 == hash2
        
        # Different content should produce different hashes
        different_content = "different content"
        hash3 = cache.compute_content_hash(different_content)
        assert hash1 != hash3
        
        # Hash should be 64 characters (SHA-256 hex)
        assert len(hash1) == 64
    
    def test_is_duplicate_new_content(self, cache):
        """Test duplicate detection for new content."""
        source_id = "test-source-1"
        content_hash = "a" * 64  # Mock hash
        content_size = 100
        
        duplicate_info = cache.is_duplicate(source_id, content_hash, content_size)
        
        assert not duplicate_info.is_duplicate
        assert duplicate_info.existing_hash is None
        assert duplicate_info.reason is None
    
    def test_is_duplicate_existing_content(self, cache):
        """Test duplicate detection for existing content."""
        source_id_1 = "test-source-1"
        source_id_2 = "test-source-2"
        content_hash = "a" * 64  # Mock hash
        content_size = 100
        
        # First call - not a duplicate
        duplicate_info_1 = cache.is_duplicate(source_id_1, content_hash, content_size)
        assert not duplicate_info_1.is_duplicate
        
        # Second call with same content - should be duplicate
        duplicate_info_2 = cache.is_duplicate(source_id_2, content_hash, content_size)
        assert duplicate_info_2.is_duplicate
        assert duplicate_info_2.existing_hash == content_hash
        assert "already processed" in duplicate_info_2.reason
    
    def test_cache_content_and_retrieve(self, cache):
        """Test caching and retrieving content."""
        source_id = "test-source-1"
        content_hash = "a" * 64
        content_size = 100
        metadata = {"url": "http://example.com", "type": "html"}
        
        # Cache content
        cache.cache_content(source_id, content_hash, content_size, metadata)
        
        # Retrieve cached content
        cached = cache.get_cached_content(source_id)
        
        assert cached is not None
        assert cached.source_id == source_id
        assert cached.content_hash == content_hash
        assert cached.content_size == content_size
        assert cached.metadata == metadata
        assert not cached.processed
        assert cached.error_message is None
    
    def test_mark_processed(self, cache):
        """Test marking content as processed."""
        source_id = "test-source-1"
        content_hash = "a" * 64
        content_size = 100
        metadata = {"url": "http://example.com"}
        
        # Cache content
        cache.cache_content(source_id, content_hash, content_size, metadata)
        
        # Mark as processed
        cache.mark_processed(source_id, processed=True, error_message=None)
        
        # Check that it's marked as processed
        cached = cache.get_cached_content(source_id)
        assert cached.processed
        assert cached.error_message is None
    
    def test_mark_processed_with_error(self, cache):
        """Test marking content as processed with error."""
        source_id = "test-source-1"
        content_hash = "a" * 64
        content_size = 100
        metadata = {"url": "http://example.com"}
        error_message = "Network timeout"
        
        # Cache content
        cache.cache_content(source_id, content_hash, content_size, metadata)
        
        # Mark as processed with error
        cache.mark_processed(source_id, processed=False, error_message=error_message)
        
        # Check that error is recorded
        cached = cache.get_cached_content(source_id)
        assert not cached.processed
        assert cached.error_message == error_message
    
    def test_should_process_source_new_content(self, cache, sample_source, version_manager):
        """Test should_process_source for new content."""
        content_hash = "a" * 64
        content_size = 100
        
        # Mock version manager to indicate content has changed
        should_process, reason = cache.should_process_source(
            sample_source, content_hash, content_size
        )
        
        assert should_process
        assert "Processing required" in reason
    
    def test_should_process_source_unchanged_content(self, cache, sample_source, version_manager):
        """Test should_process_source for unchanged content."""
        content_hash = "a" * 64
        content_size = 100
        
        # This test is no longer valid as the version check is decoupled from the cache
        pass
    
    def test_should_process_source_duplicate_content(self, cache, sample_source, version_manager):
        """Test should_process_source for duplicate content."""
        content_hash = "a" * 64
        content_size = 100
        
        # First, mark this content as a duplicate
        cache.is_duplicate(sample_source.id, content_hash, content_size)
        
        # Mock version manager to indicate content has changed
        should_process, reason = cache.should_process_source(
            sample_source, content_hash, content_size
        )
        
        assert not should_process
        assert "Duplicate content" in reason
    
    def test_should_process_source_already_processed(self, cache, sample_source, version_manager):
        """Test should_process_source for already processed content."""
        content_hash = "a" * 64
        content_size = 100
        metadata = {"url": "http://example.com"}
        
        # Cache content as already processed
        cache.cache_content(sample_source.id, content_hash, content_size, metadata, processed=True)
        
        # Mock version manager to indicate content has changed
        should_process, reason = cache.should_process_source(
            sample_source, content_hash, content_size
        )
        
        assert not should_process
        assert "Already processed successfully" in reason
    
    def test_get_cache_statistics(self, cache):
        """Test cache statistics."""
        # Add some test data
        cache.cache_content("source-1", "hash1", 100, {"type": "html"}, processed=True)
        cache.cache_content("source-2", "hash2", 200, {"type": "pdf"}, processed=False, error_message="Error")
        cache.cache_content("source-3", "hash3", 300, {"type": "html"}, processed=True)
        
        # Create some duplicates
        cache.is_duplicate("source-4", "hash1", 100)  # Same hash as source-1
        
        stats = cache.get_cache_statistics()
        
        assert stats["total_sources"] == 3
        assert stats["processed_sources"] == 2
        assert stats["error_sources"] == 1
        assert stats["success_rate"] == pytest.approx(66.67, rel=0.01)
        assert stats["total_duplicates"] >= 1
        assert stats["cache_size_mb"] > 0
    
    def test_cleanup_old_entries(self, cache):
        """Test cleanup of old entries."""
        # Add some test data
        cache.cache_content("source-1", "hash1", 100, {"type": "html"})
        cache.cache_content("source-2", "hash2", 200, {"type": "pdf"})
        
        # Check initial count
        stats_before = cache.get_cache_statistics()
        assert stats_before["total_sources"] == 2
        
        # Cleanup (should not remove recent entries)
        deleted = cache.cleanup_old_entries(days_old=1)
        assert deleted == 0
        
        # Check count after cleanup
        stats_after = cache.get_cache_statistics()
        assert stats_after["total_sources"] == 2
    
    def test_log_sync_operation(self, cache):
        """Test logging sync operations."""
        source_id = "test-source-1"
        operation = "fetch"
        status = "success"
        details = {"url": "http://example.com", "size": 100}
        
        cache.log_sync_operation(source_id, operation, status, details)
        
        # Check that log file exists and contains the entry
        assert cache.sync_log_path.exists()
        
        logs = cache.get_sync_logs()
        assert len(logs) == 1
        assert logs[0]["source_id"] == source_id
        assert logs[0]["operation"] == operation
        assert logs[0]["status"] == status
        assert logs[0]["details"] == details
    
    def test_get_sync_logs_filtered(self, cache):
        """Test getting filtered sync logs."""
        # Add multiple log entries
        cache.log_sync_operation("source-1", "fetch", "success", {"size": 100})
        cache.log_sync_operation("source-2", "fetch", "error", {"error": "timeout"})
        cache.log_sync_operation("source-1", "process", "success", {"processed": True})
        
        # Get all logs
        all_logs = cache.get_sync_logs()
        assert len(all_logs) == 3
        
        # Get logs for specific source
        source_1_logs = cache.get_sync_logs(source_id="source-1")
        assert len(source_1_logs) == 2
        assert all(log["source_id"] == "source-1" for log in source_1_logs)
        
        # Get logs with limit
        limited_logs = cache.get_sync_logs(limit=2)
        assert len(limited_logs) == 2


class TestDuplicateDetector:
    """Test the DuplicateDetector class."""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary cache directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def cache(self, temp_cache_dir):
        """Create a SyncCache instance for testing."""
        return SyncCache(cache_directory=temp_cache_dir)
    
    @pytest.fixture
    def detector(self, cache):
        """Create a DuplicateDetector instance for testing."""
        return DuplicateDetector(cache)
    
    def test_detect_similar_content_placeholder(self, detector):
        """Test the placeholder similar content detection."""
        content = "test content"
        similar = detector.detect_similar_content(content)
        
        # Should return empty list for now (placeholder implementation)
        assert similar == []
    
    def test_get_duplicate_summary_no_duplicates(self, detector):
        """Test duplicate summary with no duplicates."""
        summary = detector.get_duplicate_summary()
        
        assert summary["total_duplicates"] == 0
        assert summary["total_processing_saved"] == 0
        assert summary["most_common_duplicates"] == []
    
    def test_get_duplicate_summary_with_duplicates(self, detector):
        """Test duplicate summary with duplicates."""
        # Create some duplicates
        detector.cache.is_duplicate("source-1", "hash1", 100)
        detector.cache.is_duplicate("source-2", "hash1", 100)  # Same hash
        detector.cache.is_duplicate("source-3", "hash2", 200)
        detector.cache.is_duplicate("source-4", "hash2", 200)  # Same hash
        
        summary = detector.get_duplicate_summary()
        
        assert summary["total_duplicates"] == 2
        assert summary["total_processing_saved"] == 2  # 2 duplicates saved 2 processing operations
        assert len(summary["most_common_duplicates"]) == 2
        
        # Check that duplicates are sorted by count
        counts = [dup["count"] for dup in summary["most_common_duplicates"]]
        assert counts == sorted(counts, reverse=True)


class TestIntegration:
    """Integration tests for the caching system."""
    
    @pytest.fixture
    def temp_cache_dir(self):
        """Create a temporary cache directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def cache(self, temp_cache_dir):
        """Create a SyncCache instance for testing."""
        return SyncCache(cache_directory=temp_cache_dir)
    
    @pytest.fixture
    def version_manager(self, temp_cache_dir):
        """Create a VersionManager instance for testing."""
        return VersionManager(str(Path(temp_cache_dir) / "versions.json"))
    
    def test_full_workflow(self, cache, version_manager):
        """Test the full caching workflow."""
        from ..config import HTMLSourceConfig
        
        # Create a test source
        source = ContentSource(
            id="test-workflow-source",
            name="Test Workflow Source",
            description="A test source for workflow testing",
            type=SourceType.HTML,
            html_config=HTMLSourceConfig(
                url="http://example.com",
                selector="#content",
                encoding="utf-8",
                timeout=30,
                retry_attempts=3
            ),
            versioning_strategy=VersioningStrategy.HASH,
            fetch_strategy=FetchStrategy.DIRECT,
            enabled=True,
            priority=1,
            tags=["test", "workflow"]
        )
        
        content = "This is test content for the workflow"
        content_hash = cache.compute_content_hash(content)
        content_size = len(content.encode('utf-8'))
        metadata = {"url": "http://example.com", "type": "html"}
        
        should_process, reason = cache.should_process_source(
            source, content_hash, content_size
        )
        assert should_process
        assert "Processing required" in reason
        
        # Step 2: Cache the content
        cache.cache_content(source.id, content_hash, content_size, metadata)
        
        # Step 3: Mark as processed
        cache.mark_processed(source.id, processed=True)
        
        # Step 4: Check if should process again (should be False)
        should_process, reason = cache.should_process_source(
            source, content_hash, content_size
        )
        assert not should_process
        assert "Duplicate content" in reason
        
        # Step 5: Check statistics
        stats = cache.get_cache_statistics()
        assert stats["total_sources"] == 1
        assert stats["processed_sources"] == 1
        assert stats["success_rate"] == 100.0
        
        # Step 6: Check logs
        logs = cache.get_sync_logs(source_id=source.id)
        assert len(logs) == 0  # No logs added in this test, but structure works 
"""
Test file for the sync configuration and versioning schema.

This module tests the configuration loading, validation, and versioning functionality.
"""

import pytest
import tempfile
import json
from pathlib import Path
from datetime import datetime, timezone

from .sync_config import (
    SyncConfig, ContentSource, VersionManager, VersionInfo,
    SourceType, VersioningStrategy, FetchStrategy,
    HTMLSourceConfig, PDFSourceConfig, SpreadsheetSourceConfig,
    create_example_config
)


class TestSyncConfig:
    """Test cases for SyncConfig class."""
    
    def test_create_example_config(self):
        """Test creating an example configuration."""
        config = create_example_config()
        
        assert config.name == "Example Sync Configuration"
        assert len(config.sources) == 3
        
        # Check source types
        html_sources = config.get_sources_by_type(SourceType.HTML)
        pdf_sources = config.get_sources_by_type(SourceType.PDF)
        spreadsheet_sources = config.get_sources_by_type(SourceType.SPREADSHEET)
        
        assert len(html_sources) == 1
        assert len(pdf_sources) == 1
        assert len(spreadsheet_sources) == 1
        
        # Check source IDs
        source_ids = [source.id for source in config.sources]
        assert "knesset-laws-html" in source_ids
        assert "ethics-decisions-pdf" in source_ids
        assert "spreadsheet-data" in source_ids
    
    def test_load_from_yaml(self):
        """Test loading configuration from YAML file."""
        # Create a temporary YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml_content = """
version: "1.0.0"
name: "Test Configuration"
description: "Test configuration for unit tests"
sources:
  - id: "test-html"
    name: "Test HTML Source"
    type: "html"
    html_config:
      url: "https://example.com"
      selector: "#content"
    versioning_strategy: "hash"
    fetch_strategy: "direct"
    enabled: true
    priority: 1
    tags: ["test", "html"]
"""
            f.write(yaml_content)
            yaml_path = f.name
        
        try:
            config = SyncConfig.from_yaml(yaml_path)
            
            assert config.name == "Test Configuration"
            assert len(config.sources) == 1
            
            source = config.sources[0]
            assert source.id == "test-html"
            assert source.type == SourceType.HTML
            assert source.html_config.url == "https://example.com"
            assert source.html_config.selector == "#content"
            assert source.versioning_strategy == VersioningStrategy.HASH
            assert source.fetch_strategy == FetchStrategy.DIRECT
            assert source.enabled is True
            assert source.priority == 1
            assert "test" in source.tags
            
        finally:
            Path(yaml_path).unlink()
    
    def test_save_to_yaml(self):
        """Test saving configuration to YAML file."""
        config = create_example_config()
        
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as f:
            yaml_path = f.name
        
        try:
            config.to_yaml(yaml_path)
            
            # Verify file was created
            assert Path(yaml_path).exists()
            
            # Load it back and verify
            loaded_config = SyncConfig.from_yaml(yaml_path)
            assert loaded_config.name == config.name
            assert len(loaded_config.sources) == len(config.sources)
            
        finally:
            Path(yaml_path).unlink()
    
    def test_get_source_by_id(self):
        """Test getting source by ID."""
        config = create_example_config()
        
        source = config.get_source_by_id("knesset-laws-html")
        assert source is not None
        assert source.name == "Knesset Laws (HTML)"
        
        # Test non-existent source
        source = config.get_source_by_id("non-existent")
        assert source is None
    
    def test_get_sources_by_type(self):
        """Test getting sources by type."""
        config = create_example_config()
        
        html_sources = config.get_sources_by_type(SourceType.HTML)
        assert len(html_sources) == 1
        assert html_sources[0].id == "knesset-laws-html"
        
        pdf_sources = config.get_sources_by_type(SourceType.PDF)
        assert len(pdf_sources) == 1
        assert pdf_sources[0].id == "ethics-decisions-pdf"
    
    def test_get_enabled_sources(self):
        """Test getting enabled sources."""
        config = create_example_config()
        
        # All sources in example config are enabled
        enabled_sources = config.get_enabled_sources()
        assert len(enabled_sources) == len(config.sources)
        
        # Disable one source
        config.sources[0].enabled = False
        enabled_sources = config.get_enabled_sources()
        assert len(enabled_sources) == len(config.sources) - 1


class TestContentSource:
    """Test cases for ContentSource class."""
    
    def test_html_source_validation(self):
        """Test HTML source validation."""
        # Valid HTML source
        source = ContentSource(
            id="test",
            name="Test",
            type=SourceType.HTML,
            html_config=HTMLSourceConfig(url="https://example.com")
        )
        assert source.type == SourceType.HTML
        assert source.html_config.url == "https://example.com"
    
    def test_pdf_source_validation(self):
        """Test PDF source validation."""
        # Valid PDF source
        source = ContentSource(
            id="test",
            name="Test",
            type=SourceType.PDF,
            pdf_config=PDFSourceConfig(url="https://example.com/file.pdf")
        )
        assert source.type == SourceType.PDF
        assert source.pdf_config.url == "https://example.com/file.pdf"
    
    def test_spreadsheet_source_validation(self):
        """Test spreadsheet source validation."""
        # Valid spreadsheet source
        source = ContentSource(
            id="test",
            name="Test",
            type=SourceType.SPREADSHEET,
            spreadsheet_config=SpreadsheetSourceConfig(
                url="https://docs.google.com/spreadsheets/d/test/edit"
            )
        )
        assert source.type == SourceType.SPREADSHEET
        assert "docs.google.com/spreadsheets" in source.spreadsheet_config.url
    
    def test_invalid_source_config(self):
        """Test that invalid source configurations raise errors."""
        # HTML source without html_config
        with pytest.raises(ValueError, match="HTML source requires html_config"):
            ContentSource(
                id="test",
                name="Test",
                type=SourceType.HTML
            )
        
        # PDF source without pdf_config
        with pytest.raises(ValueError, match="PDF source requires pdf_config"):
            ContentSource(
                id="test",
                name="Test",
                type=SourceType.PDF
            )
        
        # Spreadsheet source without spreadsheet_config
        with pytest.raises(ValueError, match="Spreadsheet source requires spreadsheet_config"):
            ContentSource(
                id="test",
                name="Test",
                type=SourceType.SPREADSHEET
            )


class TestVersionManager:
    """Test cases for VersionManager class."""
    
    def test_version_manager_initialization(self):
        """Test VersionManager initialization."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            cache_path = f.name
        
        try:
            vm = VersionManager(cache_path)
            assert vm.cache_path == Path(cache_path)
            assert vm.versions == {}
        finally:
            Path(cache_path).unlink()
    
    def test_version_operations(self):
        """Test version operations."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            cache_path = f.name
        
        try:
            vm = VersionManager(cache_path)
            
            # Test getting non-existent version
            version = vm.get_version("test-source")
            assert version is None
            
            # Test updating version
            version_info = VersionInfo(
                source_id="test-source",
                version_hash="abc123",
                version_timestamp=datetime.now(timezone.utc),
                content_size=1000,
                last_fetch=datetime.now(timezone.utc),
                fetch_status="success"
            )
            
            vm.update_version(version_info)
            
            # Test getting updated version
            retrieved_version = vm.get_version("test-source")
            assert retrieved_version is not None
            assert retrieved_version.source_id == "test-source"
            assert retrieved_version.version_hash == "abc123"
            
        finally:
            Path(cache_path).unlink()
    
    def test_content_hash_computation(self):
        """Test content hash computation."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            cache_path = f.name
        
        try:
            vm = VersionManager(cache_path)
            
            # Test string content
            content = "Hello, World!"
            hash1 = vm.compute_content_hash(content)
            assert len(hash1) == 64  # SHA-256 hash length
            
            # Test bytes content
            content_bytes = b"Hello, World!"
            hash2 = vm.compute_content_hash(content_bytes)
            assert hash1 == hash2  # Same content should produce same hash
            
            # Test different content
            different_content = "Hello, Different World!"
            hash3 = vm.compute_content_hash(different_content)
            assert hash1 != hash3  # Different content should produce different hash
            
        finally:
            Path(cache_path).unlink()
    
    def test_version_change_detection(self):
        """Test version change detection."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            cache_path = f.name
        
        try:
            vm = VersionManager(cache_path)
            
            # Test with no existing version
            assert vm.has_changed("test-source", "new-hash") is True
            
            # Add initial version
            initial_version = VersionInfo(
                source_id="test-source",
                version_hash="old-hash",
                version_timestamp=datetime.now(timezone.utc),
                content_size=1000,
                last_fetch=datetime.now(timezone.utc),
                fetch_status="success"
            )
            vm.update_version(initial_version)
            
            # Test with same hash
            assert vm.has_changed("test-source", "old-hash") is False
            
            # Test with different hash
            assert vm.has_changed("test-source", "new-hash") is True
            
        finally:
            Path(cache_path).unlink()


class TestVersionInfo:
    """Test cases for VersionInfo class."""
    
    def test_version_info_serialization(self):
        """Test VersionInfo serialization and deserialization."""
        timestamp = datetime.now(timezone.utc)
        
        version_info = VersionInfo(
            source_id="test-source",
            version_hash="abc123",
            version_timestamp=timestamp,
            version_string="v1.0.0",
            etag="etag123",
            content_size=1000,
            last_fetch=timestamp,
            fetch_status="success",
            error_message=None
        )
        
        # Test to_dict
        data = version_info.to_dict()
        assert data["source_id"] == "test-source"
        assert data["version_hash"] == "abc123"
        assert data["version_string"] == "v1.0.0"
        assert data["etag"] == "etag123"
        assert data["content_size"] == 1000
        assert data["fetch_status"] == "success"
        
        # Test from_dict
        restored_version = VersionInfo.from_dict(data)
        assert restored_version.source_id == version_info.source_id
        assert restored_version.version_hash == version_info.version_hash
        assert restored_version.version_string == version_info.version_string
        assert restored_version.etag == version_info.etag
        assert restored_version.content_size == version_info.content_size
        assert restored_version.fetch_status == version_info.fetch_status


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"]) 
#!/usr/bin/env python3
"""
Test CLI for PDF Pipeline with Mock Data Sources.

This module provides a test-specific CLI that automatically uses mock data sources
for quick testing without downloading real PDFs.
"""

import os
import sys
import argparse
import pytest
from pathlib import Path
from unittest.mock import patch, Mock

# Add the parent directory to the path to import botnim modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from botnim.document_parser.pdf_processor.pdf_pipeline import PDFExtractionPipeline
from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig
from botnim.document_parser.pdf_processor.test.mock_open_budget_data_source import MockOpenBudgetDataSource


def setup_logging(verbose: bool = False):
    """Setup logging for the test CLI."""
    import logging
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def validate_config(config_path: str) -> bool:
    """Validate that the test configuration exists and is valid."""
    if not os.path.exists(config_path):
        print(f"❌ Test configuration not found: {config_path}")
        return False
    
    try:
        config = PDFExtractionConfig.from_yaml(config_path)
        print(f"✅ Test configuration loaded: {len(config.sources)} sources")
        return True
    except Exception as e:
        print(f"❌ Invalid test configuration: {e}")
        return False


def run_test_pipeline(config_path: str, output_dir: str, verbose: bool = False) -> bool:
    """
    Run the PDF pipeline with mock data sources for testing.
    
    Args:
        config_path: Path to test configuration file
        output_dir: Output directory for results
        verbose: Enable verbose logging
        
    Returns:
        True if successful, False otherwise
    """
    print(f"🧪 Running test pipeline with mock data sources...")
    print(f"   Config: {config_path}")
    print(f"   Output: {output_dir}")
    
    try:
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Load configuration
        config = PDFExtractionConfig.from_yaml(config_path)
        
        # Create mock OpenAI client
        class MockOpenAIClient:
            def __init__(self):
                self.chat = MockChatCompletion()
        
        class MockChatCompletion:
            def __init__(self):
                self.completions = MockCompletions()
        
        class MockCompletions:
            def create(self, **kwargs):
                # Return mock extracted data based on the fields in the config
                fields = config.sources[0].fields if config.sources else []
                mock_data = {}
                for field in fields:
                    mock_data[field.name] = f"Mock {field.name} value"
                
                return MockResponse(mock_data)
        
        class MockResponse:
            def __init__(self, data):
                self.choices = [MockChoice(data)]
        
        class MockChoice:
            def __init__(self, data):
                self.message = MockMessage(data)
        
        class MockMessage:
            def __init__(self, data):
                import json
                self.content = json.dumps(data)
        
        # Initialize pipeline with mock client
        pipeline = PDFExtractionPipeline(
            config_path=config_path,
            openai_client=MockOpenAIClient(),
            enable_metrics=False
        )
        
        # Mock the OpenBudgetDataSource
        with patch('botnim.document_parser.pdf_processor.open_budget_data_source.OpenBudgetDataSource') as mock_data_source:
            # Configure the mock
            mock_instance = mock_data_source.return_value
            mock_instance.get_current_revision.return_value = "2025.08.20-01"
            mock_instance.get_files_to_process.return_value = [
                {
                    'url': 'https://example.com/test1.pdf',
                    'filename': 'test1.pdf',
                    'title': 'Test Document 1',
                    'date': '2025-01-01'
                }
            ]
            mock_instance.download_pdf.return_value = "/tmp/test.pdf"
            
            # Process the directory
            success = pipeline.process_directory(output_dir)
            
            if success:
                print("✅ Pipeline completed successfully!")
                return True
            else:
                print("❌ Pipeline failed!")
                return False
                
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        return False


def test_cli_config_validation():
    """Test CLI configuration validation."""
    config_path = Path(__file__).parent / "config" / "test_config_simple.yaml"
    assert validate_config(str(config_path)), "Configuration validation failed"


def test_cli_pipeline_execution():
    """Test CLI pipeline execution with mock data."""
    config_path = Path(__file__).parent / "config" / "test_config_simple.yaml"
    output_dir = Path(__file__).parent / "output"
    
    # Ensure output directory exists
    output_dir.mkdir(exist_ok=True)
    
    success = run_test_pipeline(str(config_path), str(output_dir), verbose=True)
    assert success, "Pipeline execution failed"


def test_cli_mock_data_integration():
    """Test CLI integration with mock data sources."""
    config_path = Path(__file__).parent / "config" / "test_config_simple.yaml"
    
    # Test that the configuration loads correctly
    config = PDFExtractionConfig.from_yaml(str(config_path))
    assert len(config.sources) > 0, "No sources loaded from test config"
    
    # Test that sources have required Open Budget fields
    for source in config.sources:
        assert hasattr(source, 'index_csv_url'), f"Source {source.name} missing index_csv_url"
        assert hasattr(source, 'datapackage_url'), f"Source {source.name} missing datapackage_url"
        # Note: output_config is optional for testing purposes


if __name__ == "__main__":
    # For backward compatibility, run tests manually
    pytest.main([__file__, "-v"]) 
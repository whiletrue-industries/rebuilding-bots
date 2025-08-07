"""
Tests for the PDF-to-Spreadsheet pre-processing pipeline.
"""

import pytest
import asyncio
import tempfile
import csv
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

from botnim.sync.orchestrator import SyncOrchestrator
from botnim.sync.config import SyncConfig

# A sample PDF pipeline configuration for testing
PIPELINE_CONFIG_YAML = """
version: '1.0'
name: 'Test PDF Pipeline Config'
sources:
  - id: 'pdf-pipeline-test'
    name: 'Test PDF to Sheet Pipeline'
    type: 'pdf_pipeline'
    enabled: true
    pdf_pipeline_config:
      input_config:
        url: 'https://example.com/pdf_index'
        is_index_page: true
        file_pattern: '.*\\.pdf'
      processing_config:
        model: 'gpt-4o-mini'
        max_tokens: 4000
        temperature: 0.1
        fields:
          - name: 'title'
            type: 'string'
            description: 'The title of the document'
          - name: 'author'
            type: 'string'
            description: 'The author of the document'
      output_config:
        spreadsheet_id: 'test-spreadsheet-id'
        sheet_name: 'test-sheet'
        use_adc: true
"""

@pytest.fixture
def mock_dependencies():
    """Mocks all major dependencies for the orchestrator."""
    with patch('botnim.sync.orchestrator.SyncCache') as mock_cache, \
         patch('botnim.sync.orchestrator.VectorStoreES') as mock_vector_store, \
         patch('botnim.sync.orchestrator.get_openai_client') as mock_openai_client, \
         patch('botnim.sync.pdf_pipeline_processor.PDFDiscoveryService') as mock_discovery, \
         patch('botnim.sync.pdf_pipeline_processor.PDFDownloadManager') as mock_download, \
         patch('botnim.sync.pdf_pipeline_processor.PDFExtractionPipeline') as mock_extraction_pipeline, \
                      patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService') as mock_g_service:
        
        # Configure mock discovery service
        mock_discovery_instance = mock_discovery.return_value
        mock_discovery_instance.discover_pdfs_from_index_page.return_value = [
            {'url': 'https://example.com/doc1.pdf', 'filename': 'doc1.pdf', 'url_hash': 'hash1'},
            {'url': 'https://example.com/doc2.pdf', 'filename': 'doc2.pdf', 'url_hash': 'hash2'},
        ]

        # Configure mock download manager
        mock_download_instance = mock_download.return_value
        # Create dummy temp files
        temp_dir = tempfile.gettempdir()
        pdf1_path = Path(temp_dir) / 'doc1.pdf'
        pdf2_path = Path(temp_dir) / 'doc2.pdf'
        pdf1_path.touch()
        pdf2_path.touch()
        mock_download_instance.download_pdf.side_effect = [pdf1_path, pdf2_path]

        # Configure mock extraction pipeline
        mock_extraction_instance = mock_extraction_pipeline.return_value
        mock_extraction_instance.process_single_pdf.side_effect = [
            ({'title': 'Title 1', 'author': 'Author 1'}, {}),
            ({'title': 'Title 2', 'author': 'Author 2'}, {}),
        ]
        
        # Configure mock Google Sheets service
        mock_g_service_instance = mock_g_service.return_value
        mock_g_service_instance.upload_csv_to_sheet.return_value = True

        yield {
            "cache": mock_cache,
            "vector_store": mock_vector_store,
            "openai_client": mock_openai_client,
            "discovery": mock_discovery_instance,
            "download": mock_download_instance,
            "extraction_pipeline": mock_extraction_instance,
            "g_service": mock_g_service_instance,
        }

@pytest.mark.asyncio
async def test_pdf_pipeline_e2e(tmp_path, mock_dependencies):
    """
    Tests the full end-to-end flow of the PDF-to-Spreadsheet pipeline.
    """
    # 1. Setup: Create a temporary config file
    config_path = tmp_path / "sync_config.yaml"
    config_path.write_text(PIPELINE_CONFIG_YAML)

    # 2. Execution: Run the sync orchestrator
    config = SyncConfig.from_yaml(config_path)
    orchestrator = SyncOrchestrator(config, environment="test")
    
    # We only want to test the pre-processing step
    await orchestrator._run_preprocessing_pipelines()

    # 3. Assertions
    
    # Assert Discovery was called correctly
    mock_dependencies["discovery"].discover_pdfs_from_index_page.assert_called_once()
    
    # Assert Download was called for each discovered PDF
    assert mock_dependencies["download"].download_pdf.call_count == 2
    
    # Assert PDF processing was called for each downloaded PDF
    assert mock_dependencies["extraction_pipeline"].process_single_pdf.call_count == 2
    
    # Assert Google Sheets upload was called once with the correct parameters
    mock_dependencies["g_service"].upload_csv_to_sheet.assert_called_once()
    upload_args = mock_dependencies["g_service"].upload_csv_to_sheet.call_args
    
    # Check the contents of the CSV that was to be uploaded
    csv_path = upload_args.kwargs['csv_path']
    with open(csv_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 2
        assert rows[0]['title'] == 'Title 1'
        assert rows[1]['author'] == 'Author 2'

    assert upload_args.kwargs['spreadsheet_id'] == 'test-spreadsheet-id'
    assert upload_args.kwargs['sheet_name'] == 'test-sheet'
    assert upload_args.kwargs['replace_existing'] is True
    
    # Check that no errors were logged in the orchestrator for this pipeline
    assert len(orchestrator.error_tracker.get_errors()) == 0


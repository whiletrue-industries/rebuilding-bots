"""
Integration tests for PDF processing scenarios.

This module tests all three PDF processing scenarios:
1. Single PDF → Single Decision → Single Row
2. Single PDF → Multiple Decisions → Multiple Rows  
3. Multiple PDFs → Multiple Decisions → Multiple Rows
"""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

from botnim.sync.config import SyncConfig, SourceType
from botnim.sync.orchestrator import SyncOrchestrator
from botnim.sync.pdf_pipeline_processor import PDFPipelineProcessor
from botnim.sync.cache import SyncCache


# Test configuration for the three scenarios
SINGLE_PDF_SINGLE_DECISION_CONFIG = """
version: "1.0.0"
name: "Single PDF Single Decision Test"
sources:
  - id: "single-pdf-single-decision"
    name: "Single PDF Single Decision"
    type: "pdf_pipeline"
    enabled: true
    pdf_pipeline_config:
      input_config:
        url: "https://example.com/single-decision.pdf"
        is_index_page: false
      output_config:
        spreadsheet_id: "test-spreadsheet-id"
        sheet_name: "Single Decision"
        use_adc: true
      processing_config:
        model: "gpt-4o-mini"
        fields:
          - name: "decision_title"
            type: "string"
            description: "Title of the decision"
          - name: "decision_date"
            type: "date"
            description: "Date of the decision"
        extraction_instructions: "Extract a single decision from this document."
"""

SINGLE_PDF_MULTIPLE_DECISIONS_CONFIG = """
version: "1.0.0"
name: "Single PDF Multiple Decisions Test"
sources:
  - id: "single-pdf-multiple-decisions"
    name: "Single PDF Multiple Decisions"
    type: "pdf_pipeline"
    enabled: true
    pdf_pipeline_config:
      input_config:
        url: "https://example.com/multiple-decisions.pdf"
        is_index_page: false
      output_config:
        spreadsheet_id: "test-spreadsheet-id"
        sheet_name: "Multiple Decisions"
        use_adc: true
      processing_config:
        model: "gpt-4o-mini"
        fields:
          - name: "decision_number"
            type: "string"
            description: "Number of the decision"
          - name: "decision_title"
            type: "string"
            description: "Title of the decision"
          - name: "decision_date"
            type: "date"
            description: "Date of the decision"
        extraction_instructions: |
          This document contains multiple decisions. Extract each decision separately.
          If you find multiple decisions (numbered 1, 2, 3, etc.), return an array of objects.
          Each object should represent one decision with all relevant fields.
"""

MULTIPLE_PDFS_CONFIG = """
version: "1.0.0"
name: "Multiple PDFs Test"
sources:
  - id: "multiple-pdfs"
    name: "Multiple PDFs"
    type: "pdf_pipeline"
    enabled: true
    pdf_pipeline_config:
      input_config:
        url: "https://example.com/decisions-index"
        is_index_page: true
        file_pattern: "*.pdf"
      output_config:
        spreadsheet_id: "test-spreadsheet-id"
        sheet_name: "Multiple PDFs"
        use_adc: true
      processing_config:
        model: "gpt-4o-mini"
        fields:
          - name: "decision_number"
            type: "string"
            description: "Number of the decision"
          - name: "decision_title"
            type: "string"
            description: "Title of the decision"
          - name: "decision_date"
            type: "date"
            description: "Date of the decision"
        extraction_instructions: |
          Extract decision information from this document.
          If the document contains multiple decisions, extract each one separately.
          Return an array of objects, each representing one decision.
"""


class TestPDFScenarios:
    """Test class for PDF processing scenarios."""

    @pytest.fixture
    def mock_openai_client(self):
        """Mock OpenAI client for testing."""
        client = Mock()
        # Mock response for single decision
        single_decision_response = Mock()
        single_decision_response.choices = [Mock()]
        single_decision_response.choices[0].message.content = json.dumps({
            "decision_title": "Test Decision",
            "decision_date": "2024-01-15"
        })
        
        # Mock response for multiple decisions
        multiple_decisions_response = Mock()
        multiple_decisions_response.choices = [Mock()]
        multiple_decisions_response.choices[0].message.content = json.dumps([
            {
                "decision_number": "1",
                "decision_title": "First Decision",
                "decision_date": "2024-01-15"
            },
            {
                "decision_number": "2", 
                "decision_title": "Second Decision",
                "decision_date": "2024-01-16"
            }
        ])
        
        client.chat.completions.create.side_effect = [
            single_decision_response,
            multiple_decisions_response,
            multiple_decisions_response
        ]
        return client

    @pytest.fixture
    def mock_cache(self):
        """Mock cache for testing."""
        return Mock(spec=SyncCache)

    def test_scenario_1_single_pdf_single_decision(self, mock_openai_client, mock_cache):
        """Test Scenario 1: Single PDF → Single Decision → Single Row."""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(SINGLE_PDF_SINGLE_DECISION_CONFIG)
            config_path = f.name
        
        try:
            # Load configuration
            config = SyncConfig.from_yaml(config_path)
            source = config.get_source_by_id("single-pdf-single-decision")
            
            # Mock the PDF discovery and download
            with patch('botnim.sync.pdf_pipeline_processor.PDFDiscoveryService') as mock_discovery, \
                 patch('botnim.sync.pdf_pipeline_processor.PDFDownloadManager') as mock_download, \
                 patch('botnim.sync.pdf_pipeline_processor.PDFExtractionPipeline') as mock_pipeline, \
                 patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService') as mock_g_service:
                
                # Mock discovery - single PDF
                mock_discovery_instance = Mock()
                mock_discovery_instance.discover_pdfs_from_index_page.return_value = [
                    {'url': 'https://example.com/single-decision.pdf', 'filename': 'single-decision.pdf'}
                ]
                mock_discovery.return_value = mock_discovery_instance
                
                # Mock download
                mock_download_instance = Mock()
                mock_download_instance.download_pdf.return_value = Path('/tmp/test.pdf')
                mock_download.return_value = mock_download_instance
                
                # Mock pipeline - single decision
                mock_pipeline_instance = Mock()
                mock_pipeline_instance.process_single_pdf.return_value = (
                    [{"decision_title": "Test Decision", "decision_date": "2024-01-15"}], 
                    None
                )
                mock_pipeline.return_value = mock_pipeline_instance
                
                # Mock Google Sheets service
                mock_g_service_instance = Mock()
                mock_g_service_instance.upload_csv_to_sheet.return_value = True
                mock_g_service.return_value = mock_g_service_instance
                
                # Create processor and run
                processor = PDFPipelineProcessor(mock_cache, mock_openai_client)
                result = processor.process_pipeline_source(source)
                
                # Assertions
                assert result["status"] == "completed"
                assert result["discovered_pdfs"] == 1
                assert result["processed_pdfs"] == 1
                assert result["failed_pdfs"] == 0
                
                # Verify pipeline was called once (single PDF)
                mock_pipeline_instance.process_single_pdf.assert_called_once()
                
                # Verify Google Sheets upload was called
                mock_g_service_instance.upload_csv_to_sheet.assert_called_once()
                
        finally:
            Path(config_path).unlink()

    def test_scenario_2_single_pdf_multiple_decisions(self, mock_openai_client, mock_cache):
        """Test Scenario 2: Single PDF → Multiple Decisions → Multiple Rows."""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(SINGLE_PDF_MULTIPLE_DECISIONS_CONFIG)
            config_path = f.name
        
        try:
            # Load configuration
            config = SyncConfig.from_yaml(config_path)
            source = config.get_source_by_id("single-pdf-multiple-decisions")
            
            # Mock the PDF discovery and download
            with patch('botnim.sync.pdf_pipeline_processor.PDFDiscoveryService') as mock_discovery, \
                 patch('botnim.sync.pdf_pipeline_processor.PDFDownloadManager') as mock_download, \
                 patch('botnim.sync.pdf_pipeline_processor.PDFExtractionPipeline') as mock_pipeline, \
                 patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService') as mock_g_service:
                
                # Mock discovery - single PDF
                mock_discovery_instance = Mock()
                mock_discovery_instance.discover_pdfs_from_index_page.return_value = [
                    {'url': 'https://example.com/multiple-decisions.pdf', 'filename': 'multiple-decisions.pdf'}
                ]
                mock_discovery.return_value = mock_discovery_instance
                
                # Mock download
                mock_download_instance = Mock()
                mock_download_instance.download_pdf.return_value = Path('/tmp/test.pdf')
                mock_download.return_value = mock_download_instance
                
                # Mock pipeline - multiple decisions (array response)
                mock_pipeline_instance = Mock()
                mock_pipeline_instance.process_single_pdf.return_value = (
                    [
                        {"decision_number": "1", "decision_title": "First Decision", "decision_date": "2024-01-15"},
                        {"decision_number": "2", "decision_title": "Second Decision", "decision_date": "2024-01-16"}
                    ], 
                    None
                )
                mock_pipeline.return_value = mock_pipeline_instance
                
                # Mock Google Sheets service
                mock_g_service_instance = Mock()
                mock_g_service_instance.upload_csv_to_sheet.return_value = True
                mock_g_service.return_value = mock_g_service_instance
                
                # Create processor and run
                processor = PDFPipelineProcessor(mock_cache, mock_openai_client)
                result = processor.process_pipeline_source(source)
                
                # Assertions
                assert result["status"] == "completed"
                assert result["discovered_pdfs"] == 1
                assert result["processed_pdfs"] == 1  # One PDF processed
                assert result["failed_pdfs"] == 0
                
                # Verify pipeline was called once (single PDF)
                mock_pipeline_instance.process_single_pdf.assert_called_once()
                
                # Verify Google Sheets upload was called
                mock_g_service_instance.upload_csv_to_sheet.assert_called_once()
                
        finally:
            Path(config_path).unlink()

    def test_scenario_3_multiple_pdfs(self, mock_openai_client, mock_cache):
        """Test Scenario 3: Multiple PDFs → Multiple Decisions → Multiple Rows."""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(MULTIPLE_PDFS_CONFIG)
            config_path = f.name
        
        try:
            # Load configuration
            config = SyncConfig.from_yaml(config_path)
            source = config.get_source_by_id("multiple-pdfs")
            
            # Mock the PDF discovery and download
            with patch('botnim.sync.pdf_pipeline_processor.PDFDiscoveryService') as mock_discovery, \
                 patch('botnim.sync.pdf_pipeline_processor.PDFDownloadManager') as mock_download, \
                 patch('botnim.sync.pdf_pipeline_processor.PDFExtractionPipeline') as mock_pipeline, \
                 patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService') as mock_g_service:
                
                # Mock discovery - multiple PDFs
                mock_discovery_instance = Mock()
                mock_discovery_instance.discover_pdfs_from_index_page.return_value = [
                    {'url': 'https://example.com/decision1.pdf', 'filename': 'decision1.pdf'},
                    {'url': 'https://example.com/decision2.pdf', 'filename': 'decision2.pdf'}
                ]
                mock_discovery.return_value = mock_discovery_instance
                
                # Mock download
                mock_download_instance = Mock()
                mock_download_instance.download_pdf.return_value = Path('/tmp/test.pdf')
                mock_download.return_value = mock_download_instance
                
                # Mock pipeline - multiple decisions per PDF
                mock_pipeline_instance = Mock()
                mock_pipeline_instance.process_single_pdf.side_effect = [
                    ([{"decision_number": "1", "decision_title": "First Decision", "decision_date": "2024-01-15"}], None),
                    ([{"decision_number": "2", "decision_title": "Second Decision", "decision_date": "2024-01-16"}], None)
                ]
                mock_pipeline.return_value = mock_pipeline_instance
                
                # Mock Google Sheets service
                mock_g_service_instance = Mock()
                mock_g_service_instance.upload_csv_to_sheet.return_value = True
                mock_g_service.return_value = mock_g_service_instance
                
                # Create processor and run
                processor = PDFPipelineProcessor(mock_cache, mock_openai_client)
                result = processor.process_pipeline_source(source)
                
                # Assertions
                assert result["status"] == "completed"
                assert result["discovered_pdfs"] == 2
                assert result["processed_pdfs"] == 2  # Two PDFs processed
                assert result["failed_pdfs"] == 0
                
                # Verify pipeline was called twice (two PDFs)
                assert mock_pipeline_instance.process_single_pdf.call_count == 2
                
                # Verify Google Sheets upload was called once (single CSV with all data)
                mock_g_service_instance.upload_csv_to_sheet.assert_called_once()
                
        finally:
            Path(config_path).unlink()

    @pytest.mark.asyncio
    async def test_all_scenarios_integration(self, mock_openai_client, mock_cache):
        """Integration test for all scenarios in the orchestrator."""
        
        # Create a config with all three scenarios
        full_config = """
version: "1.0.0"
name: "All Scenarios Test"
sources:
  - id: "scenario-1"
    name: "Single PDF Single Decision"
    type: "pdf_pipeline"
    enabled: true
    pdf_pipeline_config:
      input_config:
        url: "https://example.com/single.pdf"
        is_index_page: false
      output_config:
        spreadsheet_id: "test-spreadsheet-id"
        sheet_name: "Scenario1"
        use_adc: true
      processing_config:
        model: "gpt-4o-mini"
        fields:
          - name: "decision_title"
            type: "string"
            description: "Title of the decision"
        extraction_instructions: "Extract a single decision."
  
  - id: "scenario-2"
    name: "Single PDF Multiple Decisions"
    type: "pdf_pipeline"
    enabled: true
    pdf_pipeline_config:
      input_config:
        url: "https://example.com/multiple.pdf"
        is_index_page: false
      output_config:
        spreadsheet_id: "test-spreadsheet-id"
        sheet_name: "Scenario2"
        use_adc: true
      processing_config:
        model: "gpt-4o-mini"
        fields:
          - name: "decision_number"
            type: "string"
            description: "Number of the decision"
        extraction_instructions: "Extract multiple decisions as array."
  
  - id: "scenario-3"
    name: "Multiple PDFs"
    type: "pdf_pipeline"
    enabled: true
    pdf_pipeline_config:
      input_config:
        url: "https://example.com/index"
        is_index_page: true
        file_pattern: "*.pdf"
      output_config:
        spreadsheet_id: "test-spreadsheet-id"
        sheet_name: "Scenario3"
        use_adc: true
      processing_config:
        model: "gpt-4o-mini"
        fields:
          - name: "decision_title"
            type: "string"
            description: "Title of the decision"
        extraction_instructions: "Extract decision information."
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(full_config)
            config_path = f.name
        
        try:
            # Mock all dependencies
            with patch('botnim.sync.pdf_pipeline_processor.PDFDiscoveryService') as mock_discovery, \
                 patch('botnim.sync.pdf_pipeline_processor.PDFDownloadManager') as mock_download, \
                 patch('botnim.sync.pdf_pipeline_processor.PDFExtractionPipeline') as mock_pipeline, \
                 patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService') as mock_g_service, \
                 patch('botnim.sync.orchestrator.VectorStoreES') as mock_vector_store, \
                 patch('botnim.sync.orchestrator.get_openai_client') as mock_get_openai:
                
                # Setup mocks
                mock_discovery_instance = Mock()
                mock_discovery_instance.discover_pdfs_from_index_page.return_value = [
                    {'url': 'https://example.com/test.pdf', 'filename': 'test.pdf'}
                ]
                mock_discovery.return_value = mock_discovery_instance
                
                mock_download_instance = Mock()
                mock_download_instance.download_pdf.return_value = Path('/tmp/test.pdf')
                mock_download.return_value = mock_download_instance
                
                mock_pipeline_instance = Mock()
                mock_pipeline_instance.process_single_pdf.return_value = (
                    [{"decision_title": "Test Decision"}], None
                )
                mock_pipeline.return_value = mock_pipeline_instance
                
                mock_g_service_instance = Mock()
                mock_g_service_instance.upload_csv_to_sheet.return_value = True
                mock_g_service.return_value = mock_g_service_instance
                
                mock_vector_store.return_value = Mock()
                mock_get_openai.return_value = mock_openai_client
                
                # Create orchestrator and run pre-processing
                config = SyncConfig.from_yaml(config_path)
                orchestrator = SyncOrchestrator(config, environment="test")
                
                await orchestrator._run_preprocessing_pipelines()
                
                # Verify all three scenarios were processed
                # Note: Each pipeline creates its own PDFPipelineProcessor instance, 
                # so the mocks are called once per pipeline
                assert mock_discovery_instance.discover_pdfs_from_index_page.call_count >= 1
                assert mock_pipeline_instance.process_single_pdf.call_count >= 1
                assert mock_g_service_instance.upload_csv_to_sheet.call_count >= 1
                
        finally:
            Path(config_path).unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
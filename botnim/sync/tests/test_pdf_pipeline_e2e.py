"""
End-to-end test for the PDF-to-Spreadsheet pipeline feature.

This test verifies the complete flow:
1. Configuration loading
2. PDF discovery and download
3. PDF processing and extraction
4. CSV generation
5. Google Sheets upload
6. Integration with the main sync orchestrator
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, Mock
from typing import Dict, Any

from botnim.sync.config import SyncConfig
from botnim.sync.orchestrator import SyncOrchestrator
from botnim.sync.pdf_pipeline_processor import PDFPipelineProcessor
from botnim.sync.cache import SyncCache


# Realistic test configuration
E2E_TEST_CONFIG = """
version: "1.0.0"
name: "PDF Pipeline E2E Test"
description: "End-to-end test for PDF-to-Spreadsheet pipeline"
sources:
  - id: "ethics-committee-decisions"
    name: "Ethics Committee Decisions"
    type: "pdf_pipeline"
    enabled: true
    description: "Process ethics committee decisions from PDFs"
    pdf_pipeline_config:
      input_config:
        url: "https://example.com/ethics-decisions/"
        is_index_page: true
        file_pattern: ".*pdf"
        headers:
          User-Agent: "Mozilla/5.0 (compatible; BotNim/1.0)"
      output_config:
        spreadsheet_id: "test-spreadsheet-id-12345"
        sheet_name: "Ethics Committee Decisions"
        use_adc: true
      processing_config:
        model: "gpt-4o-mini"
        max_tokens: 4000
        temperature: 0.1
        fields:
          - name: "decision_number"
            type: "string"
            description: "The decision number or identifier"
            required: true
          - name: "decision_date"
            type: "date"
            description: "The date of the decision"
            required: true
          - name: "subject"
            type: "string"
            description: "The subject or title of the decision"
            required: true
          - name: "member_name"
            type: "string"
            description: "The name of the committee member involved"
            required: false
          - name: "decision_summary"
            type: "text"
            description: "A summary of the decision"
            required: true
          - name: "full_text"
            type: "text"
            description: "The complete text of the decision"
            required: true
        extraction_instructions: |
          הפק את השדות הנדרשים מהטקסט של קובץ ה-PDF. השתמש ברשימת השדות וההנחיות. 
          החזר אובייקט JSON שבו שמות השדות, אך כל הערכים בשפה המקורית (עברית) כפי שמופיעים במסמך. 
          אל תתרגם או תשנה את הטקסט המקורי. ודא ששדה full_text תמיד מכיל את כל הטקסט של המסמך כפי שהופק מה-PDF.

          אם המסמך מכיל מספר החלטות או מקרים נפרדים (למשל החלטות ממוספרות 1, 2, 3, 4 או מקרים שונים של אנשים שונים), 
          הפק מידע עבור כל החלטה בנפרד. החזר מערך של אובייקטים JSON, כל אובייקט מייצג החלטה אחת. 
          עבור כל החלטה, מלא את השדות הרלוונטיים לאותה החלטה ספציפית. אם יש מספר החלטות, 
          החזר מערך של אובייקטים במקום אובייקט בודד.

  - id: "legal-advisor-letters"
    name: "Legal Advisor Letters"
    type: "pdf_pipeline"
    enabled: true
    description: "Process legal advisor letters and responses"
    pdf_pipeline_config:
      input_config:
        url: "https://example.com/legal-letters/"
        is_index_page: true
        file_pattern: ".*pdf"
        headers:
          User-Agent: "Mozilla/5.0 (compatible; BotNim/1.0)"
      output_config:
        spreadsheet_id: "test-spreadsheet-id-67890"
        sheet_name: "Legal Advisor Letters"
        use_adc: true
      processing_config:
        model: "gpt-4o-mini"
        max_tokens: 4000
        temperature: 0.1
        fields:
          - name: "letter_number"
            type: "string"
            description: "The letter number or reference"
            required: true
          - name: "date"
            type: "date"
            description: "The date of the letter"
            required: true
          - name: "recipient"
            type: "string"
            description: "The recipient of the letter"
            required: true
          - name: "subject"
            type: "string"
            description: "The subject of the letter"
            required: true
          - name: "content_summary"
            type: "text"
            description: "A summary of the letter content"
            required: true
          - name: "full_text"
            type: "text"
            description: "The complete text of the letter"
            required: true
        extraction_instructions: |
          הפק את השדות הנדרשים מהטקסט של מכתב היועץ המשפטי. 
          החזר אובייקט JSON עם השדות הנדרשים. אם המסמך מכיל מספר מכתבים נפרדים, 
          החזר מערך של אובייקטים.

# Global settings
default_versioning_strategy: "hash"
default_fetch_strategy: "direct"
cache_directory: "./cache"
embedding_cache_path: "./cache/embeddings.sqlite"
version_cache_path: "./cache/versions.json"
max_concurrent_sources: 3
timeout_per_source: 300
log_level: "INFO"
"""


class TestPDFPipelineE2E:
    """End-to-end tests for the PDF-to-Spreadsheet pipeline."""

    @pytest.fixture
    def temp_config_file(self):
        """Create a temporary configuration file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(E2E_TEST_CONFIG)
            config_path = f.name
        
        yield config_path
        
        # Cleanup
        if os.path.exists(config_path):
            os.unlink(config_path)

    @pytest.fixture
    def mock_openai_client(self):
        """Mock OpenAI client with realistic responses."""
        client = Mock()
        
        # Mock response for ethics committee decisions
        ethics_response = Mock()
        ethics_response.choices = [Mock()]
        ethics_response.choices[0].message.content = '''[
            {
                "decision_number": "2024/001",
                "decision_date": "2024-01-15",
                "subject": "בקשה להעברת חברות בוועדה",
                "member_name": "חבר הכנסת דוד כהן",
                "decision_summary": "אושרה העברת חברות מוועדת הכספים לוועדת החוקה",
                "full_text": "החלטת ועדת האתיקה מספר 2024/001 מיום 15.1.2024..."
            },
            {
                "decision_number": "2024/002", 
                "decision_date": "2024-01-16",
                "subject": "בקשה לפרסום הצהרת הון",
                "member_name": "חברת הכנסת שרה לוי",
                "decision_summary": "נדחתה בקשה לדחיית מועד פרסום הצהרת הון",
                "full_text": "החלטת ועדת האתיקה מספר 2024/002 מיום 16.1.2024..."
            }
        ]'''
        
        # Mock response for legal advisor letters
        legal_response = Mock()
        legal_response.choices = [Mock()]
        legal_response.choices[0].message.content = '''[
            {
                "letter_number": "LEG-2024-001",
                "date": "2024-01-10",
                "recipient": "יושב ראש הכנסת",
                "subject": "הנחיות לעניין נוהלי הצבעה",
                "content_summary": "הנחיות חדשות לעניין נוהלי הצבעה בוועדות",
                "full_text": "לכבוד יושב ראש הכנסת, הנחיות חדשות לעניין נוהלי הצבעה..."
            }
        ]'''
        
        client.chat.completions.create.side_effect = [
            ethics_response,  # First call for ethics decisions
            legal_response,   # Second call for legal letters
        ]
        
        return client

    def test_config_loading_and_validation(self, temp_config_file):
        """Test that the configuration loads and validates correctly."""
        # Load configuration
        config = SyncConfig.from_yaml(temp_config_file)
        
        # Verify basic structure
        assert config.name == "PDF Pipeline E2E Test"
        assert len(config.sources) == 2
        
        # Verify PDF pipeline sources
        pdf_pipeline_sources = config.get_sources_by_type("pdf_pipeline")
        assert len(pdf_pipeline_sources) == 2
        
        # Verify first source
        ethics_source = config.get_source_by_id("ethics-committee-decisions")
        assert ethics_source is not None
        assert ethics_source.name == "Ethics Committee Decisions"
        assert ethics_source.type == "pdf_pipeline"
        assert ethics_source.pdf_pipeline_config is not None
        
        # Verify processing config
        processing_config = ethics_source.pdf_pipeline_config.processing_config
        assert processing_config.model == "gpt-4o-mini"
        assert len(processing_config.fields) == 6
        
        # Verify field configuration
        field_names = [field.name for field in processing_config.fields]
        expected_fields = ["decision_number", "decision_date", "subject", "member_name", "decision_summary", "full_text"]
        assert field_names == expected_fields

    @pytest.mark.asyncio
    async def test_pdf_pipeline_processor_integration(self, temp_config_file, mock_openai_client):
        """Test the PDF pipeline processor with realistic data."""
        config = SyncConfig.from_yaml(temp_config_file)
        ethics_source = config.get_source_by_id("ethics-committee-decisions")
        
        # Create cache
        cache = SyncCache()
        
        # Mock PDF discovery and download
        with patch('botnim.sync.pdf_pipeline_processor.PDFDiscoveryService') as mock_discovery, \
             patch('botnim.sync.pdf_pipeline_processor.PDFDownloadManager') as mock_download, \
             patch('botnim.sync.pdf_pipeline_processor.PDFExtractionPipeline') as mock_pipeline, \
             patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService') as mock_g_service:
            
            # Mock discovery - return realistic PDF info
            mock_discovery_instance = Mock()
            mock_discovery_instance.discover_pdfs_from_index_page.return_value = [
                {
                    'url': 'https://example.com/ethics-decisions/decision1.pdf',
                    'filename': 'decision1.pdf',
                    'url_hash': 'abc123'
                },
                {
                    'url': 'https://example.com/ethics-decisions/decision2.pdf', 
                    'filename': 'decision2.pdf',
                    'url_hash': 'def456'
                }
            ]
            mock_discovery.return_value = mock_discovery_instance
            
            # Mock download
            mock_download_instance = Mock()
            mock_download_instance.download_pdf.side_effect = [
                Path('/tmp/decision1.pdf'),
                Path('/tmp/decision2.pdf')
            ]
            mock_download.return_value = mock_download_instance
            
            # Mock pipeline - return realistic extracted data
            mock_pipeline_instance = Mock()
            mock_pipeline_instance.process_single_pdf.side_effect = [
                ([
                    {
                        "decision_number": "2024/001",
                        "decision_date": "2024-01-15",
                        "subject": "בקשה להעברת חברות בוועדה",
                        "member_name": "חבר הכנסת דוד כהן",
                        "decision_summary": "אושרה העברת חברות מוועדת הכספים לוועדת החוקה",
                        "full_text": "החלטת ועדת האתיקה מספר 2024/001..."
                    }
                ], None),
                ([
                    {
                        "decision_number": "2024/002",
                        "decision_date": "2024-01-16", 
                        "subject": "בקשה לפרסום הצהרת הון",
                        "member_name": "חברת הכנסת שרה לוי",
                        "decision_summary": "נדחתה בקשה לדחיית מועד פרסום הצהרת הון",
                        "full_text": "החלטת ועדת האתיקה מספר 2024/002..."
                    }
                ], None)
            ]
            mock_pipeline.return_value = mock_pipeline_instance
            
            # Mock Google Sheets service
            mock_g_service_instance = Mock()
            mock_g_service_instance.upload_csv_to_sheet.return_value = True
            mock_g_service.return_value = mock_g_service_instance
            
            # Create processor and run
            processor = PDFPipelineProcessor(cache, mock_openai_client)
            result = processor.process_pipeline_source(ethics_source)
        
            # Verify results
            assert result["status"] == "completed"
            assert result["discovered_pdfs"] == 2
            assert result["processed_pdfs"] == 2
            assert result["failed_pdfs"] == 0
            assert len(result["errors"]) == 0
            
            # Verify pipeline was called for each PDF
            assert mock_pipeline_instance.process_single_pdf.call_count == 2
            
            # Verify Google Sheets upload was called
            mock_g_service_instance.upload_csv_to_sheet.assert_called_once()
            
            # Verify the CSV was created with correct data
            call_args = mock_g_service_instance.upload_csv_to_sheet.call_args
            csv_path = call_args[1]['csv_path']
            assert csv_path.endswith('.csv')
            assert 'Ethics_Committee_Decisions' in csv_path

    @pytest.mark.asyncio
    async def test_orchestrator_integration(self, temp_config_file, mock_openai_client):
        """Test the complete orchestrator integration."""
        config = SyncConfig.from_yaml(temp_config_file)
        
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
                [{"decision_number": "2024/001", "subject": "Test Decision"}], None
            )
            mock_pipeline.return_value = mock_pipeline_instance
            
            mock_g_service_instance = Mock()
            mock_g_service_instance.upload_csv_to_sheet.return_value = True
            mock_g_service.return_value = mock_g_service_instance
            
            mock_vector_store.return_value = Mock()
            mock_get_openai.return_value = mock_openai_client
            
            # Create orchestrator and run pre-processing
            orchestrator = SyncOrchestrator(config, environment="test")
            
            # Run pre-processing pipelines
            await orchestrator._run_preprocessing_pipelines()
            
            # Verify that both PDF pipeline sources were processed
            assert mock_discovery_instance.discover_pdfs_from_index_page.call_count >= 2
            assert mock_pipeline_instance.process_single_pdf.call_count >= 2
            assert mock_g_service_instance.upload_csv_to_sheet.call_count >= 2

    def test_csv_output_format(self, temp_config_file, mock_openai_client):
        """Test that the CSV output format is correct."""
        config = SyncConfig.from_yaml(temp_config_file)
        ethics_source = config.get_source_by_id("ethics-committee-decisions")
        
        cache = SyncCache()
        
        with patch('botnim.sync.pdf_pipeline_processor.PDFDiscoveryService') as mock_discovery, \
             patch('botnim.sync.pdf_pipeline_processor.PDFDownloadManager') as mock_download, \
             patch('botnim.sync.pdf_pipeline_processor.PDFExtractionPipeline') as mock_pipeline, \
             patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService') as mock_g_service:
            
            # Setup mocks
            mock_discovery_instance = Mock()
            mock_discovery_instance.discover_pdfs_from_index_page.return_value = [
                {'url': 'https://example.com/test.pdf', 'filename': 'test.pdf'}
            ]
            mock_discovery.return_value = mock_discovery_instance
            
            mock_download_instance = Mock()
            mock_download_instance.download_pdf.return_value = Path('/tmp/test.pdf')
            mock_download.return_value = mock_download_instance
            
            # Return realistic extracted data
            mock_pipeline_instance = Mock()
            mock_pipeline_instance.process_single_pdf.return_value = (
                [
                    {
                        "decision_number": "2024/001",
                        "decision_date": "2024-01-15",
                        "subject": "בקשה להעברת חברות בוועדה",
                        "member_name": "חבר הכנסת דוד כהן",
                        "decision_summary": "אושרה העברת חברות מוועדת הכספים לוועדת החוקה",
                        "full_text": "החלטת ועדת האתיקה מספר 2024/001..."
                    }
                ], 
                None
            )
            mock_pipeline.return_value = mock_pipeline_instance
            
            mock_g_service_instance = Mock()
            mock_g_service_instance.upload_csv_to_sheet.return_value = True
            mock_g_service.return_value = mock_g_service_instance
            
            # Create processor and run
            processor = PDFPipelineProcessor(cache, mock_openai_client)
            result = processor.process_pipeline_source(ethics_source)
            
            # Verify the result
            assert result["status"] == "completed"
            
            # Verify that Google Sheets sync was called with correct parameters
            mock_g_service_instance.upload_csv_to_sheet.assert_called_once()
            call_args = mock_g_service_instance.upload_csv_to_sheet.call_args
            
            # Verify the parameters
            assert call_args[1]['spreadsheet_id'] == 'test-spreadsheet-id-12345'
            assert call_args[1]['sheet_name'] == 'Ethics Committee Decisions'
            assert call_args[1]['replace_existing'] == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
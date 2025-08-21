#!/usr/bin/env python3
"""
Focused unit test for Google Sheets upload in PDFPipelineProcessor.
This test patches the correct functions where they're used and avoids CLI/orchestrator layers.
"""

import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from botnim.sync.pdf_pipeline_processor import PDFPipelineProcessor


@patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService.upload_csv_to_sheet')
@patch('botnim.sync.pdf_pipeline_processor.PDFPipelineProcessor._process_open_budget_pdfs')
def test_pdf_pipeline_processor_upload(mock_process_open_budget_pdfs, mock_upload_csv_to_sheet):
	"""Ensure PDFPipelineProcessor uploads CSV to the configured Google Sheet."""
	mock_upload_csv_to_sheet.return_value = True

	with tempfile.TemporaryDirectory() as temp_dir:
		temp_path = Path(temp_dir)
		dummy_csv_path = temp_path / 'output.csv'
		dummy_csv_path.write_text('url,revision\nhttp://example.com,1\n')

		# Make the processor think PDF processing succeeded and produced our dummy file
		mock_process_open_budget_pdfs.return_value = (dummy_csv_path, 1, 0, [])

		# Build a minimal dummy source with required attributes
		output_config = SimpleNamespace(spreadsheet_id='test-spreadsheet-id', sheet_name='Test Sheet', use_adc=True)
		pdf_config = SimpleNamespace(index_csv_url='http://example.com/index.csv', datapackage_url='http://example.com/datapackage.json', output_config=output_config, processing=None)
		source = SimpleNamespace(id='test-pdf-pipeline', name='Test PDF Pipeline', description='', type='pdf_pipeline', pdf_config=pdf_config, enabled=True)

		processor = PDFPipelineProcessor(cache=None, openai_client=None)
		result = processor.process_pipeline_source(source)

		# Assert upload was invoked with expected arguments
		mock_upload_csv_to_sheet.assert_called_once_with(
			csv_path=str(dummy_csv_path),
			spreadsheet_id='test-spreadsheet-id',
			sheet_name='Test Sheet',
			replace_existing=True
		)

		assert result.get('status') in ('completed', 'completed_no_data')


@patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService.upload_csv_to_sheet')
@patch('botnim.sync.pdf_pipeline_processor.PDFPipelineProcessor._process_open_budget_pdfs')
def test_pdf_pipeline_processor_upload_failure(mock_process_open_budget_pdfs, mock_upload_csv_to_sheet):
	"""If upload fails, the processor should mark status failed and still call upload once."""
	mock_upload_csv_to_sheet.return_value = False

	with tempfile.TemporaryDirectory() as temp_dir:
		dummy_csv_path = Path(temp_dir) / 'output.csv'
		dummy_csv_path.write_text('url,revision\nhttp://example.com,1\n')
		mock_process_open_budget_pdfs.return_value = (dummy_csv_path, 1, 0, [])

		output_config = SimpleNamespace(spreadsheet_id='test-spreadsheet-id', sheet_name='Test Sheet', use_adc=True)
		pdf_config = SimpleNamespace(index_csv_url='http://example.com/index.csv', datapackage_url='http://example.com/datapackage.json', output_config=output_config, processing=None)
		source = SimpleNamespace(id='test-pdf-pipeline', name='Test PDF Pipeline', description='', type='pdf_pipeline', pdf_config=pdf_config, enabled=True)

		processor = PDFPipelineProcessor(cache=None, openai_client=None)
		result = processor.process_pipeline_source(source)

		mock_upload_csv_to_sheet.assert_called_once()
		assert result.get('status') == 'failed'


@patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService.upload_csv_to_sheet')
@patch('botnim.sync.pdf_pipeline_processor.PDFPipelineProcessor._process_open_budget_pdfs')
def test_pdf_pipeline_processor_placeholder_spreadsheet_id(mock_process_open_budget_pdfs, mock_upload_csv_to_sheet):
	"""If spreadsheet_id is a placeholder, upload should not be attempted and status is failed."""
	# Upload should not be called
	with tempfile.TemporaryDirectory() as temp_dir:
		dummy_csv_path = Path(temp_dir) / 'output.csv'
		dummy_csv_path.write_text('url,revision\nhttp://example.com,1\n')
		mock_process_open_budget_pdfs.return_value = (dummy_csv_path, 1, 0, [])

		output_config = SimpleNamespace(spreadsheet_id='YOUR_SPREADSHEET_ID_HERE', sheet_name='Test Sheet', use_adc=True)
		pdf_config = SimpleNamespace(index_csv_url='http://example.com/index.csv', datapackage_url='http://example.com/datapackage.json', output_config=output_config, processing=None)
		source = SimpleNamespace(id='test-pdf-pipeline', name='Test PDF Pipeline', description='', type='pdf_pipeline', pdf_config=pdf_config, enabled=True)

		processor = PDFPipelineProcessor(cache=None, openai_client=None)
		result = processor.process_pipeline_source(source)

		mock_upload_csv_to_sheet.assert_not_called()
		assert result.get('status') == 'failed'


@patch('botnim.sync.pdf_pipeline_processor.GoogleSheetsService.upload_csv_to_sheet')
@patch('botnim.sync.pdf_pipeline_processor.PDFPipelineProcessor._process_open_budget_pdfs')
def test_pdf_pipeline_processor_no_data_skips_upload(mock_process_open_budget_pdfs, mock_upload_csv_to_sheet):
	"""When no data is processed, upload must be skipped and status is completed_no_data."""
	with tempfile.TemporaryDirectory() as temp_dir:
		dummy_csv_path = Path(temp_dir) / 'output.csv'
		dummy_csv_path.write_text('url,revision\n')
		# Simulate zero processed rows
		mock_process_open_budget_pdfs.return_value = (dummy_csv_path, 0, 0, [])

		output_config = SimpleNamespace(spreadsheet_id='test-spreadsheet-id', sheet_name='Test Sheet', use_adc=True)
		pdf_config = SimpleNamespace(index_csv_url='http://example.com/index.csv', datapackage_url='http://example.com/datapackage.json', output_config=output_config, processing=None)
		source = SimpleNamespace(id='test-pdf-pipeline', name='Test PDF Pipeline', description='', type='pdf_pipeline', pdf_config=pdf_config, enabled=True)

		processor = PDFPipelineProcessor(cache=None, openai_client=None)
		result = processor.process_pipeline_source(source)

		mock_upload_csv_to_sheet.assert_not_called()
		assert result.get('status') == 'completed_no_data'
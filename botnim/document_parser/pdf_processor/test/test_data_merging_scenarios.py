"""
Test data merging scenarios for PDF processing pipeline.

This module tests the data merging logic that handles:
1. Adding missing rows from datapackage to input data
2. Removing invalid rows that don't exist in datapackage
3. Complete pipeline execution with data validation
"""

import unittest
import tempfile
import os
import pandas as pd
import json
from unittest.mock import patch, MagicMock
from typing import List, Dict, Any

from botnim.document_parser.pdf_processor.pdf_pipeline import PDFExtractionPipeline
from botnim.document_parser.pdf_processor.sync_config_adapter import SyncConfigAdapter
from botnim.document_parser.pdf_processor.open_budget_data_source import OpenBudgetDataSource
from botnim.document_parser.pdf_processor.csv_output import read_csv, write_csv


class TestDataMergingScenarios(unittest.TestCase):
    """Test data merging scenarios for PDF processing pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), 'data')
        
        # Load mock data
        self.mock_index_path = os.path.join(self.test_data_dir, 'mock_index.csv')
        self.mock_datapackage_path = os.path.join(self.test_data_dir, 'mock_datapackage.json')
        
        # Create test configuration
        self.test_config = {
            'sources': [{
                'id': 'test_ethics_committee_decisions',
                'name': 'Test Ethics Committee Decisions',
                'index_csv_url': f'file://{self.mock_index_path}',
                'datapackage_url': f'file://{self.mock_datapackage_path}',
                'unique_id_field': 'url',
                'fields': [
                    {
                        'name': 'decision_number',
                        'type': 'string',
                        'description': 'Decision number',
                        'required': True
                    },
                    {
                        'name': 'member_name',
                        'type': 'string',
                        'description': 'Member name',
                        'required': False
                    }
                ]
            }]
        }

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_input_csv(self, data: List[Dict[str, Any]], filename: str = 'input.csv') -> str:
        """Create a mock input CSV file for testing."""
        input_path = os.path.join(self.temp_dir, filename)
        write_csv(data, input_path)
        return input_path

    def create_mock_output_csv(self, data: List[Dict[str, Any]], filename: str = 'output.csv') -> str:
        """Create a mock output CSV file for testing."""
        output_path = os.path.join(self.temp_dir, filename)
        write_csv(data, output_path)
        return output_path

    def load_mock_datapackage_data(self) -> List[Dict[str, Any]]:
        """Load mock datapackage data from the test files."""
        # Read the mock index.csv
        index_df = pd.read_csv(self.mock_index_path)
        
        # Convert to list of dictionaries with mock extracted fields
        datapackage_data = []
        for _, row in index_df.iterrows():
            datapackage_data.append({
                'url': row['url'],
                'title': row['title'],
                'filename': row['filename'],
                'date': row['date'],
                'revision': '2025.08.20-01',  # Mock revision
                'decision_number': f"DEC-{row['filename'].split('.')[0]}",
                'member_name': f"Member-{row['filename'].split('.')[0]}"
            })
        
        return datapackage_data

    def test_data_completion_adds_missing_rows(self):
        """Test that missing rows from datapackage are added to output."""
        
        # Create input CSV with only 2 out of 3 rows from datapackage
        datapackage_data = self.load_mock_datapackage_data()
        input_data = datapackage_data[:2]  # Only first 2 rows
        
        input_path = self.create_mock_input_csv(input_data, 'input_missing.csv')
        output_path = os.path.join(self.temp_dir, 'output.csv')
        
        # Mock the pipeline to return the full datapackage data
        with patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline._process_source') as mock_process, \
             patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline.__init__') as mock_init:
            
            mock_process.return_value = (datapackage_data, [])  # All 3 rows processed
            mock_init.return_value = None  # Mock the constructor
            
            # Create pipeline and run
            from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig
            config_obj = PDFExtractionConfig(**self.test_config)
            pipeline = PDFExtractionPipeline.__new__(PDFExtractionPipeline)
            pipeline.config = config_obj
            pipeline.metrics = None  # Add missing attribute
            pipeline.process_directory(self.temp_dir)
            
            # Read the output
            output_data = read_csv(output_path)
            
            # Verify all 3 rows are present
            self.assertEqual(len(output_data), 3, "Output should contain all 3 rows from datapackage")
            
            # Verify the missing row was added
            urls_in_output = {row['url'] for row in output_data}
            urls_in_input = {row['url'] for row in input_data}
            missing_url = datapackage_data[2]['url']  # The missing row
            
            self.assertIn(missing_url, urls_in_output, "Missing URL should be in output")
            self.assertNotIn(missing_url, urls_in_input, "Missing URL should not be in input")

    def test_data_cleanup_removes_invalid_rows(self):
        """Test that rows with URLs not in datapackage are removed from output."""
        
        # Create input CSV with valid rows plus invalid rows
        datapackage_data = self.load_mock_datapackage_data()
        invalid_rows = [
            {
                'url': 'https://invalid-url-1.com/document.pdf',
                'title': 'Invalid Document 1',
                'filename': 'invalid1.pdf',
                'date': '2024-01-01',
                'revision': '2025.08.20-01',
                'decision_number': 'INVALID-1',
                'member_name': 'Invalid Member 1'
            },
            {
                'url': 'https://invalid-url-2.com/document.pdf',
                'title': 'Invalid Document 2',
                'filename': 'invalid2.pdf',
                'date': '2024-01-02',
                'revision': '2025.08.20-01',
                'decision_number': 'INVALID-2',
                'member_name': 'Invalid Member 2'
            }
        ]
        
        input_data = datapackage_data + invalid_rows  # Valid + invalid rows
        input_path = self.create_mock_input_csv(input_data, 'input_invalid.csv')
        output_path = os.path.join(self.temp_dir, 'output.csv')
        
        # Mock the pipeline to return only the valid datapackage data
        with patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline._process_source') as mock_process, \
             patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline.__init__') as mock_init:
            
            mock_process.return_value = (datapackage_data, [])  # Only valid rows processed
            mock_init.return_value = None  # Mock the constructor
            
            # Create pipeline and run
            from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig
            config_obj = PDFExtractionConfig(**self.test_config)
            pipeline = PDFExtractionPipeline.__new__(PDFExtractionPipeline)
            pipeline.config = config_obj
            pipeline.metrics = None  # Add missing attribute
            pipeline.process_directory(self.temp_dir)
            
            # Read the output
            output_data = read_csv(output_path)
            
            # Verify only valid rows are present
            self.assertEqual(len(output_data), len(datapackage_data), 
                           f"Output should contain only {len(datapackage_data)} valid rows")
            
            # Verify invalid URLs are not in output
            urls_in_output = {row['url'] for row in output_data}
            invalid_urls = {row['url'] for row in invalid_rows}
            
            for invalid_url in invalid_urls:
                self.assertNotIn(invalid_url, urls_in_output, 
                               f"Invalid URL {invalid_url} should not be in output")

    def test_data_merging_preserves_existing_valid_data(self):
        """Test that existing valid data is preserved when no changes are detected."""
        
        # Create input CSV with all valid rows
        datapackage_data = self.load_mock_datapackage_data()
        input_data = datapackage_data.copy()
        input_path = self.create_mock_input_csv(input_data, 'input_valid.csv')
        output_path = os.path.join(self.temp_dir, 'output.csv')
        
        # Mock the pipeline to return the same data (simulating no changes detected)
        with patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline._process_source') as mock_process, \
             patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline.__init__') as mock_init:
            
            # Return the same data to simulate no changes, but some processing occurred
            mock_process.return_value = (input_data, [])  # Return existing data as if reprocessed
            mock_init.return_value = None  # Mock the constructor
            
            # Create pipeline and run
            from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig
            config_obj = PDFExtractionConfig(**self.test_config)
            pipeline = PDFExtractionPipeline.__new__(PDFExtractionPipeline)
            pipeline.config = config_obj
            pipeline.metrics = None  # Add missing attribute
            pipeline.process_directory(self.temp_dir)
            
            # Read the output
            output_data = read_csv(output_path)
            
            # Verify all existing data is preserved
            self.assertEqual(len(output_data), len(input_data), 
                           "Output should preserve all existing valid data")
            
            # Verify all URLs are preserved
            urls_in_output = {row['url'] for row in output_data}
            urls_in_input = {row['url'] for row in input_data}
            
            self.assertEqual(urls_in_output, urls_in_input, 
                           "All URLs from input should be preserved in output")

    def test_mixed_scenario_add_and_remove(self):
        """Test a mixed scenario with both adding missing rows and removing invalid rows."""
        
        # Create input CSV with some missing rows and some invalid rows
        datapackage_data = self.load_mock_datapackage_data()
        
        # Remove one valid row (missing)
        input_data = datapackage_data[:2]  # Only first 2 rows
        
        # Add one invalid row
        invalid_row = {
            'url': 'https://invalid-url.com/document.pdf',
            'title': 'Invalid Document',
            'filename': 'invalid.pdf',
            'date': '2024-01-01',
            'revision': '2025.08.20-01',
            'decision_number': 'INVALID',
            'member_name': 'Invalid Member'
        }
        input_data.append(invalid_row)
        
        input_path = self.create_mock_input_csv(input_data, 'input_mixed.csv')
        output_path = os.path.join(self.temp_dir, 'output.csv')
        
        # Mock the pipeline to return the full valid datapackage data
        with patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline._process_source') as mock_process, \
             patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline.__init__') as mock_init:
            
            mock_process.return_value = (datapackage_data, [])  # All 3 valid rows processed
            mock_init.return_value = None  # Mock the constructor
            
            # Create pipeline and run
            from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig
            config_obj = PDFExtractionConfig(**self.test_config)
            pipeline = PDFExtractionPipeline.__new__(PDFExtractionPipeline)
            pipeline.config = config_obj
            pipeline.metrics = None  # Add missing attribute
            pipeline.process_directory(self.temp_dir)
            
            # Read the output
            output_data = read_csv(output_path)
            
            # Verify final result has exactly the valid datapackage rows
            self.assertEqual(len(output_data), len(datapackage_data), 
                           f"Output should contain exactly {len(datapackage_data)} valid rows")
            
            # Verify the missing row was added back
            missing_url = datapackage_data[2]['url']
            urls_in_output = {row['url'] for row in output_data}
            self.assertIn(missing_url, urls_in_output, "Missing URL should be added back")
            
            # Verify the invalid row was removed
            invalid_url = invalid_row['url']
            self.assertNotIn(invalid_url, urls_in_output, "Invalid URL should be removed")

    def test_revision_change_triggers_full_reprocessing(self):
        """Test that revision change triggers processing of all files."""
        
        # Create input CSV with old revision
        datapackage_data = self.load_mock_datapackage_data()
        input_data = []
        for row in datapackage_data:
            row_copy = row.copy()
            row_copy['revision'] = '2025.08.19-01'  # Old revision
            input_data.append(row_copy)
        
        input_path = self.create_mock_input_csv(input_data, 'input_old_revision.csv')
        output_path = os.path.join(self.temp_dir, 'output.csv')
        
        # Mock the pipeline to return updated data with new revision
        updated_data = []
        for row in datapackage_data:
            row_copy = row.copy()
            row_copy['revision'] = '2025.08.20-01'  # New revision
            updated_data.append(row_copy)
        
        with patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline._process_source') as mock_process, \
             patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline.__init__') as mock_init:
            
            mock_process.return_value = (updated_data, [])  # All rows reprocessed
            mock_init.return_value = None  # Mock the constructor
            
            # Create pipeline and run
            from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig
            config_obj = PDFExtractionConfig(**self.test_config)
            pipeline = PDFExtractionPipeline.__new__(PDFExtractionPipeline)
            pipeline.config = config_obj
            pipeline.metrics = None  # Add missing attribute
            pipeline.process_directory(self.temp_dir)
            
            # Read the output
            output_data = read_csv(output_path)
            
            # Verify all rows have new revision
            for row in output_data:
                self.assertEqual(row['revision'], '2025.08.20-01', 
                               "All rows should have updated revision")

    def test_cli_command_complete_pipeline(self):
        """Test that CLI command processes all PDFs and updates spreadsheet completely."""
        
        # This test simulates the complete CLI pipeline execution
        datapackage_data = self.load_mock_datapackage_data()
        
        # Create input with mixed valid/invalid data
        input_data = datapackage_data[:2]  # Only 2 valid rows
        invalid_row = {
            'url': 'https://invalid-url.com/document.pdf',
            'title': 'Invalid Document',
            'filename': 'invalid.pdf',
            'date': '2024-01-01',
            'revision': '2025.08.20-01',
            'decision_number': 'INVALID',
            'member_name': 'Invalid Member'
        }
        input_data.append(invalid_row)
        
        input_path = self.create_mock_input_csv(input_data, 'input_cli.csv')
        output_path = os.path.join(self.temp_dir, 'output.csv')
        
        # Mock the complete pipeline processing
        with patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline._process_source') as mock_process, \
             patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline.__init__') as mock_init:
            
            mock_process.return_value = (datapackage_data, [])  # All valid data processed
            mock_init.return_value = None  # Mock the constructor
            
            # Mock the CLI command execution
            with patch('sys.argv', ['pdf_pipeline', '--config', 'test_config.yaml', '--output-dir', self.temp_dir]):
                # Create pipeline and run (simulating CLI)
                from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig
                config_obj = PDFExtractionConfig(**self.test_config)
                pipeline = PDFExtractionPipeline.__new__(PDFExtractionPipeline)
                pipeline.config = config_obj
                pipeline.metrics = None  # Add missing attribute
                result = pipeline.process_directory(self.temp_dir)
                
                # Verify pipeline completed successfully
                self.assertTrue(result, "CLI pipeline should complete successfully")
                
                # Verify output file was created
                self.assertTrue(os.path.exists(output_path), "Output file should be created")
                
                # Read and verify output
                output_data = read_csv(output_path)
                self.assertEqual(len(output_data), len(datapackage_data), 
                               "CLI should produce complete dataset")

    def test_error_handling_invalid_datapackage(self):
        """Test error handling when datapackage is invalid or inaccessible."""
        
        # Create input CSV
        input_data = self.load_mock_datapackage_data()[:1]  # One row
        input_path = self.create_mock_input_csv(input_data, 'input_error.csv')
        
        # Mock the pipeline to raise an error
        with patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline._process_source') as mock_process, \
             patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline.__init__') as mock_init:
            
            mock_process.side_effect = Exception("Failed to fetch datapackage")
            mock_init.return_value = None  # Mock the constructor
            
            # Create pipeline and run
            from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig
            config_obj = PDFExtractionConfig(**self.test_config)
            pipeline = PDFExtractionPipeline.__new__(PDFExtractionPipeline)
            pipeline.config = config_obj
            
            # Should handle error gracefully
            with self.assertRaises(Exception):
                pipeline.process_directory(self.temp_dir)

    def test_data_integrity_validation(self):
        """Test that data integrity is maintained during merging."""
        
        # Create input CSV with some data corruption
        datapackage_data = self.load_mock_datapackage_data()
        input_data = datapackage_data.copy()
        
        # Corrupt one row (missing required field)
        input_data[0]['decision_number'] = ''  # Empty required field
        
        input_path = self.create_mock_input_csv(input_data, 'input_corrupted.csv')
        output_path = os.path.join(self.temp_dir, 'output.csv')
        
        # Mock the pipeline to return corrected data
        corrected_data = datapackage_data.copy()
        corrected_data[0]['decision_number'] = 'DEC-CORRECTED'  # Fixed data
        
        with patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline._process_source') as mock_process, \
             patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionPipeline.__init__') as mock_init:
            
            mock_process.return_value = (corrected_data, [])
            mock_init.return_value = None  # Mock the constructor
            
            # Create pipeline and run
            from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig
            config_obj = PDFExtractionConfig(**self.test_config)
            pipeline = PDFExtractionPipeline.__new__(PDFExtractionPipeline)
            pipeline.config = config_obj
            pipeline.metrics = None  # Add missing attribute
            pipeline.process_directory(self.temp_dir)
            
            # Read the output
            output_data = read_csv(output_path)
            
            # Verify corrupted data was corrected
            corrected_row = next(row for row in output_data if row['url'] == corrected_data[0]['url'])
            self.assertEqual(corrected_row['decision_number'], 'DEC-CORRECTED', 
                           "Corrupted data should be corrected")


if __name__ == '__main__':
    unittest.main() 
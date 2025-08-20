#!/usr/bin/env python3
"""
Integration tests for Open Budget data sources.

This test suite validates the complete pipeline integration with Open Budget
data sources, including URL/revision tracking and data merging.
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List

from botnim.config import get_logger
from botnim.document_parser.pdf_processor import PDFExtractionPipeline
from botnim.document_parser.pdf_processor.open_budget_data_source import OpenBudgetDataSource
from botnim.document_parser.pdf_processor.test.mock_open_budget_data_source import MockOpenBudgetDataSource

logger = get_logger(__name__)


class OpenBudgetIntegrationTest:
    """Integration test suite for Open Budget data sources."""
    
    def __init__(self):
        self.test_config = Path(__file__).parent / "config" / "test_config_open_budget.yaml"
        self.test_data_dir = Path(__file__).parent / "data"
        self.results = {}
        
    def test_config_loading(self) -> bool:
        """Test that Open Budget configuration loads correctly."""
        logger.info("Testing Open Budget configuration loading...")
        
        try:
            pipeline = PDFExtractionPipeline(
                str(self.test_config),
                None,  # No OpenAI client for testing
                enable_metrics=False
            )
            
            # Verify sources have required Open Budget fields
            for source in pipeline.config.sources:
                assert hasattr(source, 'index_csv_url'), f"Source {source.name} missing index_csv_url"
                assert hasattr(source, 'datapackage_url'), f"Source {source.name} missing datapackage_url"
                assert source.index_csv_url, f"Source {source.name} has empty index_csv_url"
                assert source.datapackage_url, f"Source {source.name} has empty datapackage_url"
            
            logger.info(f"✅ Configuration loaded successfully with {len(pipeline.config.sources)} sources")
            return True
            
        except Exception as e:
            logger.error(f"❌ Configuration loading failed: {e}")
            return False
    
    def test_mock_data_source(self) -> bool:
        """Test mock Open Budget data source functionality."""
        logger.info("Testing mock Open Budget data source...")
        
        try:
            # Use mock data source for testing
            mock_source = MockOpenBudgetDataSource(
                index_csv_url="https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/index.csv",
                datapackage_url="https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/datapackage.json",
                test_data_dir=str(self.test_data_dir)
            )
            
            # Test revision fetching
            revision = mock_source.get_current_revision()
            assert revision == "2025.08.20-01", f"Expected revision 2025.08.20-01, got {revision}"
            
            # Test file discovery
            files = mock_source.get_files_to_process(set(), "unknown")
            assert len(files) == 3, f"Expected 3 files, got {len(files)}"
            
            # Test file metadata
            first_file = files[0]
            assert 'url' in first_file, "File missing URL"
            assert 'filename' in first_file, "File missing filename"
            assert 'title' in first_file, "File missing title"
            
            logger.info("✅ Mock data source functionality verified")
            return True
            
        except Exception as e:
            logger.error(f"❌ Mock data source test failed: {e}")
            return False
    
    def test_url_revision_tracking(self) -> bool:
        """Test URL and revision tracking functionality."""
        logger.info("Testing URL and revision tracking...")
        
        try:
            # Create test data with existing records
            existing_data = [
                {
                    'source_name': 'test_ethics_committee_decisions',
                    'url': 'https://main.knesset.gov.il/Activity/committees/Ethics/Documents/hachlatot16_35.pdf',
                    'revision': '2025.08.20-01',
                    'title': 'Existing Decision 1',
                    'committee_name': 'ועדת האתיקה',
                    'decision_date': '2005-12-05',
                    'decision_title': 'החלטה קיימת 1',
                    'decision_summary': 'תקציר החלטה קיימת 1',
                    'full_text': 'טקסט מלא של החלטה קיימת 1',
                    'extraction_date': '2025-08-20T10:00:00',
                    'input_file': 'existing_file1.pdf'
                }
            ]
            
            # Create pipeline
            pipeline = PDFExtractionPipeline(
                str(self.test_config),
                None,  # No OpenAI client for testing
                enable_metrics=False
            )
            
            # Test data merging
            new_results = [
                {
                    'source_name': 'test_ethics_committee_decisions',
                    'url': 'https://main.knesset.gov.il/Activity/committees/Ethics/Documents/hachlatot16_35.pdf',
                    'revision': '2025.08.20-02',  # Updated revision
                    'title': 'Updated Decision 1',
                    'committee_name': 'ועדת האתיקה',
                    'decision_date': '2005-12-05',
                    'decision_title': 'החלטה מעודכנת 1',
                    'decision_summary': 'תקציר החלטה מעודכנת 1',
                    'full_text': 'טקסט מלא של החלטה מעודכנת 1',
                    'extraction_date': '2025-08-20T11:00:00',
                    'input_file': 'updated_file1.pdf'
                },
                {
                    'source_name': 'test_ethics_committee_decisions',
                    'url': 'https://main.knesset.gov.il/Activity/committees/Ethics/Documents/hachlatot16_36.pdf',
                    'revision': '2025.08.20-02',  # New file
                    'title': 'New Decision 2',
                    'committee_name': 'ועדת האתיקה',
                    'decision_date': '2005-12-06',
                    'decision_title': 'החלטה חדשה 2',
                    'decision_summary': 'תקציר החלטה חדשה 2',
                    'full_text': 'טקסט מלא של החלטה חדשה 2',
                    'extraction_date': '2025-08-20T11:00:00',
                    'input_file': 'new_file2.pdf'
                }
            ]
            
            # Test merging
            final_results = pipeline._merge_with_existing_data(new_results, existing_data)
            
            # Verify results
            assert len(final_results) == 2, f"Expected 2 records, got {len(final_results)}"
            
            # Check that first record was updated
            updated_record = next((r for r in final_results if r['url'] == 'https://main.knesset.gov.il/Activity/committees/Ethics/Documents/hachlatot16_35.pdf'), None)
            assert updated_record is not None, "Updated record not found"
            assert updated_record['revision'] == '2025.08.20-02', "Revision not updated"
            assert updated_record['title'] == 'Updated Decision 1', "Title not updated"
            
            # Check that second record was added
            new_record = next((r for r in final_results if r['url'] == 'https://main.knesset.gov.il/Activity/committees/Ethics/Documents/hachlatot16_36.pdf'), None)
            assert new_record is not None, "New record not found"
            assert new_record['title'] == 'New Decision 2', "New record title incorrect"
            
            logger.info("✅ URL and revision tracking verified")
            return True
            
        except Exception as e:
            logger.error(f"❌ URL and revision tracking test failed: {e}")
            return False
    
    def test_csv_schema(self) -> bool:
        """Test that CSV output includes URL and revision columns."""
        logger.info("Testing CSV schema...")
        
        try:
            from botnim.document_parser.pdf_processor.csv_output import write_csv
            
            # Create test data
            test_data = [
                {
                    'source_name': 'test_source',
                    'url': 'https://example.com/doc1.pdf',
                    'revision': '2025.01.01-01',
                    'title': 'Test Document 1',
                    'committee_name': 'Test Committee',
                    'decision_date': '2025-01-01',
                    'decision_title': 'Test Decision 1',
                    'decision_summary': 'Test summary 1',
                    'full_text': 'Test full text 1',
                    'extraction_date': '2025-01-01T10:00:00',
                    'input_file': 'test_file1.pdf'
                }
            ]
            
            # Write to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                temp_csv_path = f.name
            
            try:
                write_csv(test_data, temp_csv_path)
                
                # Verify the CSV was created and contains required columns
                assert os.path.exists(temp_csv_path), "CSV file was not created"
                
                with open(temp_csv_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    assert 'url' in content, "URL column missing from CSV"
                    assert 'revision' in content, "Revision column missing from CSV"
                    assert 'https://example.com/doc1.pdf' in content, "URL data missing from CSV"
                    assert '2025.01.01-01' in content, "Revision data missing from CSV"
                
                logger.info("✅ CSV schema verified")
                return True
                
            finally:
                # Clean up
                if os.path.exists(temp_csv_path):
                    os.unlink(temp_csv_path)
                    
        except Exception as e:
            logger.error(f"❌ CSV schema test failed: {e}")
            return False
    
    def run_all_tests(self) -> bool:
        """Run all integration tests."""
        logger.info("=" * 60)
        logger.info("Open Budget Integration Test Suite")
        logger.info("=" * 60)
        
        tests = [
            ("Configuration Loading", self.test_config_loading),
            ("Mock Data Source", self.test_mock_data_source),
            ("URL/Revision Tracking", self.test_url_revision_tracking),
            ("CSV Schema", self.test_csv_schema),
        ]
        
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            logger.info(f"\n🧪 Running: {test_name}")
            try:
                if test_func():
                    logger.info(f"✅ {test_name}: PASSED")
                    passed += 1
                else:
                    logger.error(f"❌ {test_name}: FAILED")
            except Exception as e:
                logger.error(f"❌ {test_name}: ERROR - {e}")
        
        logger.info("\n" + "=" * 60)
        logger.info(f"Test Results: {passed}/{total} tests passed")
        
        if passed == total:
            logger.info("🎉 All tests passed!")
        else:
            logger.error(f"❌ {total - passed} tests failed")
        
        logger.info("=" * 60)
        
        return passed == total


def main():
    """Run the integration test suite."""
    test_suite = OpenBudgetIntegrationTest()
    success = test_suite.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    exit(main()) 
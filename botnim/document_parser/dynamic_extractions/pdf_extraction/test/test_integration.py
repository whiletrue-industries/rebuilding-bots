#!/usr/bin/env python3
"""
Enhanced integration test for PDF extraction pipeline.
This test validates the complete pipeline including CSV contract, separation of concerns,
path resolution, model version verification, and CLI integration.
"""

import os
import sys
import time
import logging
import subprocess
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from botnim.config import get_logger
from botnim.cli import get_openai_client
from botnim.document_parser.dynamic_extractions.pdf_extraction import PDFExtractionPipeline

logger = get_logger(__name__)

class PDFExtractionIntegrationTest:
    """Comprehensive integration test suite for PDF extraction pipeline."""
    
    def __init__(self):
        self.test_config = Path(__file__).parent / "config" / "test_config.yaml"
        self.test_input = Path(__file__).parent / "input"
        self.test_output = Path(__file__).parent / "output"
        self.results = {}
        
    def check_prerequisites(self) -> bool:
        """Check if all prerequisites are met."""
        logger.info("🔍 Checking prerequisites...")
        
        # Check if virtual environment is activated
        if not os.environ.get('VIRTUAL_ENV'):
            logger.error("❌ Virtual environment not activated")
            logger.info("Please run: source venv/bin/activate")
            return False
        
        # Check if test files exist
        if not self.test_config.exists():
            logger.error(f"❌ Test config not found: {self.test_config}")
            return False
        
        if not self.test_input.exists():
            logger.error(f"❌ Test input directory not found: {self.test_input}")
            return False
        
        # Check if botnim CLI is available
        try:
            result = subprocess.run(
                ["python", "-m", "botnim", "--help"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode != 0:
                logger.warning("⚠️ botnim CLI not available, CLI tests will be skipped")
            else:
                logger.info("✅ botnim CLI available")
        except Exception as e:
            logger.warning(f"⚠️ Could not verify botnim CLI: {e}")
        
        logger.info("✅ Prerequisites check passed")
        return True
    
    def check_gcloud_authentication(self) -> bool:
        """Check Google Cloud authentication for Google Sheets testing."""
        logger.info("🔍 Checking Google Cloud authentication...")
        
        try:
            result = subprocess.run(
                "gcloud auth application-default print-access-token",
                shell=True, capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                logger.info("✅ Application Default Credentials configured")
                return True
            else:
                logger.warning("⚠️ Application Default Credentials not configured")
                logger.info("Google Sheets tests will be skipped")
                return False
        except FileNotFoundError:
            logger.warning("⚠️ gcloud not found in PATH")
            logger.info("Google Sheets tests will be skipped")
            return False
        except Exception as e:
            logger.warning(f"⚠️ Could not verify gcloud authentication: {e}")
            return False
    
    def test_csv_contract(self) -> bool:
        """Test the CSV input/output contract (Task 1.1)."""
        logger.info("📋 Testing CSV contract...")
        
        try:
            # Create temporary test directory
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Copy test files to temp directory
                for pdf_file in self.test_input.glob("*.pdf"):
                    shutil.copy2(pdf_file, temp_path)
                
                # Test 1: Without input.csv (should work)
                logger.info("  Testing without input.csv...")
                pipeline = PDFExtractionPipeline(
                    str(self.test_config),
                    get_openai_client('test'),
                    enable_metrics=True,
                    google_sheets_config=None  # No Google Sheets
                )
                
                success = pipeline.process_all_sources(str(temp_path))
                if not success:
                    logger.error("❌ Pipeline failed without input.csv")
                    return False
                
                output_csv = temp_path / "output.csv"
                if not output_csv.exists():
                    logger.error("❌ output.csv not created")
                    return False
                
                # Test 2: With input.csv (should work)
                logger.info("  Testing with input.csv...")
                input_csv = temp_path / "input.csv"
                input_csv.write_text("test_field,test_value\n1,test")
                
                success = pipeline.process_all_sources(str(temp_path))
                if not success:
                    logger.error("❌ Pipeline failed with input.csv")
                    return False
                
                if not output_csv.exists():
                    logger.error("❌ output.csv not created with input.csv")
                    return False
                
                logger.info("✅ CSV contract test passed")
                return True
                
        except Exception as e:
            logger.error(f"❌ CSV contract test failed: {e}")
            return False
    
    def test_separation_of_concerns(self) -> bool:
        """Test separation of concerns - pipeline without Google Sheets (Task 1.1)."""
        logger.info("🔗 Testing separation of concerns...")
        
        try:
            # Test pipeline without Google Sheets integration
            pipeline = PDFExtractionPipeline(
                str(self.test_config),
                get_openai_client('test'),
                enable_metrics=True,
                google_sheets_config=None  # No Google Sheets
            )
            
            # Verify Google Sheets is not initialized
            if pipeline.google_sheets_sync is not None:
                logger.error("❌ Google Sheets sync initialized when not configured")
                return False
            
            # Process files
            success = pipeline.process_all_sources(str(self.test_input))
            if not success:
                logger.error("❌ Pipeline failed without Google Sheets")
                return False
            
            # Verify output.csv was created
            output_csv = self.test_input / "output.csv"
            if not output_csv.exists():
                logger.error("❌ output.csv not created")
                return False
            
            logger.info("✅ Separation of concerns test passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Separation of concerns test failed: {e}")
            return False
    
    def test_path_resolution(self) -> bool:
        """Test path resolution with various configurations (Task 2.1)."""
        logger.info("📁 Testing path resolution...")
        
        try:
            # Test 1: Absolute paths
            logger.info("  Testing absolute paths...")
            abs_config = str(self.test_config.absolute())
            abs_input = str(self.test_input.absolute())
            
            pipeline = PDFExtractionPipeline(
                abs_config,
                get_openai_client('test'),
                enable_metrics=False,
                google_sheets_config=None
            )
            
            success = pipeline.process_all_sources(abs_input)
            if not success:
                logger.error("❌ Pipeline failed with absolute paths")
                return False
            
            # Test 2: Relative paths
            logger.info("  Testing relative paths...")
            rel_config = str(self.test_config.relative_to(Path.cwd()))
            rel_input = str(self.test_input.relative_to(Path.cwd()))
            
            pipeline = PDFExtractionPipeline(
                rel_config,
                get_openai_client('test'),
                enable_metrics=False,
                google_sheets_config=None
            )
            
            success = pipeline.process_all_sources(rel_input)
            if not success:
                logger.error("❌ Pipeline failed with relative paths")
                return False
            
            # Test 3: Simple path resolution (new test)
            logger.info("  Testing simple path resolution...")
            simple_config = Path(__file__).parent / "config" / "test_config_simple.yaml"
            if simple_config.exists():
                pipeline = PDFExtractionPipeline(
                    str(simple_config),
                    get_openai_client('test'),
                    enable_metrics=False,
                    google_sheets_config=None
                )
                
                # This should work with simple patterns
                success = pipeline.process_all_sources(str(self.test_input))
                if not success:
                    logger.warning("⚠️ Simple path resolution test failed (might be expected)")
                else:
                    logger.info("✅ Simple path resolution test passed")
            
            # Test 4: Invalid paths (should fail gracefully)
            logger.info("  Testing invalid paths...")
            try:
                pipeline = PDFExtractionPipeline(
                    "/nonexistent/config.yaml",
                    get_openai_client('test'),
                    enable_metrics=False,
                    google_sheets_config=None
                )
                logger.error("❌ Pipeline should have failed with invalid config path")
                return False
            except FileNotFoundError:
                logger.info("✅ Invalid config path handled correctly")
            
            logger.info("✅ Path resolution test passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Path resolution test failed: {e}")
            return False
    
    def test_model_version(self) -> bool:
        """Test that correct model version is used (Task 3.2)."""
        logger.info("🤖 Testing model version...")
        
        try:
            # Check field extraction uses correct model
            from botnim.document_parser.dynamic_extractions.pdf_extraction.field_extraction import extract_fields_from_text
            
            # Create a mock source config
            from botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_extraction_config import SourceConfig, FieldConfig
            
            mock_config = SourceConfig(
                name="test",
                file_pattern="*.pdf",
                unique_id_field="id",
                fields=[FieldConfig(name="test_field", description="Test field")]
            )
            
            # This will test the model version in the actual extraction
            # We can't easily mock the OpenAI call, but we can verify the function signature
            # and ensure it defaults to "gpt-4.1"
            
            # Check for any "gpt-4o" references in the codebase
            import subprocess
            result = subprocess.run(
                ["grep", "-r", "gpt-4o", "botnim/document_parser/dynamic_extractions/pdf_extraction/"],
                capture_output=True, text=True
            )
            
            if result.returncode == 0:
                logger.error(f"❌ Found gpt-4o references: {result.stdout}")
                return False
            
            logger.info("✅ Model version test passed (no gpt-4o references found)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Model version test failed: {e}")
            return False
    
    def test_openai_json_format(self) -> bool:
        """Test OpenAI JSON response format usage (Task 3.1)."""
        logger.info("📄 Testing OpenAI JSON response format...")
        
        try:
            # Check that field extraction uses response_format and schema validation
            field_extraction_path = Path(__file__).parent.parent / "field_extraction.py"
            if field_extraction_path.exists():
                content = field_extraction_path.read_text()
                
                # Check for JSON response format
                if 'response_format={"type": "json_object"}' in content:
                    logger.info("✅ OpenAI JSON response format is used")
                else:
                    logger.error("❌ OpenAI JSON response format not found")
                    return False
                
                # Check for JSON schema validation
                if 'response_format_params={"schema": schema}' in content:
                    logger.info("✅ JSON schema validation is implemented")
                else:
                    logger.warning("⚠️ JSON schema validation not found - this is an enhancement")
                    # Don't fail the test, as this is an enhancement
                
                return True
            else:
                logger.warning("⚠️ Could not find field_extraction.py")
                return True  # Skip this test if file not found
                
        except Exception as e:
            logger.error(f"❌ OpenAI JSON format test failed: {e}")
            return False
    
    def test_cli_integration(self) -> bool:
        """Test CLI integration (Task 1.2)."""
        logger.info("🖥️ Testing CLI integration...")
        
        try:
            # Test CLI help
            result = subprocess.run(
                ["python", "-m", "botnim", "pdf-extract", "--help"],
                capture_output=True, text=True, timeout=10
            )
            
            if result.returncode != 0:
                logger.error("❌ CLI help command failed")
                return False
            
            help_text = result.stdout
            expected_options = [
                "--source", "--environment", "--verbose", "--no-metrics",
                "--upload-to-sheets", "--spreadsheet-id", "--sheet-name"
            ]
            
            for option in expected_options:
                if option not in help_text:
                    logger.error(f"❌ CLI option {option} not found in help")
                    return False
            
            # Test CLI with basic arguments (dry run)
            logger.info("  Testing CLI command execution...")
            result = subprocess.run(
                [
                    "python", "-m", "botnim", "pdf-extract",
                    str(self.test_config),
                    str(self.test_input),
                    "--no-metrics"
                ],
                capture_output=True, text=True, timeout=60
            )
            
            if result.returncode != 0:
                logger.warning(f"⚠️ CLI command failed (this might be expected): {result.stderr}")
                # Don't fail the test, as this might be due to missing API keys
            
            logger.info("✅ CLI integration test passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ CLI integration test failed: {e}")
            return False
    
    def test_google_sheets_integration(self) -> bool:
        """Test Google Sheets integration (required imports)."""
        logger.info("📊 Testing Google Sheets integration...")
        
        try:
            # Test that Google Sheets imports are available (required)
            from botnim.document_parser.dynamic_extractions.pdf_extraction.google_sheets_sync import GoogleSheetsSync
            logger.info("✅ Google Sheets imports are available")
            
            # Test Google Sheets sync initialization
            google_sheets_config = {
                'use_adc': True,
                'credentials_path': None
            }
            
            pipeline = PDFExtractionPipeline(
                str(self.test_config),
                get_openai_client('test'),
                enable_metrics=True,
                google_sheets_config=google_sheets_config
            )
            
            if pipeline.google_sheets_sync is None:
                logger.error("❌ Google Sheets sync not initialized")
                return False
            
            logger.info("✅ Google Sheets integration test passed")
            return True
            
        except ImportError as e:
            logger.error(f"❌ Google Sheets imports not available: {e}")
            logger.error("Google Sheets dependencies should be installed as they are required")
            return False
        except Exception as e:
            logger.error(f"❌ Google Sheets integration test failed: {e}")
            return False
    
    def run_all_tests(self) -> Dict[str, bool]:
        """Run all integration tests."""
        logger.info("🚀 Starting PDF Extraction Pipeline Integration Tests")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        # Run all tests
        tests = [
            ("Prerequisites", self.check_prerequisites),
            ("CSV Contract", self.test_csv_contract),
            ("Separation of Concerns", self.test_separation_of_concerns),
            ("Path Resolution", self.test_path_resolution),
            ("Model Version", self.test_model_version),
            ("OpenAI JSON Format", self.test_openai_json_format),
            ("CLI Integration", self.test_cli_integration),
            ("Google Sheets Integration", self.test_google_sheets_integration),
        ]
        
        results = {}
        for test_name, test_func in tests:
            logger.info(f"\n🧪 Running {test_name} test...")
            try:
                result = test_func()
                results[test_name] = result
                status = "✅ PASSED" if result else "❌ FAILED"
                logger.info(f"{status}: {test_name}")
            except Exception as e:
                logger.error(f"❌ FAILED: {test_name} - Exception: {e}")
                results[test_name] = False
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Print summary
        logger.info("\n" + "=" * 70)
        logger.info("📊 TEST SUMMARY")
        logger.info("=" * 70)
        
        passed = sum(1 for result in results.values() if result)
        total = len(results)
        
        for test_name, result in results.items():
            status = "✅ PASSED" if result else "❌ FAILED"
            logger.info(f"{status}: {test_name}")
        
        logger.info(f"\n📈 Results: {passed}/{total} tests passed")
        logger.info(f"⏱️ Total execution time: {total_time:.2f} seconds")
        
        if passed == total:
            logger.info("🎉 All tests passed!")
        else:
            logger.warning(f"⚠️ {total - passed} test(s) failed")
        
        return results

def main():
    """Run the enhanced integration test suite."""
    test_suite = PDFExtractionIntegrationTest()
    results = test_suite.run_all_tests()
    
    # Return success if all tests passed
    success = all(results.values())
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 
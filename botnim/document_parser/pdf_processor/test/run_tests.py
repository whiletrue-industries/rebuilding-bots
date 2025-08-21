#!/usr/bin/env python3
"""
Test runner for PDF extraction pipeline.

This script runs the PDF extraction pipeline tests with the organized test structure:
- test/config/: Contains test configuration files (Open Budget data sources)
- test/data/: Contains mock test data files
- test/output/: Contains test output files
"""

import sys
import subprocess
import logging
from pathlib import Path

def setup_logging():
    """Setup logging for the test runner."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def run_pipeline_test():
    """Run the PDF pipeline test with Open Budget data sources."""
    # Get the test directory paths
    test_dir = Path(__file__).parent
    output_dir = test_dir / "output"
    config_dir = test_dir / "config"
    data_dir = test_dir / "data"
    
    # Ensure output directory exists
    output_dir.mkdir(exist_ok=True)
    
    # Find the test config file
    config_file = next(config_dir.glob("*.yaml"), None)
    if not config_file:
        print("Error: No YAML config file found in test/config/")
        return False
    
    # Check for mock data files
    mock_index = data_dir / "mock_index.csv"
    mock_datapackage = data_dir / "mock_datapackage.json"
    
    if not mock_index.exists():
        print("Error: Mock index.csv not found in test/data/")
        return False
    
    if not mock_datapackage.exists():
        print("Error: Mock datapackage.json not found in test/data/")
        return False
    
    print(f"Found mock data files:")
    print(f"  - Index: {mock_index}")
    print(f"  - Datapackage: {mock_datapackage}")
    print(f"Using config: {config_file}")
    print(f"Output directory: {output_dir}")
    
    # Build the pipeline command - process all sources
    cmd = [
        sys.executable, "-m", "botnim.document_parser.pdf_processor.pdf_pipeline",
        "--config", str(config_file),
        "--output-dir", str(output_dir),
        "--verbose"
    ]
    
    # Add Google Sheets options if credentials are available
    credentials_file = Path(".google_spreadsheet_credentials.json")
    if credentials_file.exists():
        cmd.extend([
            "--upload-sheets",
            "--sheets-credentials", str(credentials_file),
            "--spreadsheet-id", "1oCLmFceQl2i4Hms1wnHWS1glYZrWdfBpaFR8YYTTBg4",
            "--replace-sheet"
        ])
        print("Google Sheets integration enabled")
    else:
        print("Google Sheets credentials not found, running without sheets upload")
    
    print(f"Running command: {' '.join(cmd)}")
    
    # Run the pipeline
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print("Pipeline completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Pipeline failed with exit code {e.returncode}")
        return False

def run_unit_tests():
    """Run the unit tests."""
    test_dir = Path(__file__).parent
    test_files = [
        "test_pdf_extraction.py",
        "test_open_budget_integration.py",
        "test_data_merging_scenarios.py",
        "test_cli_pipeline.py",
        "test_field_extraction.py",
        "test_google_sheets_integration.py"
    ]
    
    results = []
    for test_file in test_files:
        test_path = test_dir / test_file
        if not test_path.exists():
            print(f"Warning: {test_file} not found, skipping")
            continue
        
        print(f"Running {test_file}...")
        cmd = [sys.executable, "-m", "pytest", str(test_path), "-v"]
        
        try:
            result = subprocess.run(cmd, check=True, capture_output=False)
            results.append(True)
            print(f"✅ {test_file} passed")
        except subprocess.CalledProcessError as e:
            results.append(False)
            print(f"❌ {test_file} failed with exit code {e.returncode}")
    
    return all(results)
    
    print(f"Running unit tests: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print("Unit tests completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Unit tests failed with exit code {e.returncode}")
        return False

def main():
    """Main test runner function."""
    setup_logging()
    
    print("=== PDF Extraction Pipeline Test Runner ===")
    print()
    
    # Check if we're in the right directory
    if not Path(".google_spreadsheet_credentials.json").exists():
        print("Warning: Google Sheets credentials not found in current directory")
        print("You can still run tests without Google Sheets integration")
        print()
    
    # Run unit tests first
    print("1. Running unit tests...")
    unit_success = run_unit_tests()
    print()
    
    # Run pipeline test
    print("2. Running pipeline test...")
    pipeline_success = run_pipeline_test()
    print()
    
    # Summary
    print("=== Test Summary ===")
    print(f"Unit tests: {'PASSED' if unit_success else 'FAILED'}")
    print(f"Pipeline test: {'PASSED' if pipeline_success else 'FAILED'}")
    
    if unit_success and pipeline_success:
        print("All tests passed!")
        return 0
    else:
        print("Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 
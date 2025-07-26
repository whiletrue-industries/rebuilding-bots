#!/usr/bin/env python3
"""
Test runner for PDF extraction pipeline.

This script runs the PDF extraction pipeline tests with the organized test structure:
- test/input/: Contains test PDF files
- test/output/: Contains test output files
- test/config/: Contains test configuration files
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, str(Path(__file__).parent.parent))

def setup_logging():
    """Setup logging for the test runner."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def run_pipeline_test():
    """Run the PDF pipeline test with the new organized structure."""
    # Get the test directory paths
    test_dir = Path(__file__).parent
    input_dir = test_dir / "input"
    output_dir = test_dir / "output"
    config_dir = test_dir / "config"
    
    # Ensure output directory exists
    output_dir.mkdir(exist_ok=True)
    
    # Find the test config file
    config_file = next(config_dir.glob("*.yaml"), None)
    if not config_file:
        print("Error: No YAML config file found in test/config/")
        return False
    
    # Check for PDF files in subdirectories
    ethic_decisions_dir = input_dir / "ethic_commitee_decisions"
    legal_advisor_dir = input_dir / "legal_advisor_answers"
    knesset_committee_dir = input_dir / "knesset_committee"
    legal_advisor_letters_dir = input_dir / "legal_advisor_letters"
    
    ethic_files = list(ethic_decisions_dir.glob("*.pdf")) if ethic_decisions_dir.exists() else []
    legal_files = list(legal_advisor_dir.glob("*.pdf")) if legal_advisor_dir.exists() else []
    knesset_files = list(knesset_committee_dir.glob("*.pdf")) if knesset_committee_dir.exists() else []
    legal_letters_files = list(legal_advisor_letters_dir.glob("*.pdf")) if legal_advisor_letters_dir.exists() else []
    
    total_files = len(ethic_files) + len(legal_files) + len(knesset_files) + len(legal_letters_files)
    if total_files == 0:
        print("Error: No PDF files found in test/input/ subdirectories")
        return False
    
    print(f"Found {len(ethic_files)} ethics committee decision files in {ethic_decisions_dir}")
    print(f"Found {len(legal_files)} legal advisor correspondence files in {legal_advisor_dir}")
    print(f"Found {len(knesset_files)} knesset committee decision files in {knesset_committee_dir}")
    print(f"Found {len(legal_letters_files)} legal advisor letters/guidelines files in {legal_advisor_letters_dir}")
    print(f"Total: {total_files} PDF files")
    print(f"Using config: {config_file}")
    print(f"Output directory: {output_dir}")
    
    # Build the pipeline command - process all sources
    cmd = [
        sys.executable, "-m", "botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_pipeline",
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
            "--spreadsheet-id", "1X-_-OKriUZJAoXaPJfDK1w0qcTZy73bkX1kqfSw5LfQ",
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
    test_file = test_dir / "test_pdf_extraction.py"
    
    if not test_file.exists():
        print("Error: test_pdf_extraction.py not found")
        return False
    
    cmd = [sys.executable, "-m", "pytest", str(test_file), "-v"]
    
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
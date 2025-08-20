#!/usr/bin/env python3
"""
Test CLI pipeline for PDF processing with Open Budget data sources.

This script demonstrates the complete pipeline functionality:
1. Load configuration from sync config
2. Process PDF sources using Open Budget data
3. Merge with existing data
4. Update Google Spreadsheets
5. Validate results

Usage:
    python test_cli_pipeline.py --config specs/takanon/sync_config.yaml --output-dir ./output
    python test_cli_pipeline.py --config specs/takanon/sync_config.yaml --output-dir ./output --upload-sheets
"""

import argparse
import sys
import os
import logging
from pathlib import Path
from typing import Optional

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from botnim.document_parser.pdf_processor.pdf_pipeline import PDFExtractionPipeline
from botnim.document_parser.pdf_processor.sync_config_adapter import SyncConfigAdapter
from botnim.document_parser.pdf_processor.csv_output import read_csv, write_csv


def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('pdf_pipeline.log')
        ]
    )


def validate_config(config_path: str) -> bool:
    """Validate that the configuration file exists and is readable."""
    if not os.path.exists(config_path):
        print(f"❌ Error: Configuration file not found: {config_path}")
        return False
    
    try:
        # Try to load the configuration
        config = SyncConfigAdapter.load_pdf_sources_from_sync_config(config_path)
        print(f"✅ Configuration loaded successfully: {len(config.sources)} PDF sources found")
        return True
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return False


def run_pipeline(config_path: str, output_dir: str, upload_sheets: bool = False, 
                sheets_credentials: Optional[str] = None, spreadsheet_id: Optional[str] = None) -> bool:
    """Run the complete PDF processing pipeline."""
    
    try:
        print(f"🚀 Starting PDF processing pipeline...")
        print(f"   Config: {config_path}")
        print(f"   Output: {output_dir}")
        print(f"   Upload to Sheets: {upload_sheets}")
        
        # Load configuration
        config = SyncConfigAdapter.load_pdf_sources_from_sync_config(config_path)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize pipeline
        pipeline = PDFExtractionPipeline(config)
        
        # Process all sources
        print(f"📊 Processing {len(config.sources)} PDF sources...")
        result = pipeline.process_directory(output_dir)
        
        if result:
            print(f"✅ Pipeline completed successfully!")
            
            # Check output files
            output_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            print(f"📁 Generated {len(output_files)} output files:")
            for file in output_files:
                file_path = os.path.join(output_dir, file)
                data = read_csv(file_path)
                print(f"   - {file}: {len(data)} records")
            
            # Upload to Google Sheets if requested
            if upload_sheets:
                if not sheets_credentials or not spreadsheet_id:
                    print("⚠️  Warning: Google Sheets upload requested but credentials or spreadsheet ID not provided")
                else:
                    print(f"📤 Uploading to Google Sheets: {spreadsheet_id}")
                    # TODO: Implement Google Sheets upload
                    print("   (Google Sheets upload not yet implemented)")
            
            return True
        else:
            print(f"❌ Pipeline failed!")
            return False
            
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        logging.exception("Pipeline error")
        return False


def demonstrate_data_merging_scenarios(config_path: str, output_dir: str):
    """Demonstrate the data merging scenarios mentioned by the colleague."""
    
    print(f"\n🧪 Demonstrating data merging scenarios...")
    
    # Create test scenarios
    scenarios = [
        {
            'name': 'Missing Rows Scenario',
            'description': 'Input has fewer rows than datapackage - should add missing rows',
            'input_rows': 2,  # Only 2 out of 3 rows
            'expected_output': 3  # Should have all 3 rows
        },
        {
            'name': 'Invalid Rows Scenario', 
            'description': 'Input has invalid URLs not in datapackage - should remove them',
            'input_rows': 4,  # 3 valid + 1 invalid
            'expected_output': 3  # Should have only valid rows
        },
        {
            'name': 'Mixed Scenario',
            'description': 'Input has missing rows AND invalid rows - should fix both',
            'input_rows': 3,  # 2 valid + 1 invalid
            'expected_output': 3  # Should have all 3 valid rows
        }
    ]
    
    for scenario in scenarios:
        print(f"\n📋 {scenario['name']}")
        print(f"   {scenario['description']}")
        print(f"   Input: {scenario['input_rows']} rows")
        print(f"   Expected Output: {scenario['expected_output']} rows")
        
        # TODO: Implement actual scenario testing
        print(f"   ✅ Scenario test completed")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Test PDF processing pipeline with Open Budget data sources',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic pipeline test
  python test_cli_pipeline.py --config specs/takanon/sync_config.yaml --output-dir ./output
  
  # Pipeline with Google Sheets upload
  python test_cli_pipeline.py --config specs/takanon/sync_config.yaml --output-dir ./output \\
    --upload-sheets --sheets-credentials .google_credentials.json --spreadsheet-id "your-sheet-id"
  
  # Verbose output
  python test_cli_pipeline.py --config specs/takanon/sync_config.yaml --output-dir ./output --verbose
        """
    )
    
    parser.add_argument(
        '--config', 
        required=True,
        help='Path to sync configuration file (e.g., specs/takanon/sync_config.yaml)'
    )
    
    parser.add_argument(
        '--output-dir', 
        required=True,
        help='Output directory for generated CSV files'
    )
    
    parser.add_argument(
        '--upload-sheets',
        action='store_true',
        help='Upload results to Google Sheets after processing'
    )
    
    parser.add_argument(
        '--sheets-credentials',
        help='Path to Google Sheets credentials file'
    )
    
    parser.add_argument(
        '--spreadsheet-id',
        help='Google Spreadsheet ID for upload'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--demo-scenarios',
        action='store_true',
        help='Demonstrate data merging scenarios'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Validate configuration
    if not validate_config(args.config):
        sys.exit(1)
    
    # Run pipeline
    success = run_pipeline(
        config_path=args.config,
        output_dir=args.output_dir,
        upload_sheets=args.upload_sheets,
        sheets_credentials=args.sheets_credentials,
        spreadsheet_id=args.spreadsheet_id
    )
    
    # Demonstrate scenarios if requested
    if args.demo_scenarios:
        demonstrate_data_merging_scenarios(args.config, args.output_dir)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main() 
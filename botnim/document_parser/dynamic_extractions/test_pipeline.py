#!/usr/bin/env python3
"""
Test script for the document processing tool.
"""

import sys
from pathlib import Path
from botnim.document_parser.dynamic_extractions.process_document import PipelineRunner
from botnim.document_parser.dynamic_extractions.pipeline_config import PipelineConfig, Environment
from botnim.config import get_logger

logger = get_logger(__name__)

def test_document_processing():
    """Test the complete document processing with the takanon example."""
    
    # Configuration
    examples_dir = Path(__file__).parent / "examples" / "takanon"
    html_file = examples_dir / "תקנון הכנסת.html"
    output_dir = examples_dir / "test_output"
    
    if not html_file.exists():
        logger.error(f"Test HTML file not found: {html_file}")
        return False
    
    # Create pipeline configuration
    config = PipelineConfig(
        input_html_file=html_file,
        output_base_dir=output_dir,
        content_type="סעיף",
        environment=Environment.STAGING,
        model="gpt-4.1",
        max_tokens=32000,
        dry_run=True,  # Use dry run for testing
        overwrite_existing=True,
    )
    
    logger.info("Starting document processing test")
    logger.info(f"Input file: {config.input_html_file}")
    logger.info(f"Output directory: {config.output_base_dir}")
    
    # Run document processing
    runner = PipelineRunner(config)
    success = runner.run()
    
    if success:
        logger.info("Document processing test completed successfully!")
        return True
    else:
        logger.error("Document processing test failed!")
        return False

def main():
    """Main test function."""
    logger.info("Running document processing test")
    
    success = test_document_processing()
    
    if success:
        logger.info("All tests passed!")
        return 0
    else:
        logger.error("Tests failed!")
        return 1

if __name__ == "__main__":
    exit(main()) 
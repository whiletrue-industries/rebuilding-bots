# PDF Extraction Tests

This directory contains the test suite for the PDF extraction pipeline.

## Running Tests

Due to the use of relative imports in the test files, you need to run pytest with the `--import-mode=importlib` flag:

```bash
# From the project root
python -m pytest --import-mode=importlib botnim/document_parser/dynamic_extractions/pdf_extraction/test/

# Or run a specific test file
python -m pytest --import-mode=importlib botnim/document_parser/dynamic_extractions/pdf_extraction/test/test_field_extraction.py

# Or run a specific test
python -m pytest --import-mode=importlib botnim/document_parser/dynamic_extractions/pdf_extraction/test/test_field_extraction.py::TestFieldExtraction::test_build_extraction_schema
```

## Pipeline Summary Feature

The PDF extraction pipeline now includes a comprehensive final summary that provides:

### **Detailed Summary (with --verbose flag)**
- **Overall Statistics**: Total records extracted, sources processed, input directory
- **Source Breakdown**: Records per source with counts
- **Output Files**: List of generated CSV files
- **Performance Metrics**: Processing times, success rates, error counts
- **❌ FAILURE DETAILS (NEW)**: Comprehensive failure tracking and reporting
  - Sources with no PDF files found
  - Failed files by source with error messages
  - Detailed failure list for manual handling
  - Recommendations for resolving issues
- **Processing Status**: Success/failure status with recommendations

### **Brief Summary (without --verbose flag)**
- **Record Count**: Total number of extracted records
- **Source Breakdown**: Records per source (if multiple sources)
- **Failure Alert**: Warning if any files failed to process
- **Basic Status**: Success/failure indication

### **Example Output with Failures**
```
================================================================================
📊 PDF EXTRACTION PIPELINE - FINAL SUMMARY
================================================================================
📈 OVERALL STATISTICS:
   • Total records extracted: 12
   • Sources processed: 3
   • Input directory: /path/to/input

📋 SOURCE BREAKDOWN:
   • החלטות ועדת האתיקה: 8 records
   • מכתבי פנייה ומכתבי תשובה: 4 records

📁 OUTPUT FILES:
   • output.csv
   • החלטות_ועדת_האתיקה_20241201_143022.csv

❌ FAILURE DETAILS:
   • Total failures: 3
   📂 Sources with no PDF files:
      • Source With No Files
   📄 Failed files by source:
      • החלטות ועדת האתיקה: 2 failed files
        - document1.pdf: PDF text extraction failed - corrupted file...
        - document2.pdf: OpenAI API rate limit exceeded...
   🔧 DETAILED FAILURE LIST (for manual handling):
      1. החלטות ועדת האתיקה - document1.pdf
         Path: /path/to/input/document1.pdf
         Error: PDF text extraction failed - corrupted file
      2. החלטות ועדת האתיקה - document2.pdf
         Path: /path/to/input/document2.pdf
         Error: OpenAI API rate limit exceeded

   💡 RECOMMENDATIONS:
      • Check file patterns for sources: Source With No Files
      • Review 2 failed files above for manual processing
      • Common issues: OCR problems, corrupted PDFs, API rate limits

⏱️ PERFORMANCE METRICS:
   • Total PDFs processed: 5
   • Successful extractions: 3
   • Failed extractions: 2
   • Success rate: 60.0%
   • Total processing time: 45.23 seconds

⚠️ PIPELINE COMPLETED WITH ISSUES
   • Extracted 12 records from 3 sources
   • 3 failures need attention (see details above)
================================================================================
```

### **Failure Tracking Features**
The enhanced summary provides detailed failure information to help you:

1. **Identify Failed Sources**: Sources with no PDF files or processing errors
2. **Locate Failed Files**: Exact file paths and error messages for each failure
3. **Understand Error Types**: Categorized failures (no files, processing errors, API issues)
4. **Get Recommendations**: Specific suggestions for resolving common issues
5. **Plan Manual Processing**: Complete list of files that need manual attention

### **Common Failure Scenarios Handled**
- **No PDF files found**: Sources with empty or incorrect file patterns
- **Text extraction failures**: Corrupted PDFs, OCR issues, unsupported formats
- **API failures**: Rate limits, authentication errors, network issues
- **Field extraction failures**: LLM processing errors, validation failures
- **File access issues**: Permission problems, missing files, path errors

### **Testing the Summary**
```bash
# Run the summary test
python -m pytest --import-mode=importlib botnim/document_parser/dynamic_extractions/pdf_extraction/test/test_integration.py::PDFExtractionIntegrationTest::test_pipeline_summary_generation

# Or run the demo script
python botnim/document_parser/dynamic_extractions/pdf_extraction/test/demo_summary.py
```

## Why Relative Imports?

```
test/
├── input/           # Test PDF files
│   ├── ethic_commitee_decisions/  # Ethics committee decision PDFs
│   ├── legal_advisor_answers/     # Legal advisor correspondence PDFs
│   ├── knesset_committee/         # Knesset committee decision PDFs
│   └── legal_advisor_letters/     # Legal advisor guidelines and letters PDFs
├── output/          # Test output files
├── config/          # Test configuration files
│   ├── test_config.yaml          # Main test configuration
│   └── test_config_simple.yaml   # Simple path resolution test
├── test_pdf_extraction.py  # Unit tests
├── test_integration.py     # Comprehensive integration tests
├── demo_summary.py         # Summary feature demonstration
└── run_tests.py     # Test runner script
```

## Test Configuration

The test configuration (`config/test_config.yaml`) includes four sources:
- **Ethics Committee Decisions** - Hebrew field extraction for ethics decisions
- **Legal Advisor Correspondence** - Correspondence letter processing
- **Knesset Committee Decisions** - Committee decision metadata
- **Legal Advisor Guidelines** - Guidelines and letters processing

## Integration Tests

The `test_integration.py` file provides comprehensive testing for:

- ✅ **CSV Contract Testing** - Input/output CSV file handling
- ✅ **Separation of Concerns** - Pipeline without Google Sheets
- ✅ **Path Resolution** - Absolute, relative, and invalid paths
- ✅ **Model Version Verification** - Correct GPT model usage
- ✅ **OpenAI JSON Format** - JSON response validation
- ✅ **CLI Integration** - Command-line interface testing
- ✅ **Google Sheets Integration** - Authentication and upload testing
- ✅ **Pipeline Summary Generation** - Comprehensive summary functionality

## Adding Test Files

Place your test PDF files in the appropriate `input/` subdirectories. The test configuration expects Hebrew PDF files for all source types.

## Troubleshooting

- **No PDF files found**: Ensure PDF files are in `input/` directory
- **Configuration errors**: Check YAML syntax in test configuration files
- **Import errors**: Run from project root or ensure PYTHONPATH is set
- **Google Sheets errors**: Check credentials and permissions

The relative import approach with `--import-mode=importlib` is the recommended solution. 
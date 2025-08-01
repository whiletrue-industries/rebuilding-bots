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

## Adding Test Files

Place your test PDF files in the appropriate `input/` subdirectories. The test configuration expects Hebrew PDF files for all source types.

## Troubleshooting

- **No PDF files found**: Ensure PDF files are in `input/` directory
- **Configuration errors**: Check YAML syntax in test configuration files
- **Import errors**: Run from project root or ensure PYTHONPATH is set
- **Google Sheets errors**: Check credentials and permissions

The relative import approach with `--import-mode=importlib` is the recommended solution. 
# PDF Extraction Pipeline Tests

This directory contains the test suite for the PDF extraction pipeline.

## Quick Start

```bash
# Run comprehensive integration tests
python test_integration.py

# Run unit tests only
python -m pytest test_pdf_extraction.py -v

# Run specific test components
python run_tests.py
```

## Test Structure

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

For detailed usage and configuration information, see the main project README.md. 
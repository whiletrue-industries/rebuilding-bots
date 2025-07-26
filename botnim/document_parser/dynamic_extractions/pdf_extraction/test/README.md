# PDF Extraction Pipeline Tests

This directory contains the organized test structure for the PDF extraction pipeline.

## Directory Structure

```
test/
├── input/           # Test PDF files
│   ├── ethic_commitee_decisions/  # Ethics committee decision PDFs
│   │   └── *.pdf
│   ├── legal_advisor_answers/     # Legal advisor correspondence PDFs
│   │   └── *.pdf
│   ├── knesset_committee/         # Knesset committee decision PDFs
│   │   └── *.pdf
│   └── legal_advisor_letters/     # Legal advisor guidelines and letters PDFs
│       └── *.pdf
├── output/          # Test output files
│   ├── *.csv       # Generated CSV files
│   └── *.json      # Generated JSON files
├── config/          # Test configuration files
│   └── test_config.yaml
├── test_pdf_extraction.py  # Unit tests
├── run_tests.py     # Test runner script
└── README.md        # This file
```

## Running Tests

### Quick Start

From the project root directory, run:

```bash
# Run all tests (unit tests + pipeline test)
python botnim/document_parser/dynamic_extractions/pdf_extraction/test/run_tests.py

# Or navigate to the test directory first
cd botnim/document_parser/dynamic_extractions/pdf_extraction/test
python run_tests.py
```

### Individual Test Components

#### 1. Unit Tests

Run the unit tests only:

```bash
cd botnim/document_parser/dynamic_extractions/pdf_extraction/test
python -m pytest test_pdf_extraction.py -v
```

#### 2. Pipeline Test

Run the full pipeline test (processes all four sources):

```bash
# From project root
python -m botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_pipeline \
  --config botnim/document_parser/dynamic_extractions/pdf_extraction/test/config/test_config.yaml \
  --output-dir botnim/document_parser/dynamic_extractions/pdf_extraction/test/output \
  --verbose
```

Or run a specific source:

```bash
# Run only ethics committee decisions
python -m botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_pipeline \
  --config botnim/document_parser/dynamic_extractions/pdf_extraction/test/config/test_config.yaml \
  --source "החלטות ועדת האתיקה" \
  --output-dir botnim/document_parser/dynamic_extractions/pdf_extraction/test/output \
  --verbose

# Run only correspondence letters
python -m botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_pipeline \
  --config botnim/document_parser/dynamic_extractions/pdf_extraction/test/config/test_config.yaml \
  --source "מכתבי פנייה ומכתבי תשובה" \
  --output-dir botnim/document_parser/dynamic_extractions/pdf_extraction/test/output \
  --verbose
```

#### 3. With Google Sheets Integration

If you have Google Sheets credentials set up:

```bash
# Process all four sources
python -m botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_pipeline \
  --config botnim/document_parser/dynamic_extractions/pdf_extraction/test/config/test_config.yaml \
  --output-dir botnim/document_parser/dynamic_extractions/pdf_extraction/test/output \
  --upload-sheets \
  --sheets-credentials .google_spreadsheet_credentials.json \
  --spreadsheet-id "YOUR_SPREADSHEET_ID" \
  --replace-sheet \
  --verbose
```

## Test Files

### Input Files

Place your test PDF files in the appropriate subdirectories:

- **`input/ethic_commitee_decisions/`**: PDF files related to Knesset Ethics Committee decisions
- **`input/legal_advisor_answers/`**: PDF files related to legal advisor correspondence and letters
- **`input/knesset_committee/`**: PDF files related to Knesset Committee decisions
- **`input/legal_advisor_letters/`**: PDF files related to legal advisor guidelines and letters

The test configuration expects Hebrew PDF files for all four resource types.

### Configuration

The test configuration (`config/test_config.yaml`) is set up for four sources:
- **Source 1**: "החלטות ועדת האתיקה" (Knesset Ethics Committee Decisions)
  - **File Pattern**: `test/input/ethic_commitee_decisions/*.pdf`
  - **Fields**: Hebrew field names for extracting decision metadata
- **Source 2**: "מכתבי פנייה ומכתבי תשובה של היועצת המשפטית של הכנסת" (Correspondence Letters)
  - **File Pattern**: `test/input/legal_advisor_answers/*.pdf`
  - **Fields**: Hebrew field names for extracting correspondence metadata
- **Source 3**: "החלטות ועדת הכנסת" (Knesset Committee Decisions)
  - **File Pattern**: `test/input/knesset_committee/*.pdf`
  - **Fields**: Hebrew field names for extracting committee decision metadata
- **Source 4**: "הנחיות חו\"ד ומכתבים של היועצת המשפטית לכנסת" (Legal Advisor Guidelines and Letters)
  - **File Pattern**: `test/input/legal_advisor_letters/*.pdf`
  - **Fields**: Hebrew field names for extracting guidelines and letters metadata
- **Output**: CSV and Google Sheets integration for all four sources

### Output Files

Test outputs are saved to the `output/` directory:
- CSV files with extracted data
- Log files (if verbose logging is enabled)
- Any other generated files

## Test Configuration

The test configuration includes:

1. **Field Definitions**: Hebrew field names for ethics decisions
2. **Extraction Instructions**: Hebrew instructions for the LLM
3. **File Patterns**: Points to the test input directory
4. **Metadata Mapping**: Maps PDF URLs and download dates

## Adding New Tests

### Adding New PDF Files

1. Place new PDF files in `input/`
2. Update `test_config.yaml` if new fields are needed
3. Run tests to verify extraction works

### Adding New Test Cases

1. Add new test functions to `test_pdf_extraction.py`
2. Follow pytest conventions
3. Use the existing test fixtures and mocks

### Modifying Test Configuration

1. Edit `config/test_config.yaml`
2. Update field definitions as needed
3. Modify extraction instructions if required
4. Test with existing PDF files

## Troubleshooting

### Common Issues

1. **No PDF files found**: Ensure PDF files are in `input/` directory
2. **Configuration errors**: Check YAML syntax in `test_config.yaml`
3. **Import errors**: Run from project root or ensure PYTHONPATH is set
4. **Google Sheets errors**: Check credentials and permissions

### Debug Mode

Enable verbose logging for debugging:

```bash
python run_tests.py --verbose
```

### Clean Output

To clean test outputs:

```bash
rm -rf botnim/document_parser/dynamic_extractions/pdf_extraction/test/output/*
```

## Integration with CI/CD

The test runner can be integrated into CI/CD pipelines:

```bash
# Run tests and exit with appropriate code
python botnim/document_parser/dynamic_extractions/pdf_extraction/test/run_tests.py
```

The script returns:
- `0` if all tests pass
- `1` if any test fails 
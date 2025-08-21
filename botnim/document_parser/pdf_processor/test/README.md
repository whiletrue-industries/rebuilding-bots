# PDF Extraction Tests

This directory contains the test suite for the PDF extraction pipeline with Open Budget data sources.

## Running Tests

Due to the use of relative imports in the test files, you need to run pytest with the `--import-mode=importlib` flag:

```bash
# From the project root
python -m pytest --import-mode=importlib botnim/document_parser/pdf_processor/test/

# Or run a specific test file
python -m pytest --import-mode=importlib botnim/document_parser/pdf_processor/test/test_field_extraction.py

# Or run a specific test
python -m pytest --import-mode=importlib botnim/document_parser/pdf_processor/test/test_field_extraction.py::TestFieldExtraction::test_build_extraction_schema
```

## Current Test Structure

```
test/
├── config/          # Test configuration files
│   ├── test_config.yaml              # Main test configuration (Open Budget format)
│   ├── test_config_simple.yaml       # Simple test configuration
│   └── test_config_open_budget.yaml  # Open Budget specific tests
├── data/            # Mock data files
│   ├── mock_index.csv                # Mock Open Budget index.csv
│   └── mock_datapackage.json         # Mock Open Budget datapackage.json
├── test_pdf_extraction.py            # Unit tests for PDF extraction
├── test_open_budget_integration.py   # Open Budget integration tests
├── test_data_merging_scenarios.py    # Data merging and change detection tests
├── test_cli_pipeline.py              # CLI integration tests
├── test_field_extraction.py          # Field extraction unit tests
├── test_integration.py               # Comprehensive integration tests
├── mock_open_budget_data_source.py   # Mock Open Budget data source
└── run_tests.py                      # Test runner script
```

## Test Configuration

The test configuration (`config/test_config.yaml`) includes sources configured for Open Budget data sources:
- **Ethics Committee Decisions** - Hebrew field extraction for ethics decisions
- **Legal Advisor Letters** - Correspondence letter processing

All sources now use:
- `index_csv_url` and `datapackage_url` for Open Budget integration
- `output_config` with `spreadsheet_id` and `sheet_name` for Google Sheets
- `unique_id_field: "url"` for change detection

## Test Categories

### **Unit Tests**
- **`test_pdf_extraction.py`**: Core PDF extraction logic and configuration
- **`test_field_extraction.py`**: Field extraction with JSON schema validation

### **Integration Tests**
- **`test_open_budget_integration.py`**: Open Budget data source integration
- **`test_data_merging_scenarios.py`**: Change detection and data merging logic
- **`test_cli_pipeline.py`**: CLI integration with mock data sources
- **`test_integration.py`**: Comprehensive end-to-end testing

### **Mock Data**
- **`mock_open_budget_data_source.py`**: Mock implementation for isolated testing
- **`data/mock_*.csv`**: Mock Open Budget data files

## Key Test Features

### **Open Budget Integration Testing**
- URL and revision tracking
- Change detection logic
- Data merging with existing records
- Mock data source functionality

### **Data Merging Scenarios**
- Adding missing rows from datapackage
- Removing invalid rows not in datapackage
- Complete pipeline execution
- Error handling and validation

### **CLI Integration**
- Mock-based testing without real PDFs
- Configuration validation
- Pipeline execution testing

## Pipeline Summary Feature

The PDF extraction pipeline includes comprehensive final summaries with:
- Overall statistics and source breakdown
- Performance metrics and error tracking
- Detailed failure reporting
- Recommendations for issue resolution

## Adding Test Files

For new tests:
1. Use the mock Open Budget data source for isolated testing
2. Follow the current configuration format with `output_config`
3. Use the existing mock data files in `data/` directory
4. Ensure tests work with the orchestrator integration

## Troubleshooting

- **No PDF files found**: Tests now use Open Budget sources, not local PDFs
- **Configuration errors**: Check YAML syntax and ensure `output_config` is present
- **Import errors**: Run from project root with `--import-mode=importlib`
- **Google Sheets errors**: Tests use mock data, no real API calls needed

The test suite is now fully aligned with the current Open Budget-based workflow and orchestrator integration. 
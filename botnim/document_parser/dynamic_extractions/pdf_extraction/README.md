# PDF Extraction and Sync Pipeline

This module provides a comprehensive pipeline for extracting structured data from Hebrew PDFs and syncing it to Google Spreadsheets. The pipeline supports configurable field extraction using LLMs and handles both local processing and cloud synchronization.

## Features

- **Configurable PDF Sources**: Define multiple PDF sources with custom field extraction schemas
- **Hebrew Text Support**: Full support for Hebrew text extraction and preservation
- **LLM-based Field Extraction**: Uses OpenAI GPT-4.1 for intelligent field extraction
- **Local Text Extraction**: Uses `pdfplumber` and `pdfminer.six` for robust text extraction
- **CSV Output**: Generates structured CSV files with proper UTF-8 encoding
- **Google Sheets Sync**: Upload results directly to Google Sheets
- **Comprehensive Logging**: Detailed logging for debugging and monitoring
- **CLI Interface**: Command-line tools for testing and automation

## Installation

Set up Google Sheets API credentials (optional, for sync functionality):
   - Create a Google Cloud project
   - Enable Google Sheets API
   - Create a service account and download the JSON credentials file

## Configuration

The pipeline uses YAML configuration files to define PDF sources and extraction schemas. See `config_schema.yaml` for the complete schema.

### Example Configuration

```yaml
sources:
  - name: "Knesset Ethics Decisions"
    description: "Decisions of the Knesset Ethics Committee, 2010-2024"
    file_pattern: "ethics_decisions/*.pdf"
    unique_id_field: "source_url"
    metadata:
      source_url: "{pdf_url}"
      download_date: "{download_date}"
    fields:
      - name: "source_url"
        description: "Direct URL to the PDF file"
        example: "https://knesset.gov.il/ethics/decisions/ethics_decision_2023_01.pdf"
      - name: "decision_date"
        description: "Date of the ethics decision"
        example: "2023-05-12"
      - name: "case_number"
        description: "Case or file number"
        example: "1234/2023"
      - name: "member_name"
        description: "Name of the Knesset member"
        example: "יוסי כהן"
      - name: "decision_text"
        description: "Full text of the decision"
      - name: "full_text"
        description: "Full content of the document"
        hint: "Always include the entire text of the document as extracted, in Hebrew."
    extraction_instructions: |
      הפק את השדות הנדרשים מהטקסט של קובץ ה-PDF. השתמש ברשימת השדות וההנחיות. 
      החזר אובייקט JSON שבו שמות השדות באנגלית, אך כל הערכים בשפה המקורית (עברית) 
      כפי שמופיעים במסמך. אל תתרגם או תשנה את הטקסט המקורי. ודא ששדה full_text 
      תמיד מכיל את כל הטקסט של המסמך כפי שהופק מה-PDF.
```

## Usage

### 1. Text Extraction

Extract raw text from PDF files:

```bash
python text_extraction.py --pdf path/to/document.pdf --output extracted_text.txt
```

### 2. Field Extraction

Extract structured fields from text using LLM:

```bash
python field_extraction.py \
  --text extracted_text.txt \
  --config config_schema.yaml \
  --source "Knesset Ethics Decisions" \
  --output extracted_fields.json
```

### 3. CSV Output

Convert extracted data to CSV:

```bash
python csv_output.py \
  --input extracted_fields.json \
  --config config_schema.yaml \
  --source "Knesset Ethics Decisions" \
  --output-dir ./output
```

### 4. Google Sheets Sync

Upload CSV to Google Sheets:

```bash
python google_sheets_sync.py \
  --csv output/Knesset_Ethics_Decisions_20241201_120000.csv \
  --credentials path/to/credentials.json \
  --spreadsheet-id "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms" \
  --sheet-name "Knesset_Ethics_Decisions" \
  --replace
```

### 5. End-to-End Pipeline

Process entire pipeline for all sources:

```bash
python pdf_pipeline.py \
  --config config_schema.yaml \
  --output-dir ./output \
  --upload-sheets \
  --sheets-credentials path/to/credentials.json \
  --spreadsheet-id "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms" \
  --verbose
```

Process specific source only:

```bash
python pdf_pipeline.py \
  --config config_schema.yaml \
  --source "Knesset Ethics Decisions" \
  --output-dir ./output \
  --verbose
```

## Module Structure

```
pdf_extraction/
├── config_schema.yaml              # Configuration schema
├── pdf_extraction_config.py        # Pydantic models for config validation
├── text_extraction.py              # PDF text extraction using pdfplumber/pdfminer
├── field_extraction.py             # LLM-based field extraction
├── csv_output.py                   # CSV generation and output
├── google_sheets_sync.py           # Google Sheets upload functionality
├── pdf_pipeline.py                 # End-to-end pipeline orchestration
├── test_pdf_extraction.py          # Unit tests
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

## API Reference

### PDFExtractionConfig

Main configuration class for loading and validating YAML configs.

```python
from botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_extraction_config import PDFExtractionConfig

config = PDFExtractionConfig.from_yaml("config_schema.yaml")
```

### PDFExtractionPipeline

Main pipeline class for orchestrating the entire extraction process.

```python
from botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_pipeline import PDFExtractionPipeline

pipeline = PDFExtractionPipeline("config.yaml", openai_client, "output_dir")
success = pipeline.process_all_sources(
    upload_to_sheets=True,
    sheets_credentials="credentials.json",
    spreadsheet_id="spreadsheet_id"
)
```

### GoogleSheetsSync

Google Sheets upload functionality.

```python
from botnim.document_parser.dynamic_extractions.pdf_extraction.google_sheets_sync import GoogleSheetsSync

sync = GoogleSheetsSync("credentials.json")
success = sync.upload_csv_to_sheet(
    "data.csv", 
    "spreadsheet_id", 
    "sheet_name", 
    replace_existing=False
)
```

## Error Handling

The pipeline includes comprehensive error handling:

- **PDF Text Extraction**: Falls back from `pdfplumber` to `pdfminer.six` if primary method fails
- **Field Extraction**: Returns error dict if LLM extraction fails
- **CSV Output**: Validates data structure and warns about missing fields
- **Google Sheets**: Handles authentication errors and API failures gracefully

## Testing

Run unit tests:

```bash
python -m unittest test_pdf_extraction.py
```

## Troubleshooting

### Common Issues

1. **PDF Text Extraction Fails**
   - Ensure PDF is not password-protected
   - Check if PDF contains actual text (not just images)
   - Verify `pdfplumber` and `pdfminer.six` are installed

2. **Field Extraction Returns Errors**
   - Check OpenAI API key and quota
   - Verify extraction instructions are clear and in Hebrew
   - Ensure text was successfully extracted from PDF

3. **Google Sheets Upload Fails**
   - Verify service account credentials are valid
   - Check spreadsheet permissions (service account needs edit access)
   - Ensure spreadsheet ID is correct

4. **CSV Output is Empty**
   - Check input JSON structure matches expected format
   - Verify field names in config match extracted field names
   - Ensure data is properly flattened

### Logging

Enable verbose logging for debugging:

```bash
python pdf_pipeline.py --config config.yaml --verbose
```

This module is part of the botnim project and follows the same licensing terms. 
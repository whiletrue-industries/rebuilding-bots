# PDF Processing with Open Budget Data Sources

## Overview

The PDF processing system now uses Open Budget datapackages to automatically discover and process PDF documents. This system fetches structured data from Open Budget sources, downloads PDFs on-demand, extracts structured information, and stores results in Google Spreadsheets before vectorization. This approach provides better data consistency, change tracking, and integration with existing Open Budget infrastructure.

> **Note:** This document describes the Open Budget-based PDF processing workflow. The system uses Open Budget datapackages with `index.csv` and `datapackage.json` files for structured data discovery and change tracking.

## Key Features

- **Open Budget Integration**: Uses standardized Open Budget datapackages
- **URL and Revision Tracking**: Tracks changes using revision-based detection
- **Structured Data Extraction**: Uses AI to extract structured data from PDFs
- **Google Sheets Integration**: Stores extracted data in Google Spreadsheets
- **Change Detection**: Only processes new or updated PDFs based on revision tracking
- **No Local Storage**: Downloads PDFs temporarily for processing only

## Architecture

### Core Components

1. **OpenBudgetDataSource**: Fetches and manages Open Budget datapackage data
2. **PDFExtractionPipeline**: Processes PDFs and extracts structured data
3. **SyncConfigAdapter**: Converts sync config to PDF extraction config
4. **CSV Output Handler**: Manages CSV input/output with URL and revision tracking

### Data Flow

```
Open Budget Datapackage → Change Detection → PDF Download → AI Extraction → Google Sheets → Vector Store
                                      ↓
   Revision Tracking
```

## Configuration

### Sync Configuration

PDF sources are configured in the sync configuration file (e.g., `specs/takanon/sync_config.yaml`):

```yaml
- id: "ethics-committee-decisions-pdf"
  name: "החלטות ועדת האתיקה (Ethics Committee Decisions)"
  description: "החלטות PDF מוועדת האתיקה - Open Budget source"
  type: "pdf"
  pdf_config:
    index_csv_url: "https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/index.csv"
    datapackage_url: "https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/datapackage.json"
    processing:
      model: "gpt-4.1-mini"
      max_tokens: 4000
      temperature: 0.1
      fields:
        - name: "committee_name"
          type: "string"
          description: "Name of the committee that made the decision"
          required: true
        - name: "decision_date"
          type: "date"
          description: "Date when the decision was made"
          required: true
        - name: "member_name"
          type: "string"
          description: "Name of the Knesset member"
          required: false
        - name: "decision_summary"
          type: "string"
          description: "Summary of the decision"
          required: false
      options:
        enable_ocr: true
        ocr_language: "heb+eng"
        chunk_size: 1000
        chunk_overlap: 200
        max_file_size_mb: 50
  versioning_strategy: "revision"
  fetch_strategy: "open_budget"
  enabled: true
  priority: 5
  tags: ["משפטי", "אתיקה", "החלטות", "pdf", "open-budget"]
```

### Open Budget Data Structure

Each Open Budget datapackage contains:

1. **`index.csv`**: Lists all available PDF files with metadata
   ```csv
   url,title,filename,date
   https://example.com/doc1.pdf,Decision 1,doc1.pdf,2024-01-01
   https://example.com/doc2.pdf,Decision 2,doc2.pdf,2024-01-02
   ```

2. **`datapackage.json`**: Contains metadata including revision information
   ```json
   {
     "name": "ethics_committee_decisions",
     "revision": "2025.08.20-01",
     "hash": "abc123...",
     "resources": [
       {
         "name": "index",
         "path": "index.csv"
       }
     ]
   }
```

## Usage

### Command Line Interface

#### Process PDF Sources

```bash
# Process PDF sources using Open Budget data
python -m botnim.document_parser.pdf_processor.pdf_pipeline \
  --config specs/takanon/sync_config.yaml \
  --output-dir ./output \
  --verbose
```

#### Process with Google Sheets Integration

```bash
# Process and upload to Google Sheets
python -m botnim.document_parser.pdf_processor.pdf_pipeline \
  --config specs/takanon/sync_config.yaml \
  --output-dir ./output \
  --upload-sheets \
  --sheets-credentials .google_spreadsheet_credentials.json \
  --spreadsheet-id "your-spreadsheet-id" \
  --replace-sheet
```

### Programmatic Usage

```python
from botnim.document_parser.pdf_processor.pdf_pipeline import PDFExtractionPipeline
from botnim.document_parser.pdf_processor.sync_config_adapter import SyncConfigAdapter

# Load configuration from sync config
config = SyncConfigAdapter.load_pdf_sources_from_sync_config("specs/takanon/sync_config.yaml")

# Initialize pipeline
pipeline = PDFExtractionPipeline(config)

# Process all sources
results = pipeline.process_directory("./output")

print(f"Processed {len(results)} sources")
```

## Processing Workflow

### 1. Configuration Loading

The system loads PDF source configurations from the main sync configuration:

- Uses `SyncConfigAdapter` to convert sync config format to PDF extraction format
- Validates Open Budget URLs (`index_csv_url`, `datapackage_url`)
- Loads field extraction schemas from `processing` section

### 2. Change Detection

For each source, the system performs change detection:

- Fetches current `datapackage.json` to get latest revision
- Compares with existing data in Google Spreadsheet
- Identifies new URLs or updated revisions
- Only processes files that are new or have changed

### 3. Data Fetching

The system fetches structured data from Open Budget sources:

- Downloads `index.csv` to get list of available PDFs
- Downloads `datapackage.json` to get revision information
- Filters files based on change detection results

### 4. PDF Processing

Selected PDFs are processed using AI extraction:

- Downloads PDF from Open Budget URL
- Extracts text content (with OCR fallback if needed)
- Applies AI-based field extraction using configured schema
- Generates structured data records

### 5. Data Merging

New results are merged with existing data:

- Preserves unchanged records from existing Google Spreadsheet
- Adds new records for newly processed PDFs
- Updates records for PDFs with changed revisions
- Maintains URL and revision tracking columns

### 6. Output Generation

Results are written to CSV and optionally uploaded to Google Sheets:

- Creates CSV with all records (new + existing)
- Includes URL and revision columns for tracking
- Optionally uploads to Google Sheets
- Maintains data integrity and prevents duplicates

## Open Budget Data Sources

### Available Sources

The system currently supports these Open Budget datapackages:

1. **Ethics Committee Decisions**
   - URL: `https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/`
   - Content: Ethics committee decisions and rulings

2. **Knesset Committee Decisions**
   - URL: `https://next.obudget.org/datapackages/knesset/knesset_committee_decisions/`
   - Content: General Knesset committee decisions

3. **Legal Advisor Guidance**
   - URL: `https://next.obudget.org/datapackages/knesset/knesset_legal_advisor/`
   - Content: Legal guidance and opinions

4. **Legal Advisor Letters**
   - URL: `https://next.obudget.org/datapackages/knesset/knesset_legal_advisor_letters/`
   - Content: Correspondence and letters from legal advisor

### Adding New Sources

To add a new Open Budget source:

1. **Verify Open Budget Structure**: Ensure the source has `index.csv` and `datapackage.json`
2. **Add Configuration**: Add a new source entry in `sync_config.yaml`
3. **Define Fields**: Configure the field extraction schema
4. **Test**: Run with test configuration first

```yaml
- id: "new-source-pdf"
  name: "New Source"
  type: "pdf"
  pdf_config:
    index_csv_url: "https://next.obudget.org/datapackages/your-source/index.csv"
    datapackage_url: "https://next.obudget.org/datapackages/your-source/datapackage.json"
    processing:
      fields:
        - name: "field1"
          type: "string"
          description: "Description of field1"
        - name: "field2"
          type: "date"
          description: "Description of field2"
  versioning_strategy: "revision"
  fetch_strategy: "open_budget"
```

## Change Detection Logic

### Revision-Based Tracking

The system uses revision-based change detection:

1. **Current Revision**: Fetched from `datapackage.json`
2. **Existing Revision**: Stored in Google Spreadsheet
3. **URL Tracking**: Tracks individual PDF URLs
4. **Change Logic**:
   - If revision changed: Process all files in index.csv
   - If revision unchanged: Only process new URLs
   - If no existing data: Process all files

### Implementation

```python
def get_files_to_process(self, existing_urls: Set[str], existing_revision: str) -> List[Dict]:
    """Determine which files need processing based on change detection."""
    current_revision = self.get_current_revision()
    
    if existing_revision != current_revision:
        # Revision changed - process all files
        return self.index_data
    else:
        # Revision unchanged - only process new URLs
        return [file for file in self.index_data if file['url'] not in existing_urls]
```

## Data Schema

### CSV Input/Output Format

The system uses a standardized CSV format with tracking columns:

```csv
url,revision,title,date,field1,field2,field3
https://example.com/doc1.pdf,2025.08.20-01,Decision 1,2024-01-01,value1,value2,value3
https://example.com/doc2.pdf,2025.08.20-01,Decision 2,2024-01-02,value4,value5,value6
```

**Required Columns:**
- `url`: Unique identifier for the PDF
- `revision`: Current revision from datapackage.json
- `title`: Title from index.csv (optional)
- `date`: Date from index.csv (optional)

**Dynamic Columns:**
- All configured fields from the processing schema
- Automatically collected from all sources

### Field Extraction Schema

Fields are defined in the configuration:

```yaml
fields:
  - name: "decision_number"
    type: "string"
    description: "Number of the decision"
    required: true
  - name: "decision_date"
    type: "date"
    description: "Date of the decision"
    required: true
  - name: "member_name"
    type: "string"
    description: "Name of the Knesset member"
    required: false
```

## Testing

### Unit Tests

Run the test suite:

```bash
cd botnim/document_parser/pdf_processor/test
python test_open_budget_integration.py
```

### Integration Tests

Test with mock Open Budget data:

```bash
# Run integration tests with mock data
python test_open_budget_integration.py

# Run specific test components
python test_pdf_extraction.py -v
```

### Test Configuration

Tests use mock Open Budget data sources:

```yaml
# test/config/test_config_open_budget.yaml
sources:
  - id: "test_ethics_committee_decisions"
    name: "Test Ethics Committee Decisions"
    index_csv_url: "file://test/data/mock_index.csv"
    datapackage_url: "file://test/data/mock_datapackage.json"
    unique_id_field: "url"
    fields:
      - name: "decision_number"
        type: "string"
        description: "Decision number"
```

## Error Handling

### Common Error Scenarios

1. **Network Errors**: Failed requests to Open Budget URLs
2. **Invalid Datapackage**: Missing or malformed datapackage.json
3. **Processing Failures**: PDF extraction or AI processing errors
4. **Schema Validation**: Field extraction validation failures

### Error Recovery

- Network errors are retried with exponential backoff
- Invalid datapackages raise clear error messages
- Processing errors are logged with detailed context
- Schema validation errors provide field-level feedback

## Integration with Sync Workflow

### As Part of Main Sync

The PDF processing integrates with the main sync workflow:

```python
def run_sync_workflow():
    # Process PDF sources (Open Budget)
    process_pdf_sources()
    
    # Process spreadsheet sources (including generated PDF data)
    process_spreadsheet_sources()
    
    # Process HTML sources
    process_html_sources()
```

### Standalone Operation

PDF processing can run independently:

```bash
# Run PDF processing only
python -m botnim.document_parser.pdf_processor.pdf_pipeline \
  --config sync_config.yaml \
  --output-dir ./output
```

## Best Practices

### Configuration

1. **Use Specific Field Schemas**: Define clear, specific field descriptions
2. **Enable OCR**: Handle image-based PDFs with `enable_ocr: true`
3. **Set Appropriate Timeouts**: Balance reliability and performance
4. **Validate URLs**: Ensure Open Budget URLs are accessible

### Monitoring

1. **Track Processing Status**: Monitor completion rates and errors
2. **Log Change Detection**: Monitor revision changes and file counts
3. **Monitor Google Sheets**: Track data upload success rates
4. **Performance Metrics**: Monitor processing times and throughput

### Security

1. **Validate Open Budget URLs**: Ensure sources are trusted
2. **Limit File Sizes**: Prevent large file downloads
3. **Sanitize Content**: Clean extracted text
4. **Access Control**: Restrict Google Sheets access

## Troubleshooting

### Common Issues

1. **No Files Processed**
   - Check if Open Budget URLs are accessible
   - Verify datapackage.json contains revision field
   - Check change detection logic

2. **Network Failures**
   - Verify Open Budget service availability
   - Check timeout settings
   - Review network configuration

3. **Processing Failures**
   - Check PDF file accessibility
   - Verify field extraction schema
   - Review error logs

4. **Google Sheets Issues**
   - Verify authentication credentials
   - Check spreadsheet permissions
   - Review upload configuration

### Debug Mode

Enable debug logging for detailed troubleshooting:

```python
import logging
logging.getLogger('botnim.document_parser.pdf_processor').setLevel(logging.DEBUG)
```

## Migration from Legacy System

### Key Changes

1. **Data Source**: From direct PDF scraping to Open Budget datapackages
2. **Change Detection**: From file-based to revision-based tracking
3. **Storage**: From direct vector store to Google Sheets intermediate storage
4. **Configuration**: From separate PDF config to integrated sync config

### Migration Steps

1. **Update Configuration**: Replace `file_pattern` with Open Budget URLs
2. **Update Tests**: Use mock Open Budget data instead of local PDFs
3. **Update Documentation**: Reflect new architecture and workflow
4. **Clean Repository**: Remove local PDF files and update .gitignore

## Future Enhancements

### Planned Features

1. **Incremental Processing**: Process only changed content within PDFs
2. **Batch Processing**: Process multiple PDFs in parallel
3. **Content Validation**: Verify extracted content quality
4. **Advanced Filtering**: More sophisticated file selection
5. **Webhook Integration**: Notify external systems of new content

### Performance Optimizations

1. **Caching**: Cache Open Budget datapackage data
2. **Parallel Downloads**: Download multiple PDFs simultaneously
3. **Streaming Processing**: Process PDFs as they download
4. **Compression**: Compress temporary files

### Integration Enhancements

1. **Real-time Updates**: Subscribe to Open Budget change notifications
2. **Multi-source Aggregation**: Combine data from multiple Open Budget sources
3. **Advanced Analytics**: Track processing metrics and trends
4. **API Integration**: Provide REST API for external access

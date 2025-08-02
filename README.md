# Rebuilding - Bots

## Introduction

This is a repository for the rebuilding anew bots (bot-nim).

### Recent Improvements

The PDF extraction pipeline has been significantly enhanced with:
- **Enhanced JSON Schema Validation** - robust client-side validation using jsonschema library with detailed error messages
- **Source-specific Google Sheets integration** - each source automatically gets its own sheet
- **DRY architecture** - clean separation of concerns and reusable components  
- **Comprehensive testing** - full test suite covering all aspects of the pipeline
- **Robust error handling** - graceful handling of edge cases and API limits
- **Performance monitoring** - detailed metrics and structured logging

## Getting Started

```bash
# Create a virtual environment
$ python3 -m venv venv
$ source venv/bin/activate
# Install the package
$ pip install -U -e .
$ botnim --help
```

for development:
```bash
$ pip install -U -e .[dev]
```

## Environment Variables for Elasticsearch

To support different Elasticsearch clusters for local development, staging, and production, you must set the following environment variables:

**Required for Production:**
- `ES_HOST_PRODUCTION`: Elasticsearch host URL for production (e.g., `https://prod-es.example.com:9200`)
- `ES_USERNAME_PRODUCTION`: Username for production Elasticsearch
- `ES_PASSWORD_PRODUCTION` or `ELASTIC_PASSWORD_PRODUCTION`: Password for production Elasticsearch
- `ES_CA_CERT_PRODUCTION`: Path to CA certificate file for production (optional, for SSL verification)

**Required for Staging:**
- `ES_HOST_STAGING`: Elasticsearch host URL for staging (e.g., `https://staging-es.example.com:9200`)
- `ES_USERNAME_STAGING`: Username for staging Elasticsearch
- `ES_PASSWORD_STAGING` or `ELASTIC_PASSWORD_STAGING`: Password for staging Elasticsearch
- `ES_CA_CERT_STAGING`: Path to CA certificate file for staging (optional, for SSL verification)

**Required for Local Development:**
- `ES_HOST_LOCAL`: Elasticsearch host URL for local development (defaults to `https://localhost:9200`)
- `ES_USERNAME_LOCAL`: Username for local Elasticsearch
- `ES_PASSWORD_LOCAL` or `ELASTIC_PASSWORD_LOCAL`: Password for local Elasticsearch
- `ES_CA_CERT_LOCAL`: Path to CA certificate file for local (optional, for SSL verification)

**Optional Fallback:**
- `ES_CA_CERT`: Generic CA certificate path (used as fallback if environment-specific CA cert is not set)

**Note:** You must explicitly specify the environment when running commands. The application will use the environment-specific variables based on your choice. If any of these variables are missing for the environment you're using, the application will show a clear error message indicating which variables need to be set.

**Important:** The application no longer has default fallback environments. All commands and scripts require explicit environment specification to prevent accidental deployments to the wrong environment.

**Example .env file:**
```env
# Production
ES_HOST_PRODUCTION=https://prod-es.example.com:9200
ES_USERNAME_PRODUCTION=prod_user
ES_PASSWORD_PRODUCTION=prod_pass
ES_CA_CERT_PRODUCTION=/path/to/prod-ca.crt

# Staging
ES_HOST_STAGING=https://staging-es.example.com:9200
ES_USERNAME_STAGING=staging_user
ES_PASSWORD_STAGING=staging_pass
ES_CA_CERT_STAGING=/path/to/staging-ca.crt

# Local Development
ES_HOST_LOCAL=https://localhost:9200
ES_USERNAME_LOCAL=elastic
ES_PASSWORD_LOCAL=changeme
ES_CA_CERT_LOCAL=/path/to/local-ca.crt

# Optional: Generic fallback CA certificate
ES_CA_CERT=/path/to/generic-ca.crt
```

**Usage Examples:**
```bash
# Local development
botnim sync local takanon --backend es

# Staging
botnim sync staging takanon --backend es

# Production
botnim sync production takanon --backend es

# Demo scripts (environment is required)
python backend/es/demo-load-data-to-es.py local
python backend/es/demo-query-es.py "your query" local

**Note:** The demo scripts use direct imports from the `botnim` package. Make sure the package is installed in development mode (`pip install -e .`) to run these scripts.
```

## Directory Structure

- `.env.sample`: Sample environment file for the benchmarking scripts.
- `botnim/`: Main package directory.
  - `__init__.py`: Package initialization file.
  - `cli.py`: Command line interface for the bots.
  - `sync.py`: Script for syncing the specifications with the OpenAI account.
  - `collect_sources.py`: Module to collect and process the sources for the bots.
  - `vector_store/`: Vector store management package.
    - `__init__.py`: Package initialization.
    - `vector_store_base.py`: Abstract base class for vector store implementations.
    - `vector_store_openai.py`: OpenAI Vector Store implementation.
    - `vector_store_es.py`: Elasticsearch Vector Store implementation
        - see the `backend/es` directory for examples
        - run `pytest` to test the Elasticsearch Vector Store.
  - `benchmark/`: Benchmarking scripts for the bots.
      Copy this file to `.env` and fill in the necessary values.
    - `run-benchmark.py`: Main benchmarking script.
    - `assistant_loop.py`: Local assistant loop tool.
- `specs/`: Specifications for the bots.
  - `budgetkey/`: Specifications for the budgetkey bot.
    - `config.yaml`: Agent configuration file.
    - `agent.txt`: Agent instructions.
  - `takanon/`: Specifications for the takanon bot.
    - `config.yaml`: Agent configuration file.
    - `agent.txt`: Agent instructions.
    - `extraction/`: Extracted and processed text from the Knesset Takanon
  - `openapi/`: OpenAPI definitions of the BudgetKey (and other deprecated) APIs.
- `botnim/document_parser/`: Document extraction and processing tools (formerly takanon_extractions/)
  - `dynamic_extractions/`: Main extraction pipeline and utilities
    - `process_document.py`: Full document processing pipeline (now accessible via `botnim process-document`)
    - `extract_structure.py`: Structure extraction (now accessible via `botnim extract-structure`)
    - `extract_content.py`: Content extraction (now accessible via `botnim extract-content`)
    - `generate_markdown_files.py`: Markdown generation (now accessible via `botnim generate-markdown-files`)
    - `pdf_extraction/`: PDF extraction and Google Sheets sync pipeline
      - `pdf_pipeline.py`: Main orchestration pipeline (now accessible via `botnim pdf-extract`)
      - `text_extraction.py`: PDF text extraction with Hebrew RTL fixes
      - `field_extraction.py`: LLM-based structured data extraction with enhanced JSON schema validation
      - `google_sheets_service.py`: High-level Google Sheets service wrapper
      - `google_sheets_sync.py`: Low-level Google Sheets API operations
      - `csv_output.py`: CSV generation and data flattening
      - `metrics.py`: Performance metrics and structured logging
      - `metadata_handler.py`: Metadata management for PDF files
      - `pdf_extraction_config.py`: Configuration models and YAML loading
      - `exceptions.py`: Custom exception classes for error handling
      - `test/`: Comprehensive test suite with sample PDFs
    - `logs/`: Intermediate and output files (structure.json, pipeline metadata, markdown chunks)
- `ui/`: DEPRECATED: User interface for the bots.

## Common Tasks

### Querying the Vector Store

The `botnim query` command provides several ways to interact with the vector store:

```bash
# Search in the vector store
botnim query search staging takanon common_knowledge "מה עושה יושב ראש הכנסת?"
botnim query search staging takanon common_knowledge --num-results 5 "your query here"
botnim query search staging takanon common_knowledge -n 5 "your query here"
# Show full content of search results
botnim query search staging takanon common_knowledge "your query here" --full
# or use the short flag
botnim query search staging takanon common_knowledge "your query here" -f
# Display results in right-to-left order
botnim query search staging takanon common_knowledge "your query here" --rtl

# List all available indexes
botnim query list-indexes staging --bot budgetkey
botnim query list-indexes staging
# Display indexes in right-to-left order
botnim query list-indexes staging --rtl

# Show fields/structure of an index
botnim query show-fields staging budgetkey common_knowledge
# Display fields in right-to-left order
botnim query show-fields staging budgetkey common_knowledge --rtl

# List all available search modes
botnim query list-modes
```

Available query commands:
- `search`: Search the vector store with semantic or specialized search
  - Options:
    - `--num-results`, `-n`: Number of results to return (default: depends on search mode)
    - `--search-mode`: Use a specific search mode (see `list-modes` for available modes)
    - `--full`, `-f`: Show full content of results instead of just summaries
    - `--rtl`: Display results in right-to-left order
- `list-indexes`: Show all available Elasticsearch indexes
  - Options:
    - `--rtl`: Display indexes in right-to-left order
- `show-fields`: Display the structure and field types of an index
  - Options:
    - `--rtl`: Display fields in right-to-left order
- `list-modes`: List all available search modes and their default settings

### Search Modes

The vector store supports multiple search modes to optimize query results based on the context of the search. Each mode has its own default for `num_results`, which can be overridden with `--num-results`/`-n`.

To see all available search modes and their defaults, run:

```bash
botnim query list-modes
```

#### Example search mode usage

```bash
botnim query search staging takanon legal_text "סעיף 12" --search-mode TAKANON_SECTION_NUMBER
```

#### Current search modes (from registry):

- **REGULAR**: Standard semantic search across all main fields. Default num_results: 7
- **TAKANON_SECTION_NUMBER**: Specialized search mode for finding Takanon sections by their number (e.g. 'סעיף 12'). Default num_results: 3

(For a full, up-to-date list, use `botnim query list-modes`)

### Updating Vector Store Content

To update or add content to the vector store:

```bash
# Update all contexts for the takanon bot (complete rebuild)
botnim sync staging takanon --backend es --replace-context all

# Update only a specific context without rebuilding others
botnim sync staging takanon --backend es --replace-context <context name>

# Check the content after updating
botnim query search staging takanon ethics_rules "<query>" --num-results 3
```

### Evaluating Query Performance

The `botnim evaluate` command allows you to evaluate the performance of queries against a vector store:

```bash
# Basic usage
botnim evaluate takanon legal_text staging path/to/query_evaluations.csv

# With custom parameters
botnim evaluate takanon legal_text staging path/to/query_evaluations.csv --max-results 30 --adjusted-f1-limit 10
```

Required CSV columns:
- `question_id`: Unique identifier for each question
- `question_text`: The actual question text
- `doc_filename`: The filename of the expected document

Options:
- `--max-results`: Maximum number of results to retrieve per query (default: 20)
- `--adjusted-f1-limit`: Number of documents to consider for adjusted F1 score calculation (default: 7)

The command will:
1. Run each query against the vector store
2. Compare retrieved documents with expected documents
3. Calculate regular and adjusted F1 scores
4. Save results to a new CSV file with the suffix '_results'
5. Print summary statistics

To find available contexts for a bot, use:
```bash
botnim query list-indexes staging --bot takanon
```

### Updating the Specifications

1. Edit the specifications in the `specs/` directory.
2. If using external sources (e.g., Google Spreadsheets):
   - Configure the source URL in the bot's `config.yaml`
   - The content will be automatically downloaded during sync
Either:
3. `botnim sync {staging/production} {budgetkey/takanon} --backend {openai/es}` to sync the specifications with the OpenAI account.
   - Use `--replace-context` flag to force a complete rebuild of the vector store (useful when context files have been modified)
   - Use `--context-to-update` option to update only a specific context (e.g., `--context-to-update ethics_rules`) without replacing all contexts
Or
3. Commit the changes to the repository
4. Run the 'Sync' action from the GitHub Actions tab.

### Running the Benchmark

Running the benchmark in production is best done using the action in the GitHub Actions tab.

For running locally:
`botnim benchmarks {staging/production} {budgetkey/takanon} {TRUE/FALSE whether to save results locally}`

## Document Processing Pipeline

The document processing pipeline extracts structured content from HTML legal documents and converts them to individual markdown files.

### Quick Start

```bash
# Process a document with markdown generation
botnim process-document botnim/document_parser/extract_sources/your_document.html specs/takanon/extraction/ --generate-markdown
```

### Advanced Usage

- Structure extraction only:
  ```bash
  botnim extract-structure "botnim/document_parser/extract_sources/your_document.html" "botnim/document_parser/dynamic_extractions/logs/your_document_structure.json"
  ```
- Content extraction only:
  ```bash
  botnim extract-content "botnim/document_parser/extract_sources/your_document.html" "botnim/document_parser/dynamic_extractions/logs/your_document_structure.json" "סעיף" --output specs/takanon/extraction/your_document_structure_content.json
  ```
- Markdown generation only:
  ```bash
  botnim generate-markdown-files specs/takanon/extraction/your_document_structure_content.json --write-files --output-dir botnim/document_parser/dynamic_extractions/logs/chunks/
  ```

### Output Structure

The pipeline produces outputs as follows:

- **In the output directory you specify:**
  - Only the final `*_structure_content.json` file is saved here. This is the file used for downstream sync/ingestion.
- **In the logs directory (`botnim/document_parser/dynamic_extractions/logs/`):**
  - All intermediate files, including:
    - `*_structure.json` (document structure)
    - `*_pipeline_metadata.json` (execution metadata)
    - `chunks/` (markdown files, if generated)

### Example Directory Layout

```
specs/takanon/extraction/
    תקנון הכנסת_structure_content.json
    חוק_רציפות_הדיון_בהצעות_חוק_structure_content.json

botnim/document_parser/dynamic_extractions/logs/
    תקנון הכנסת_structure.json
    תקנון הכנסת_pipeline_metadata.json
    חוק_רציפות_הדיון_בהצעות_חוק_structure.json
    חוק_רציפות_הדיון_בהצעות_חוק_structure_pipeline_metadata.json
    chunks/
        תקנון הכנסת_סעיף_1.md
        חוק_רציפות_הדיון_בהצעות_חוק_סעיף_1.md
        ...
```

## PDF Extraction Pipeline

The PDF extraction pipeline provides comprehensive tools for extracting structured data from Hebrew PDFs and syncing to Google Sheets.

### Features

- **Multi-source PDF processing** with configurable extraction schemas
- **Enhanced JSON Schema Validation** - robust client-side validation using jsonschema library with detailed error messages
- **Source-specific Google Sheets integration** - each source automatically gets its own sheet
- **Hebrew text handling** with RTL (right-to-left) text direction fixes
- **OCR Support** - automatic fallback to OCR for image-based PDFs using Tesseract
- **LLM-based field extraction** using OpenAI GPT-4.1 with JSON response format
- **Comprehensive testing framework** with unit tests and integration tests
- **Performance metrics and structured logging**
- **Robust error handling** with custom exception types
- **DRY architecture** - clean separation of concerns and reusable components
- **Modular design** - each component has a single responsibility and can be used independently
- **Metadata Management** - hash-based filenames for long PDF names to prevent filesystem issues

### Google Sheets Setup

To use Google Sheets integration, you need to set up authentication:

#### Method 1: Application Default Credentials (Recommended)

```bash
# Install Google Cloud CLI
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/spreadsheets
```

**Important**: The `cloud-platform` scope is required by Google Cloud, and the `spreadsheets` scope is needed for Google Sheets API access.

#### Method 2: Service Account Key

1. **Create a Google Cloud Project** (or use an existing one)
2. **Enable the Google Sheets API**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Navigate to "APIs & Services" > "Library"
   - Search for "Google Sheets API" and enable it

3. **Create a Service Account**:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "Service Account"
   - Fill in the service account details
   - Click "Create and Continue"

4. **Generate JSON Key**:
   - In the service account list, click on your new service account
   - Go to the "Keys" tab
   - Click "Add Key" > "Create New Key"
   - Choose "JSON" format
   - Download the JSON file

5. **Share Your Spreadsheet**:
   - Open your Google Spreadsheet
   - Click "Share" and add your service account email (found in the JSON file)
   - Give it "Editor" permissions

### Usage

```bash
# Process PDFs using a configuration file
botnim pdf-extract config.yaml input_dir

# Process specific source only
botnim pdf-extract config.yaml input_dir --source "Ethics Committee Decisions"

# With Google Sheets integration (ADC) - each source gets its own sheet
botnim pdf-extract config.yaml input_dir --upload-to-sheets --spreadsheet-id "your-spreadsheet-id" --use-adc

# Complete pipeline example (process PDFs + upload to Google Sheets)
botnim pdf-extract config.yaml input_dir --spreadsheet-id "your-spreadsheet-id" --use-adc

# With Google Sheets integration (Service Account) - each source gets its own sheet
botnim pdf-extract config.yaml input_dir --upload-to-sheets --spreadsheet-id "your-spreadsheet-id" --credentials-path "credentials.json"

# With additional options (--sheet-name is deprecated, each source gets its own sheet)
botnim pdf-extract config.yaml input_dir --verbose --no-metrics --replace-sheet
```

### Configuration

The pipeline uses YAML configuration files to define PDF sources and extraction schemas. Each source will automatically get its own sheet in Google Sheets when using the `--upload-to-sheets` option:

```yaml
sources:
  - name: "Ethics Committee Decisions"
    description: "Decisions of the Knesset Ethics Committee"
    file_pattern: "ethics_decisions/*.pdf"
    unique_id_field: "source_url"
    fields:
      - name: "decision_date"
        description: "Date of the ethics decision"
        example: "2023-05-12"
      - name: "member_name"
        description: "Name of the Knesset member"
        example: "יוסי כהן"
    extraction_instructions: "Extract the specified fields from the document text..."
```

### Enhanced JSON Schema Validation

The pipeline includes robust client-side JSON schema validation using the `jsonschema` library:

### CSV Field Handling

The pipeline automatically handles different field schemas from multiple sources:
- **Dynamic Field Collection**: Collects all unique field names from all records across different sources
- **Proper CSV Quoting**: Uses `csv.QUOTE_ALL` to handle Hebrew text with commas correctly
- **Source Splitting**: Automatically splits data by `source_name` for separate Google Sheets
- **Unified Output**: Combines records from different sources with different field schemas into a single CSV

- **Comprehensive Validation**: Validates field types, required fields, and prevents unexpected fields
- **Detailed Error Messages**: Provides specific field-level error information for debugging
- **Required Dependency**: jsonschema is now a required dependency for robust validation
- **Performance Optimized**: Minimal overhead with fast validation processing

**Validation Features**:
- ✅ Field type validation (all fields must be strings)
- ✅ Required field validation (all configured fields must be present)
- ✅ Unexpected field prevention (`additionalProperties: false`)
- ✅ Array and single object support
- ✅ Detailed error reporting with field paths

**Example Validation Error**:
```
JSON schema validation failed:
  - content: 'content' is a required property
  - extra_field: Additional properties are not allowed ('extra_field' was unexpected)
```

### Testing

```bash
# Run comprehensive integration tests (covers all PR feedback points)
cd botnim/document_parser/dynamic_extractions/pdf_extraction/test
python test_integration.py

# Run unit tests for field extraction with schema validation
python test_field_extraction.py

# Run unit tests only
python -m pytest test_pdf_extraction.py -v

# Run specific test components
python run_tests.py
```

**Test Coverage:**
- ✅ **Prerequisites** - Environment and dependencies check
- ✅ **CSV Contract** - Input/output CSV file handling with proper quoting
- ✅ **Separation of Concerns** - Pipeline without Google Sheets
- ✅ **Path Resolution** - Absolute, relative, and invalid paths
- ✅ **OpenAI JSON Format** - JSON response format validation
- ✅ **JSON Schema Validation** - Enhanced client-side validation with jsonschema
- ✅ **CLI Integration** - Command-line interface testing
- ✅ **Google Sheets Integration** - Authentication and upload testing
- ✅ **OCR Processing** - Image-based PDF handling with Tesseract
- ✅ **CSV Field Handling** - Multi-source field schema management
- ✅ **Metadata Management** - Hash-based filename generation

### Troubleshooting Google Sheets Authentication

If you encounter authentication errors like "Request had insufficient authentication scopes":

1. **Re-authenticate with proper scopes**:
   ```bash
   gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/spreadsheets
   ```

2. **Verify your Google Cloud project has Google Sheets API enabled**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Navigate to "APIs & Services" > "Library"
   - Search for "Google Sheets API" and ensure it's enabled

3. **Check spreadsheet permissions**:
   - Ensure your Google account has "Editor" access to the target spreadsheet
   - If using a service account, make sure the service account email is added as an editor

4. **Common error messages and solutions**:
   - `ACCESS_TOKEN_SCOPE_INSUFFICIENT`: Re-authenticate with the correct scopes
   - `PERMISSION_DENIED`: Check spreadsheet sharing permissions
   - `API not enabled`: Enable Google Sheets API in your Google Cloud project

### Performance Monitoring

The pipeline automatically collects performance metrics:
- Processing time per PDF
- Text extraction vs field extraction time
- Success rates and error tracking
- Detailed logs in JSON format

Results are saved to `pipeline_metrics.json` and displayed as a summary at the end of processing.

## Configuration for Sync

- In your `config.yaml`, use `type: split` for each JSON structure content file:
  ```yaml
  sources:
    - type: split
      source: extraction/תקנון הכנסת_structure_content.json
    - type: split
      source: extraction/חוק_רציפות_הדיון_בהצעות_חוק_structure_content.json
  ```

## Context Specification

- Sources can be configured in `config.yaml` with different types:
  ```yaml
  context:
    - name: "Knowledge Base Name"
      type: "files"      # Multiple separate files
      source: "*.md"    # File pattern to match
    - name: "Spreadsheet Knowledge"
      type: "google-spreadsheet"  # Google spreadsheet source
      source: "https://..."  # Spreadsheet URL
    - name: "Split Content"
      type: "split_file"    # Single file that needs splitting
      source: "path/to/file.md"
  ```

## Tools

### CLI Assistant

The botnim assistant command provides an interactive chat interface with OpenAI assistants:

```bash
# Basic usage - will show list of available assistants
botnim assistant

# Start chat with a specific assistant
botnim assistant --assistant-id <assistant-id>

# Enable RTL support for Hebrew
botnim assistant --rtl

# Choose environment for vector search
botnim assistant --environment production  # or staging (default)
```

## Additional Documentation

For more detailed information about specific components:

- **PDF Extraction Testing**: See `botnim/document_parser/dynamic_extractions/pdf_extraction/test/README.md` for detailed testing procedures
- **Document Processing**: See `botnim/document_parser/dynamic_extractions/README.md` for advanced document processing workflows
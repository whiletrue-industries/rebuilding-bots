# Rebuilding - Bots

## Introduction

This is a repository for the rebuilding anew bots (bot-nim).

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

## Document Extraction and Sync (Updated)

- The document processing pipeline now saves only the final `*_structure_content.json` files in the `specs/takanon/extraction/` folder. These are the only files required for sync/ingestion.
- All intermediate files (structure.json, pipeline metadata, and markdown files if generated) are saved in `botnim/document_parser/dynamic_extractions/logs/`.
- Markdown files are **not required** for sync—they are only for manual inspection.
- To generate markdown files for manual inspection, use the `--generate-markdown` flag with the pipeline:

```bash
botnim process-document botnim/document_parser/extract_sources/חוק הכנסת.html specs/takanon/extraction/ --generate-markdown
```

- For advanced/manual markdown generation, you can use the CLI directly:

```bash
botnim generate-markdown-files specs/takanon/extraction/חוק הכנסת_structure_content.json --write-files --output-dir botnim/document_parser/dynamic_extractions/logs/chunks/
```

  - Use `--write-files` to actually write files to disk.
  - Use `--dry-run` to preview what would be generated, without writing files.
  - If neither flag is provided, markdown content is generated in memory (for programmatic use).

- The pipeline and CLI both support in-memory markdown generation for direct ingestion or further processing in automated workflows.

### Example Directory Layout

```
specs/takanon/extraction/
    תקנון הכנסת_structure_content.json
    חוק_רציפות_הדיון_בהצעות_חוק_structure_content.json

botnim/document_parser/dynamic_extractions/logs/
    תקנון הכנסת_structure.json
    תקנון הכנסת_pipeline_metadata.json
    חוק_רציפות_הדיון_בהצעות_חוק_structure.json
    חוק_רציפות_הדיון_בהצעות_חוק_pipeline_metadata.json
    chunks/
        תקנון הכנסת_סעיף_1.md
        חוק_רציפות_הדיון_בהצעות_חוק_סעיף_1.md
        ...
```

## Configuration for Sync

- In your `config.yaml`, use `type: split` for each JSON structure content file:
  ```yaml
  sources:
    - type: split
      source: extraction/תקנון הכנסת_structure_content.json
    - type: split
      source: extraction/חוק_רציפות_הדיון_בהצעות_חוק_structure_content.json
  ```

## Manual Markdown Generation

- To generate markdown files for manual review, run:
  ```bash
  botnim process-document botnim/document_parser/extract_sources/חוק הכנסת.html specs/takanon/extraction/ --generate-markdown
  ```
- Markdown files will be written to `botnim/document_parser/dynamic_extractions/logs/chunks/`.

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
- `botnim/cli_assistant.py`: Interactive CLI tool for chatting with OpenAI assistants (supports RTL languages)

The botnim assistant command provides an interactive chat interface with OpenAI assistants:

# Basic usage - will show list of available assistants
```bash
botnim assistant
```
# Start chat with a specific assistant
```bash
botnim assistant --assistant-id <assistant-id>
```
# Enable RTL support for Hebrew
```bash
botnim assistant --rtl
```
# Choose environment for vector search
```bash
botnim assistant --environment production  # or staging (default)
```
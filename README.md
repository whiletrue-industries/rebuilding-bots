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
- `specs/`: Specifications for the bots.
  - `budgetkey/`: Specifications for the budgetkey bot.
    - `config.yaml`: Agent configuration file.
    - `agent.txt`: Agent instructions.
  - `takanon/`: Specifications for the takanon bot.
    - `config.yaml`: Agent configuration file.
    - `agent.txt`: Agent instructions.
    - `extraction/`: Extracted and processed text from the Knesset Takanon
  - `openapi/`: OpenAPI definitions of the BudgetKey (and other deprecated) APIs.
- `takanon_extractions/`: Code and extracted content from the Knesset Takanon and other laws.
  - `process_clauses.py`: Script to parse the Knesset Takanon HTML, extract the document structure, save it as JSON/YAML and Markdown files and then to split the JSON data into individual Markdown files for each clause.
- `ui/`: DEPRECATED: User interface for the bots.

## Common Tasks

### Querying the Vector Store

The `botnim query` command provides several ways to interact with the vector store:

```bash
# Search in the vector store
botnim query search staging takanon common_knowledge "מה עושה יושב ראש הכנסת?"
botnim query search staging takanon common_knowledge --results 5 "your query here"

# List all available indexes
botnim query list-indexes staging --bot budgetkey
botnim query list-indexes staging

# Show fields/structure of an index
botnim query show-fields staging budgetkey common_knowledge
```

Available query commands:
- `search`: Search the vector store with semantic search
- `list-indexes`: Show all available Elasticsearch indexes
- `show-fields`: Display the structure and field types of an index

### Updating the Specifications

1. Edit the specifications in the `specs/` directory.
2. If using external sources (e.g., Google Spreadsheets):
   - Configure the source URL in the bot's `config.yaml`
   - The content will be automatically downloaded during sync
Either:
3. `botnim sync {staging/production} {budgetkey/takanon} --backend {openai/es}` to sync the specifications with the OpenAI account.
   - Use `--replace-context` flag to force a complete rebuild of the vector store (useful when context files have been modified)
Or
3. Commit the changes to the repository
4. Run the 'Sync' action from the GitHub Actions tab.

### Running the Benchmark

Running the benchmark in production is best done using the action in the GitHub Actions tab.

For running locally:
`botnim benchmarks {staging/production} {budgetkey/takanon} {TRUE/FALSE whether to save results locally}`

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
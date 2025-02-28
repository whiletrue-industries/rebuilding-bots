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
- `content_extraction/`: Content and metadata extraction logic, including the raw extracted content from the Knesset Takanon and other laws.
  - `process_clauses.py`: Script to parse the Knesset Takanon HTML, extract the document structure, save it as JSON/YAML and Markdown files and then to split the JSON data into individual Markdown files for each clause.
- `ui/`: DEPRECATED: User interface for the bots.

## Common Tasks

### Querying the Vector Store

The `botnim query` command provides several ways to interact with the vector store:

```bash
# Search in the vector store
botnim query search staging takanon common_knowledge "מה עושה יושב ראש הכנסת?"
botnim query search staging takanon common_knowledge --results 5 "your query here"
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
```

Available query commands:
- `search`: Search the vector store with semantic search
  - Options:
    - `--num-results`, `-n`: Number of results to return (default: 7)
    - `--full`, `-f`: Show full content of results instead of just summaries
    - `--rtl`: Display results in right-to-left order
- `list-indexes`: Show all available Elasticsearch indexes
  - Options:
    - `--rtl`: Display indexes in right-to-left order
- `show-fields`: Display the structure and field types of an index
  - Options:
    - `--rtl`: Display fields in right-to-left order

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
```bash`
botnim assistant --rtl
```
# Choose environment for vector search
```bash
botnim assistant --environment production  # or staging (default)
```


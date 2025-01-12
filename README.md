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

## Directory Structure

- `.env.sample`: Sample environment file for the benchmarking scripts.
- `botnim/`: Main package directory.
  - `__init__.py`: Package initialization file.
  - `cli.py`: Command line interface for the bots.
  - `sync.py`: Script for syncing the specifications with the OpenAI account.
  - `kb/`: Knowledge base management package.
    - `__init__.py`: Package initialization.
    - `base.py`: Abstract base class for knowledge base implementations.
    - `openai.py`: OpenAI Vector Store implementation.
    - `manager.py`: Context manager for handling knowledge base operations.
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

### Available Commands

The following CLI commands are available:

```bash
# Download external sources (e.g., Google Spreadsheets)
botnim download

# Sync bots with OpenAI
botnim sync {staging/production} {budgetkey/takanon/all} [--replace-context]

# Run benchmarks
botnim benchmarks {staging/production} {budgetkey/takanon/all} [--local] [--reuse-answers] [--select failed/all/ID] [--concurrency N]
```

### Updating the Specifications

1. Edit the specifications in the `specs/` directory.
2. If using external sources (e.g., Google Spreadsheets):
   - Configure the source URL in the bot's `config.yaml`
   - Run `botnim download` to fetch and convert the latest data
3. In case of changes to the vector stores, remove them in the OpenAI account playground.
Either:
4. `botnim sync {staging/production} {budgetkey/takanon}` to sync the specifications with the OpenAI account.
Or
5. Commit the changes to the repository
6. Run the 'Sync' action from the GitHub Actions tab.

## Software Design

### Knowledge Base Management

The project now uses a modular knowledge base system that supports different backend implementations:

- `KnowledgeBase`: Abstract base class defining the interface for vector store implementations
- `OpenAIVectorStore`: Implementation for OpenAI's vector store operations
- `ContextManager`: Handles loading and processing of context files
- External sources can be configured in `config.yaml` and downloaded using the CLI

This design allows for:
- Easy addition of new knowledge base backends (e.g., Elasticsearch)
- Clear separation between vector store operations and assistant management
- Improved error handling and logging
- Consistent interface across different implementations
- Automated handling of external knowledge sources

The system separates responsibilities:
- Vector stores handle document storage and retrieval
- Assistant management is handled directly in the sync process
- Context management handles document processing and source downloads

### Logging

The project uses a centralized logging configuration:
- All logging configuration is managed in `config.py`
- Each module gets its logger through `get_logger(__name__)`
- This ensures consistent logging behavior across the application
- Default log level is INFO, configurable through logging.basicConfig

### Running the Benchmark

Running the benchmark in production is best done using the action in the GitHub Actions tab.

For running locally:
`botnim benchmarks {staging/production} {budgetkey/takanon} {TRUE/FALSE whether to save results locally}`

  
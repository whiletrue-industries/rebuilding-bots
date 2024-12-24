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

### Updating the Specifications

1. Edit the specifications in the `specs/` directory.
2. In case of changes to the vector stores, remove them in the OpenAI account playground.
Either:
3. Use the sync command (see Syncing Options below) to sync the specifications with the OpenAI account.
Or:
3. Commit the changes to the repository
4. Run the 'Sync' action from the GitHub Actions tab.

### Syncing Options

The sync command supports the following options:
- Environment: `production` or `staging`
- Bots: `budgetkey`, `takanon`, or `all`
- Flags:
  - `--replace-context`: Deletes and recreates all vector stores for the assistant
  - `--replace-common-knowledge`: Deletes and recreates only the common knowledge vector stores (those marked with `split: common-knowledge.md`)

Examples:
```bash
botnim sync production takanon                           # Regular sync
botnim sync staging all --replace-context               # Sync all bots, replacing all contexts
botnim sync production budgetkey --replace-common-knowledge  # Sync with common knowledge refresh
```

### Running the Benchmark

Running the benchmark in production is best done using the action in the GitHub Actions tab.

For running locally:
`botnim benchmarks {staging/production} {budgetkey/takanon} {TRUE/FALSE whether to save results locally}`

  

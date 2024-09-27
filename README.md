# Rebuilding - Bots

## Introduction

This is a repository for the rebuilding anew bots (bot-nim).

## Directory Structure

- `benchmark/`: Benchmarking scripts for the bots.
  - `.env.sample`: Sample environment file for the benchmarking scripts.
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
  - `sync.py`: Script for syncing the specifications with the OpenAI account.
- `takanon_extractions/`: Code for extracting and processing text from the Knesset Takanon.
- `ui/`: DEPRECATED: User interface for the bots.

## Common Tasks

### Updating the Specifications

1. Edit the specifications in the `specs/` directory.
2. In case of changes to the vector stores, remove them in the OpenAI account playground.
Either:
3. `python specs/sync.py` to sync the specifications with the OpenAI account.
Or
3. Commit the changes to the repository
4. Run the 'Sync' action from the GitHub Actions tab.

### Running the Benchmark

Running the benchmark is best done using the action in the GitHub Actions tab.

For running locally:
`python benchmark/run-benchmark.py`


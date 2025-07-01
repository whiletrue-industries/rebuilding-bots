# Rebuild Repository Structure and Main Flows Analysis

## Overview

The `rebuild` repository contains the **Rebuilding - Bots** project, which is a comprehensive system for creating and managing AI-powered bots that provide specialized knowledge assistance. The project specifically focuses on Israeli governmental and legal information through two main bots: **BudgetKey** (budget-related queries) and **Takanon** (Knesset bylaws and procedures).

## Repository Structure

### Root Level
```
rebuild/
├── botnim/                  # Main Python package
├── specs/                   # Bot specifications and configurations
├── backend/                 # Infrastructure components
├── ui/                      # Deprecated web interface
├── takanon_extractions/     # Data extraction tools and content
├── .github/                 # CI/CD workflows
├── cache/                   # Metadata and processing cache
├── logs/                    # Application logs
├── setup.py                 # Package configuration
├── requirements.txt         # Python dependencies
└── README.md               # Project documentation
```

### Core Package: `botnim/`

The main Python package containing all core functionality:

- **`cli.py`** - Command-line interface with commands for sync, query, benchmarks, and assistant chat
- **`sync.py`** - Core synchronization logic for updating OpenAI assistants and vector stores
- **`collect_sources.py`** - Data collection from various sources (files, Google Sheets, etc.)
- **`config.py`** - Configuration management and environment settings
- **`query.py`** - Vector store querying functionality with multiple search modes
- **`cli_assistant.py`** - Interactive chat interface with OpenAI assistants
- **`dynamic_extraction.py`** - LLM-powered content extraction and metadata generation

#### Vector Store Subsystem: `botnim/vector_store/`
- **`vector_store_base.py`** - Abstract base class for vector store implementations
- **`vector_store_openai.py`** - OpenAI Vector Store implementation
- **`vector_store_es.py`** - Elasticsearch implementation (primary backend)
- **`search_modes.py`** - Specialized search configurations for different query types
- **`search_config.py`** - Search configuration management

#### Benchmarking System: `botnim/benchmark/`
- **`runner.py`** - Benchmark execution engine
- **`evaluate_queries.py`** - Query performance evaluation
- **`evaluate_metrics_cli.py`** - CLI for metrics evaluation
- **`assistant_loop.py`** - Local assistant testing tool

### Bot Specifications: `specs/`

Each bot has its own directory with configuration files:

#### `specs/takanon/` (Knesset Bylaws Bot)
- **`config.yaml`** - Bot configuration including contexts and data sources
- **`agent.txt`** - Bot instructions and behavior definition
- **`extraction/`** - Processed legal documents and clauses

#### `specs/budgetkey/` (Budget Information Bot)
- **`config.yaml`** - Bot configuration
- **`agent.txt`** - Bot instructions
- **`common-knowledge.md`** - Budget-related knowledge base

### Data Extraction: `takanon_extractions/`

Specialized tools for processing legal documents:
- **`process_caluses.py`** - HTML parser for Knesset Takanon structure extraction
- **`transform_to_markdown.py`** - LLM-powered document transformation tool
- **`takanon.htm`** - Source HTML document
- **`output/`** - Processed documents and extractions

### Infrastructure: `backend/`

#### Elasticsearch Backend: `backend/es/`
- **`docker-compose.yml`** - Elasticsearch cluster configuration
- **`demo-*.py`** - Example scripts for ES operations
- **`README.md`** - Setup and usage instructions

## Main Flows

### 1. Bot Synchronization Flow

**Command**: `botnim sync <environment> <bot> --backend <backend>`

**Process**:
1. **Configuration Loading** - Read bot config from `specs/{bot}/config.yaml`
2. **Source Collection** - Gather data from various sources:
   - Local markdown files
   - Google Spreadsheets
   - Split documents
3. **Content Processing** - Extract metadata using LLM-powered analysis
4. **Vector Store Update** - Upload processed content to chosen backend (OpenAI/Elasticsearch)
5. **Assistant Creation/Update** - Configure OpenAI assistant with tools and vector store access

**Key Components**:
- `sync.py::sync_agents()` - Main orchestration
- `collect_sources.py` - Multi-source data collection
- `vector_store_*.py` - Backend-specific implementations

### 2. Query Flow

**Command**: `botnim query search <environment> <bot> <context> "<query>"`

**Process**:
1. **Search Mode Selection** - Choose appropriate search strategy based on query type
2. **Vector Store Query** - Execute semantic/specialized search
3. **Result Processing** - Format and rank results
4. **Output Formatting** - Present results with metadata and source links

**Search Modes**:
- **REGULAR** - Standard semantic search (default: 7 results)
- **TAKANON_SECTION_NUMBER** - Specialized for finding specific legal sections (default: 3 results)

**Key Components**:
- `query.py::run_query()` - Main query execution
- `search_modes.py` - Search strategy definitions
- `vector_store_es.py` - Elasticsearch query implementation

### 3. Benchmarking Flow

**Command**: `botnim benchmarks <environment> <bot>`

**Process**:
1. **Test Case Loading** - Load predefined questions and expected answers
2. **Query Execution** - Run each test query through the system
3. **Result Evaluation** - Compare actual vs expected results using F1 scores
4. **Performance Metrics** - Calculate accuracy and relevance metrics
5. **Report Generation** - Output detailed performance analysis

**Key Components**:
- `benchmark/runner.py` - Benchmark orchestration
- `benchmark/evaluate_queries.py` - Evaluation logic
- `benchmark/query_evaluations.csv` - Test cases

### 4. Interactive Assistant Flow

**Command**: `botnim assistant --assistant-id <id>`

**Process**:
1. **Assistant Selection** - Choose from available OpenAI assistants
2. **Chat Session** - Interactive conversation with context awareness
3. **Vector Search Integration** - Automatic knowledge base queries
4. **RTL Support** - Hebrew language display optimization

**Key Components**:
- `cli_assistant.py::assistant_main()` - Chat interface
- OpenAI Assistant API integration
- Vector store function calling

### 5. Content Processing Flow

**Triggered during sync operations**

**Process**:
1. **Source Detection** - Identify content type and structure
2. **HTML Parsing** - Extract structured data from legal documents
3. **LLM Enhancement** - Generate metadata and improve formatting
4. **Chunking** - Split large documents into manageable pieces
5. **Caching** - Store processed content to avoid reprocessing

**Key Components**:
- `takanon_extractions/process_caluses.py` - Legal document parsing
- `dynamic_extraction.py` - LLM-powered metadata extraction
- `collect_sources.py` - Multi-format content processing

## Configuration System

### Environment Management
- **Production** - Live bots with full functionality
- **Staging** - Development and testing environment
- Environment-specific OpenAI API keys and assistant names

### Bot Configuration Structure
```yaml
slug: bot-identifier
name: Display Name
description: Bot description
instructions: agent.txt
context:
  - slug: context-id
    name: Context Display Name
    sources:
      - type: files|google-spreadsheet|split_file
        source: path/pattern or URL
```

### Vector Store Backends
- **OpenAI** - Managed vector store service
- **Elasticsearch** - Self-hosted, more control and customization

## Key Features

### Multi-Source Data Integration
- Local markdown files
- Google Spreadsheets (dynamic content)
- Split document processing
- Automated metadata extraction

### Advanced Search Capabilities
- Semantic vector search
- Specialized search modes for legal queries
- Section number lookup optimization
- Multilingual support (Hebrew RTL)

### Comprehensive Evaluation
- Automated benchmarking
- F1 score calculations
- Performance tracking
- Query effectiveness metrics

### Developer-Friendly Tools
- CLI interface for all operations
- Interactive assistant testing
- Comprehensive logging
- Flexible configuration system

## Dependencies and Technology Stack

### Core Technologies
- **Python 3.10+** - Main language
- **OpenAI API** - LLM and assistant services
- **Elasticsearch** - Vector search backend
- **Click** - CLI framework
- **PyYAML** - Configuration management

### Data Processing
- **BeautifulSoup** - HTML parsing
- **Dataflows** - ETL operations
- **html2text** - Content conversion

### Infrastructure
- **Docker Compose** - Elasticsearch deployment
- **GitHub Actions** - CI/CD automation
- **Environment Variables** - Configuration management

This architecture provides a robust, scalable system for building and managing specialized AI assistants with deep domain knowledge, particularly suited for governmental and legal information systems.
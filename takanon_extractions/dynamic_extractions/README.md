# Document Processing Tool

A tool for extracting structured content from HTML legal documents and converting them to individual markdown files.

## Overview

This tool processes HTML legal documents through three automated stages:

1. **Structure Analysis** - Uses AI to identify document hierarchy and structure
2. **Content Extraction** - Extracts full content for specified section types (like clauses, chapters, etc.)
3. **File Generation** - Creates individual markdown files for each content section

## Getting Started

The simplest way to process a document:

```bash
python process_document.py your_document.html output_folder
```

This will:
- Analyze the document structure
- Extract content from sections (default: "סעיף" - Hebrew clauses)
- Generate individual markdown files in `output_folder/chunks/`

## Architecture

### Core Components

- **`process_document.py`** - Main document processing script with error handling and monitoring
- **`pipeline_config.py`** - Configuration management and validation
- **`extract_structure.py`** - Document structure extraction using OpenAI API
- **`extract_content.py`** - Content extraction from HTML
- **`generate_markdown_files.py`** - Markdown file generation

### Key Features

- **Robust Error Handling** - Comprehensive error handling with proper logging
- **Configuration Management** - Centralized configuration with validation
- **Pipeline Orchestration** - Coordinated execution of all stages
- **Monitoring & Observability** - Detailed logging and execution metadata
- **Validation** - Input/output validation at each stage
- **Dry Run Support** - Test pipeline without generating files

## Usage

### Quick Start

```bash
# Run complete pipeline
python process_document.py input.html output_directory

# With custom content type
python process_document.py input.html output_directory --content-type "סעיף"

# Dry run (no files generated in final stage)
python process_document.py input.html output_directory --dry-run
```

### Advanced Usage

```bash
# Use production environment llm config
python process_document.py input.html output_directory --environment production

# Custom model and token limits
python process_document.py input.html output_directory --model gpt-4.1 --max-tokens 32000

# Save configuration for reuse
python process_document.py input.html output_directory --save-config config.json

# Load configuration from file
python process_document.py --config config.json
```

### Individual Components

You can also run individual pipeline stages:

```bash
# Structure extraction only
python extract_structure.py input.html structure.json --mark-type "סעיף"

# Content extraction only
python extract_content.py input.html structure.json "סעיף"

# Markdown generation only
python generate_markdown_files.py structure_content.json
```

## Configuration

### Pipeline Configuration

The pipeline accepts the following configuration options:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input_html_file` | Path | Required | Path to input HTML file |
| `output_base_dir` | Path | Required | Base directory for all outputs |
| `content_type` | str | "סעיף" | Type of content to extract |
| `environment` | str | "staging" | OpenAI environment (staging/production) |
| `model` | str | "gpt-4o" | OpenAI model to use |
| `max_tokens` | int | 32000 | Maximum tokens for API calls |
| `dry_run` | bool | False | Run without generating final files |
| `overwrite_existing` | bool | False | Overwrite existing files |
| `mediawiki_mode` | bool | False | Apply MediaWiki-specific heuristics (e.g., selflink class) |

### Environment Variables

Required environment variables:

```bash
# For staging
OPENAI_API_KEY_STAGING=your_staging_key

# For production
OPENAI_API_KEY_PRODUCTION=your_production_key
```

## Output Structure

The pipeline generates the following output structure:

```
output_directory/
├── input_structure.json           # Document structure
├── input_structure_content.json   # Structure with content
├── pipeline_metadata.json         # Execution metadata
└── chunks/                        # Individual markdown files
    ├── document_section1.md
    ├── document_section2.md
    └── ...
```

### Metadata

Each execution generates detailed metadata including:

- Execution times for each stage
- File sizes and counts
- Error and warning messages
- Configuration used
- Performance metrics

## Error Handling

The pipeline implements comprehensive error handling:

- **Configuration Validation** - Validates all inputs before execution
- **Stage Dependencies** - Ensures each stage completes before proceeding
- **File Validation** - Validates JSON structure between stages
- **Graceful Degradation** - Continues processing when possible
- **Detailed Logging** - Structured logging for debugging

## Logging

All components use structured logging with different levels:

- **INFO** - Normal operation progress
- **WARNING** - Non-fatal issues
- **ERROR** - Fatal errors that stop execution

Logs include:
- Timestamps
- Component names
- Execution context
- Performance metrics

## Testing

### Run Tests

```bash
# Run automated test
python test_pipeline.py

# Test with specific example
python process_document.py "examples/takanon/תקנון הכנסת.html" test_output --dry-run
```

### Validation

The pipeline includes validation at multiple levels:

1. **Input Validation** - File existence, format checks
2. **Configuration Validation** - Parameter validation
3. **Output Validation** - JSON schema validation
4. **Content Validation** - Structure integrity checks

## Performance Considerations

### Optimization Tips

1. **Token Limits** - Adjust `max_tokens` based on document size
2. **Model Selection** - Use appropriate model for complexity
3. **Batch Processing** - Process multiple documents in sequence
4. **Caching** - Reuse structure files when possible

### Resource Usage

- **Memory** - Scales with document size
- **API Calls** - One call per document for structure extraction
- **Storage** - Temporary files cleaned automatically

## Troubleshooting

### Common Issues

1. **API Key Issues**
   - Ensure correct environment variables are set
   - Check API key permissions

2. **Large Documents**
   - Increase `max_tokens` parameter
   - Consider document preprocessing

3. **Content Extraction Issues**
   - Verify HTML structure
   - Check content type spelling

4. **Permission Errors**
   - Ensure write permissions for output directory
   - Check file system limits

### Debug Mode

Enable debug logging:

```bash
export LOG_LEVEL=DEBUG
python process_document.py input.html output_directory
```


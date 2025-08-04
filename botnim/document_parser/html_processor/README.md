# Document Processing Pipeline

> **Note:** All main functionality accessible via the `botnim` CLI. See the project root `README.md` for comprehensive usage and examples.

A tool for extracting structured content from HTML legal documents and converting them to individual markdown files.

## Overview

This tool processes HTML legal documents through three automated stages:

1. **Structure Analysis** - Uses AI to identify document hierarchy and structure
2. **Content Extraction** - Extracts full content for specified section types (like clauses, chapters, etc.)
3. **File Generation** - Creates individual markdown files for each content section

## Quick Start

The simplest way to process a document:

```bash
botnim process-document botnim/document_parser/data/sources/html/your_document.html specs/takanon/extraction/ --generate-markdown
```

This will:
- Analyze the document structure
- Extract content from sections (default: "סעיף" - Hebrew clauses)
- Generate individual markdown files in `botnim/document_parser/html_processor/logs/chunks/`

## Architecture

### Core Components

- **`process_document.py`** - Main document processing script (now accessible via `botnim process-document`)
- **`pipeline_config.py`** - Configuration management and validation
- **`extract_structure.py`** - Document structure extraction (now accessible via `botnim extract-structure`)
- **`extract_content.py`** - Content extraction (now accessible via `botnim extract-content`)
- **`generate_markdown_files.py`** - Markdown file generation (now accessible via `botnim generate-markdown-files`)

## CLI Usage

### Quick Start

```bash
botnim process-document botnim/document_parser/data/sources/html/your_document.html specs/takanon/extraction/ --generate-markdown
```

### Advanced Usage

- Structure extraction only:
  ```bash
  botnim extract-structure "botnim/document_parser/data/sources/html/your_document.html" "botnim/document_parser/html_processor/logs/your_document_structure.json"
  ```
- Content extraction only:
  ```bash
  botnim extract-content "botnim/document_parser/data/sources/html/your_document.html" "botnim/document_parser/html_processor/logs/your_document_structure.json" "סעיף" --output specs/takanon/extraction/your_document_structure_content.json
  ```
- Markdown generation only:
  ```bash
  botnim generate-markdown-files specs/takanon/extraction/your_document_structure_content.json --write-files --output-dir botnim/document_parser/html_processor/logs/chunks/
  ```

### In-Memory Markdown Generation for Sync/Automation

- The function `generate_markdown_from_json` can be used programmatically to generate markdown content in memory as a dictionary, without writing files to disk.
- This is useful for direct ingestion or further processing in automated workflows.

## Output Structure

The pipeline produces outputs as follows:

- **In the output directory you specify:**
  - Only the final `*_structure_content.json` file is saved here. This is the file used for downstream sync/ingestion.
- **In the logs directory (`botnim/document_parser/html_processor/logs/`):**
  - All intermediate files, including:
    - `*_structure.json` (document structure)
    - `*_pipeline_metadata.json` (execution metadata)
    - `chunks/` (markdown files, if generated)

### Example Directory Layout

```
specs/takanon/extraction/
    תקנון הכנסת_structure_content.json
    חוק_רציפות_הדיון_בהצעות_חוק_structure_content.json

botnim/document_parser/html_processor/logs/
    תקנון הכנסת_structure.json
    תקנון הכנסת_pipeline_metadata.json
    חוק_רציפות_הדיון_בהצעות_חוק_structure.json
    חוק_רציפות_הדיון_בהצעות_חוק_structure_pipeline_metadata.json
    chunks/
        תקנון הכנסת_סעיף_1.md
        חוק_רציפות_הדיון_בהצעות_חוק_סעיף_1.md
        ...
```

## Configuration Note

- For context types in your config, use `type: split` for JSON structure content files.
- Example:
  ```yaml
  sources:
    - type: split
      source: extraction/תקנון הכנסת_structure_content.json
    - type: split
      source: extraction/חוק_רציפות_הדיון_בהצעות_חוק_structure_content.json
  ```

## Python API (Advanced Use)

While the CLI is preferred for most users, you can also use the Python API directly for advanced workflows:

```python
from .generate_markdown_files import generate_markdown_from_json
markdown_dict = generate_markdown_from_json("specs/takanon/extraction/your_document_structure_content.json")
```

## Related Components

- **PDF Extraction Pipeline**: See `pdf_extraction/` directory for PDF processing capabilities
- **Testing**: See `pdf_extraction/test/` for comprehensive test suites

---

For more information, see the main project `README.md`.


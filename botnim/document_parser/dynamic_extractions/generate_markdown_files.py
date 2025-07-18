#!/usr/bin/env python3
"""
Generate individual markdown files from JSON structure with content.
"""

import argparse
import json
import sys
import re
from pathlib import Path
from botnim.config import get_logger

# Logger setup
logger = get_logger(__name__)

def sanitize_filename(filename):
    """
    Sanitize filename for filesystem compatibility.
    """
    # Replace problematic characters with underscores
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Replace multiple spaces/underscores with single underscore
    filename = re.sub(r'[_\s]+', '_', filename)
    # Remove leading/trailing underscores
    filename = filename.strip('_')
    return filename

def get_base_filename(input_file_path):
    """
    Extract base filename without extension from input file path.
    """
    return Path(input_file_path).stem

def build_hierarchy_path(item, parent_path=[]):
    """
    Build the hierarchical path for a given item.
    Returns a list of (section_name, depth) tuples.
    """
    current_path = parent_path.copy()
    
    # Add current item to path if it has a section_name
    if item.get('section_name'):
        depth = item.get('depth', len(current_path) + 1)
        current_path.append((item['section_name'], depth))
    
    return current_path

def generate_markdown_content(item, hierarchy_path, document_name):
    """
    Generate markdown content with hierarchical context.
    """
    content_lines = []
    
    # Add document name as the top-level header
    content_lines.append(f"# {document_name}")
    content_lines.append("")  # Empty line after heading
    
    # Add hierarchical context as headings
    for section_name, depth in hierarchy_path:
        # Use appropriate markdown heading level (## for depth 1, ### for depth 2, etc.)
        # Start from ## since # is reserved for document name
        heading_level = '#' * min(depth + 1, 6)  # Limit to 6 levels max, offset by 1
        content_lines.append(f"{heading_level} {section_name}")
        content_lines.append("")  # Empty line after heading
    
    # Add the actual content
    content = item.get('content', '').strip()
    if content:
        content_lines.append(content)
    
    return '\n'.join(content_lines)

def generate_markdown_dict(items, document_name, parent_path=[]):
    """
    Traverse the structure and generate a dict of {filename: markdown_content} for items with content.
    """
    markdown_dict = {}
    for item in items:
        current_path = build_hierarchy_path(item, parent_path)
        if item.get('content'):
            section_name = item.get('section_name', 'unknown')
            sanitized_section = sanitize_filename(section_name)
            filename = f"{document_name}_{sanitized_section}.md"
            markdown_content = generate_markdown_content(item, current_path, document_name)
            markdown_dict[filename] = markdown_content
        if 'children' in item and isinstance(item['children'], list):
            child_dict = generate_markdown_dict(item['children'], document_name, current_path)
            markdown_dict.update(child_dict)
    return markdown_dict

def generate_markdown_from_json(json_path, output_dir=None, write_files=False, dry_run=False):
    """
    Generate markdown files from a JSON structure with content.
    Args:
        json_path: Path to the JSON file with content (str or Path)
        output_dir: Output directory for markdown files (str or Path, optional)
        write_files: If True, write markdown files to disk
        dry_run: If True, only log what would be generated
    Returns:
        markdown_dict: dict of {filename: markdown_content}
    Raises:
        FileNotFoundError, ValueError, or IOError on error
    """
    logger.info("Starting markdown file generation (pipeline-friendly function)")
    json_path = Path(json_path)
    if not json_path.exists():
        logger.error(f"JSON file not found: {json_path}")
        raise FileNotFoundError(f"JSON file not found: {json_path}")
    try:
        logger.info(f"Reading JSON file: {json_path}")
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info("JSON file read successfully")
    except Exception as e:
        logger.error(f"Error reading JSON file: {e}")
        raise
    # Determine output directory
    if output_dir:
        output_dir = Path(output_dir)
    else:
        output_dir = json_path.parent / 'chunks'
    # Get document name from metadata
    document_name = data.get('metadata', {}).get('document_name', '')
    if not document_name:
        input_file = data.get('metadata', {}).get('input_file', '')
        if not input_file:
            logger.error("No document_name or input_file found in metadata")
            raise ValueError("No document_name or input_file found in metadata")
        document_name = get_base_filename(input_file)
    document_name = sanitize_filename(document_name)
    logger.info(f"Document name: {document_name}")
    structure = data.get('structure', [])
    if not structure:
        logger.error("No structure found in JSON")
        raise ValueError("No structure found in JSON")
    markdown_dict = generate_markdown_dict(structure, document_name)
    if dry_run:
        logger.info("Running in dry-run mode - no files will be created")
        logger.info("Files that would be generated:")
        for filename in markdown_dict:
            logger.info(f"  {output_dir / filename}")
        logger.info(f"Total files that would be generated: {len(markdown_dict)}")
    if write_files and not dry_run:
        logger.info(f"Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Generating markdown files...")
        for filename, content in markdown_dict.items():
            filepath = output_dir / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
        logger.info(f"Total files generated: {len(markdown_dict)}")
    return markdown_dict


def main():
    """CLI interface for markdown generation."""
    parser = argparse.ArgumentParser(description='Generate markdown content from JSON structure with content')
    parser.add_argument('json_file', help='Path to the JSON file with content')
    parser.add_argument('--output-dir', '-o', help='Output directory for markdown files (used only with --write-files)')
    parser.add_argument('--write-files', action='store_true', help='Write markdown files to disk for manual verification')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be generated without creating files')
    args = parser.parse_args()

    generate_markdown_from_json(
        args.json_file,
            output_dir=args.output_dir,
        write_files=args.write_files,
            dry_run=args.dry_run
        )


if __name__ == "__main__":
    main() 
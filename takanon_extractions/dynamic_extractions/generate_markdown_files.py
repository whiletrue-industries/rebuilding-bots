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

def traverse_and_generate(items, document_name, output_dir, parent_path=[]):
    """
    Traverse the structure and generate markdown files for items with content.
    """
    generated_files = []
    
    for item in items:
        # Build current hierarchy path
        current_path = build_hierarchy_path(item, parent_path)
        
        # If item has content, generate a markdown file
        if item.get('content'):
            section_name = item.get('section_name', 'unknown')
            
            # Create filename
            sanitized_section = sanitize_filename(section_name)
            filename = f"{document_name}_{sanitized_section}.md"
            filepath = output_dir / filename
            
            # Generate markdown content
            markdown_content = generate_markdown_content(item, current_path, document_name)
            
            # Write file
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(markdown_content)
                generated_files.append(filepath)
                logger.info(f"Generated: {filepath}")
            except Exception as e:
                logger.error(f"Error writing file {filepath}: {e}")
        
        # Recursively process children
        if 'children' in item and isinstance(item['children'], list):
            child_files = traverse_and_generate(
                item['children'], 
                document_name, 
                output_dir, 
                current_path
            )
            generated_files.extend(child_files)
    
    return generated_files

def main():
    """CLI interface for markdown generation."""
    parser = argparse.ArgumentParser(description='Generate individual markdown files from JSON structure with content')
    parser.add_argument('json_file', help='Path to the JSON file with content')
    parser.add_argument('--output-dir', '-o', help='Output directory for markdown files (default: chunks subfolder in JSON file directory)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be generated without creating files')
    
    args = parser.parse_args()
    
    logger.info("Starting markdown file generation")
    logger.info(f"JSON file: {args.json_file}")
    logger.info(f"Dry run: {args.dry_run}")
    
    # Read JSON file
    json_path = Path(args.json_file)
    if not json_path.exists():
        logger.error(f"JSON file not found: {json_path}")
        return 1
    
    try:
        logger.info(f"Reading JSON file: {json_path}")
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info("JSON file read successfully")
    except Exception as e:
        logger.error(f"Error reading JSON file: {e}")
        return 1
    
    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = json_path.parent / 'chunks'
    
    # Create output directory if it doesn't exist
    if not args.dry_run:
        logger.info(f"Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get document name from metadata
    document_name = data.get('metadata', {}).get('document_name', '')
    if not document_name:
        # Fallback to extracting from input_file if document_name not available
        input_file = data.get('metadata', {}).get('input_file', '')
        if not input_file:
            logger.error("No document_name or input_file found in metadata")
            return 1
        document_name = get_base_filename(input_file)
    
    document_name = sanitize_filename(document_name)
    logger.info(f"Document name: {document_name}")
    logger.info(f"Output directory: {output_dir}")
    
    if args.dry_run:
        logger.info("Running in dry-run mode - no files will be created")
    
    # Process structure
    structure = data.get('structure', [])
    if not structure:
        logger.error("No structure found in JSON")
        return 1
    
    # Generate markdown files
    try:
        if args.dry_run:
            logger.info("Files that would be generated:")
            # For dry run, we'll just show what would be generated
            def dry_run_traverse(items, parent_path=[]):
                count = 0
                for item in items:
                    current_path = build_hierarchy_path(item, parent_path)
                    if item.get('content'):
                        section_name = item.get('section_name', 'unknown')
                        sanitized_section = sanitize_filename(section_name)
                        filename = f"{document_name}_{sanitized_section}.md"
                        filepath = output_dir / filename
                        hierarchy_str = " > ".join([name for name, _ in current_path])
                        logger.info(f"  {filepath}")
                        logger.info(f"    Hierarchy: {hierarchy_str}")
                        count += 1
                    if 'children' in item:
                        count += dry_run_traverse(item['children'], current_path)
                return count
            
            total_files = dry_run_traverse(structure)
            logger.info(f"Total files that would be generated: {total_files}")
        else:
            logger.info("Generating markdown files...")
            generated_files = traverse_and_generate(structure, document_name, output_dir)
            logger.info(f"Successfully generated {len(generated_files)} markdown files in {output_dir}")
    
    except Exception as e:
        logger.error(f"Error generating markdown files: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 
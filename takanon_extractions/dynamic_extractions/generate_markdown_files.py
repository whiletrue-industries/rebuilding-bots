#!/usr/bin/env python3
"""
Generate individual markdown files from JSON structure with content.
"""

import argparse
import json
import sys
import re
from pathlib import Path

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
                print(f"Generated: {filepath}")
            except Exception as e:
                print(f"Error writing file {filepath}: {e}")
        
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
    parser = argparse.ArgumentParser(description='Generate individual markdown files from JSON structure with content')
    parser.add_argument('json_file', help='Path to the JSON file with content')
    parser.add_argument('--output-dir', '-o', help='Output directory for markdown files (default: chunks subfolder in JSON file directory)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be generated without creating files')
    
    args = parser.parse_args()
    
    # Read JSON file
    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"Error: JSON file not found: {json_path}")
        sys.exit(1)
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        sys.exit(1)
    
    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = json_path.parent / 'chunks'
    
    # Create output directory if it doesn't exist
    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get document name from metadata
    document_name = data.get('metadata', {}).get('document_name', '')
    if not document_name:
        # Fallback to extracting from input_file if document_name not available
        input_file = data.get('metadata', {}).get('input_file', '')
        if not input_file:
            print("Error: No document_name or input_file found in metadata")
            sys.exit(1)
        document_name = get_base_filename(input_file)
    
    document_name = sanitize_filename(document_name)
    
    if args.dry_run:
        print(f"Dry run mode - would generate files with document name: {document_name}")
        print(f"Output directory: {output_dir}")
        print()
    
    # Process structure
    structure = data.get('structure', [])
    if not structure:
        print("Error: No structure found in JSON")
        sys.exit(1)
    
    # Generate markdown files
    try:
        if args.dry_run:
            print("Files that would be generated:")
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
                        print(f"  {filepath}")
                        print(f"    Hierarchy: {hierarchy_str}")
                        count += 1
                    if 'children' in item:
                        count += dry_run_traverse(item['children'], current_path)
                return count
            
            total_files = dry_run_traverse(structure)
            print(f"\nTotal files that would be generated: {total_files}")
        else:
            generated_files = traverse_and_generate(structure, document_name, output_dir)
            print(f"\nSuccessfully generated {len(generated_files)} markdown files in {output_dir}")
    
    except Exception as e:
        print(f"Error generating markdown files: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
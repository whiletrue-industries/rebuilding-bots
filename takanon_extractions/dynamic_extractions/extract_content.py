#!/usr/bin/env python3
"""
Extract content for specific section types from HTML files based on structure JSON.
"""

import argparse
import json
import sys
from pathlib import Path
from bs4 import BeautifulSoup
from markdownify import markdownify as md

def extract_content_for_sections(html_content, structure_data, target_content_type):
    """
    Extract complete content for sections of the specified type.
    
    Args:
        html_content: Raw HTML content
        structure_data: Parsed JSON structure
        target_content_type: Type of content to extract (e.g., "סעיף")
    
    Returns:
        Updated structure with content added
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all sections with html_id in the structure
    sections_with_ids = []
    
    def collect_sections(items):
        for item in items:
            if item.get('html_id'):
                sections_with_ids.append(item)
            if 'children' in item:
                collect_sections(item['children'])
    
    collect_sections(structure_data['structure'])
    
    # Sort sections by their position in the HTML
    sections_with_positions = []
    for section in sections_with_ids:
        html_id = section['html_id']
        element = soup.find(id=html_id)
        if element:
            # Find the position of this element in the document
            position = 0
            for elem in soup.find_all():
                if elem == element:
                    break
                position += 1
            sections_with_positions.append((section, element, position))
    
    # Sort by position in document
    sections_with_positions.sort(key=lambda x: x[2])
    
    # Extract content for target sections
    for i, (section, element, position) in enumerate(sections_with_positions):
        if section.get('section_type') == target_content_type:
            # Find the content boundaries
            start_element = element
            
            # Find the next section element to determine boundaries
            next_section_element = None
            if i + 1 < len(sections_with_positions):
                next_section_element = sections_with_positions[i + 1][1]
            
            # Extract content between this section and the next
            content_elements = []
            current = start_element
            
            while current:
                content_elements.append(current)
                current = current.next_sibling
                
                # Stop if we hit the next section
                if current and next_section_element and current == next_section_element:
                    break
                    
                # Stop if we hit another section with selflink class
                if (current and hasattr(current, 'get') and 
                    current.get('class') and 'selflink' in current.get('class', [])):
                    break
            
            # Convert collected elements to HTML string
            content_html = ""
            for elem in content_elements:
                if hasattr(elem, 'get_text'):  # It's a tag
                    content_html += str(elem)
                elif hasattr(elem, 'strip'):  # It's a string/text
                    content_html += str(elem)
            
            # Clean up and convert to markdown
            if content_html.strip():
                # Remove extra whitespace and clean up
                content_html = content_html.strip()
                
                # Convert to markdown
                markdown_content = md(content_html, heading_style="ATX")
                
                # Clean up the markdown
                markdown_content = markdown_content.strip()
                
                # Add the content to the section
                section['content'] = markdown_content
    
    return structure_data

def main():
    parser = argparse.ArgumentParser(description='Extract content for specific section types from HTML files')
    parser.add_argument('html_file', help='Path to the HTML file')
    parser.add_argument('structure_file', help='Path to the JSON structure file')
    parser.add_argument('content_type', help='Type of content to extract (e.g., "סעיף")')
    parser.add_argument('--output', '-o', help='Output file path (default: add _content suffix)')
    
    args = parser.parse_args()
    
    # Read HTML file
    html_path = Path(args.html_file)
    if not html_path.exists():
        print(f"Error: HTML file not found: {html_path}")
        sys.exit(1)
    
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
    except Exception as e:
        print(f"Error reading HTML file: {e}")
        sys.exit(1)
    
    # Read structure file
    structure_path = Path(args.structure_file)
    if not structure_path.exists():
        print(f"Error: Structure file not found: {structure_path}")
        sys.exit(1)
    
    try:
        with open(structure_path, 'r', encoding='utf-8') as f:
            structure_data = json.load(f)
    except Exception as e:
        print(f"Error reading structure file: {e}")
        sys.exit(1)
    
    # Extract content
    try:
        updated_structure = extract_content_for_sections(html_content, structure_data, args.content_type)
    except Exception as e:
        print(f"Error extracting content: {e}")
        sys.exit(1)
    
    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = structure_path.with_name(structure_path.stem + '_content.json')
    
    # Write output
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(updated_structure, f, ensure_ascii=False, indent=2)
        print(f"Content extracted and saved to: {output_path}")
    except Exception as e:
        print(f"Error writing output file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
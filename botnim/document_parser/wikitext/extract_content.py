#!/usr/bin/env python3
"""
Extract content for specific section types from HTML files based on structure JSON.
"""

import json
from pathlib import Path
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from botnim.config import get_logger
from urllib.parse import unquote
import re

# Logger setup
logger = get_logger(__name__)

def decode_markdown_links(text, mediawiki_mode=False, input_url=None):
    # This regex finds markdown links: [text](url)
    if input_url is not None:
        text = re.sub(
            r'\((#[^)]+)\)',
            lambda m: f'({input_url}{m.group(1)})',
            text
        )
    if mediawiki_mode:
        text = re.sub(
            r'\(/wiki/',
            r'\(https://he.wikisource.org/wiki/',
            text
        )
    return re.sub(
        r'\((https?://[^)]+)\)',
        lambda m: f'({unquote(m.group(1))})',
        text
    )

def extract_content_for_sections(html_content, structure_data, target_content_type, mediawiki_mode=False, input_url=None):
    """
    Extract complete content for sections of the specified type.
    
    Args:
        html_content: Raw HTML content
        structure_data: Parsed JSON structure
        target_content_type: Type of content to extract (e.g., "סעיף")
        mediawiki_mode: If True, apply MediaWiki-specific heuristics (e.g., selflink class)
    
    Returns:
        Updated structure with content added
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Collect all sections with html_id using a generator
    def collect_sections(items):
        for item in items:
            if item.get('html_id'):
                yield item
            if 'children' in item:
                yield from collect_sections(item['children'])
    sections_with_ids = list(collect_sections(structure_data['structure']))

    # Sort sections by their position in the HTML
    all_ids = [el['id'] for el in soup.find_all(attrs={'id': True})]
    def section_sort_key(section):
        try:
            return all_ids.index(section['html_id'])
        except (ValueError, KeyError, TypeError):
            logger.warning(f"html_id '{section['html_id']}' from structure not found in HTML.")
            return float('inf')
    sections_with_ids.sort(key=section_sort_key)

    # Extract content for target sections
    for section in sections_with_ids:
        if section.get('section_type') == target_content_type:
            html_id = section['html_id']
            if html_id not in all_ids:
                continue  # Skip sections whose id is not found in HTML
            element = soup.find(id=html_id)
            if not element:
                continue
            # Find the content boundaries
            start_element = element
            # Find the next section element to determine boundaries
            next_section_element = None
            idx = sections_with_ids.index(section)
            if idx + 1 < len(sections_with_ids):
                next_html_id = sections_with_ids[idx + 1]['html_id']
                next_section_element = soup.find(id=next_html_id) if next_html_id in all_ids else None
            # Extract content between this section and the next
            content_elements = []
            current = start_element
            while current:
                content_elements.append(current)
                current = current.next_sibling
                # Stop if we hit the next section
                if current and next_section_element and current == next_section_element:
                    break
                # MediaWiki-specific: Stop if we hit another section with selflink class
                if mediawiki_mode and current and hasattr(current, 'get') and current.get('class') and 'selflink' in current.get('class', []):
                    # This is a MediaWiki-specific heuristic to avoid navigation/TOC elements
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
                # Decode percent-encoded URLs in markdown links
                markdown_content = decode_markdown_links(markdown_content, mediawiki_mode, input_url)
                # Add the content to the section
                section['content'] = markdown_content
    
    return structure_data

def extract_content_from_html(html_path, structure_path, content_type, output_path, mediawiki_mode=False, input_url=None):
    """
    Pipeline-friendly function to extract content for sections from HTML and structure files.
    Args:
        html_path: Path to the HTML file (str or Path)
        structure_path: Path to the JSON structure file (str or Path)
        content_type: Type of content to extract (e.g., "סעיף")
        output_path: Path to write the output JSON (str or Path)
        mediawiki_mode: If True, apply MediaWiki-specific heuristics (e.g., selflink class)
    Raises:
        FileNotFoundError, ValueError, or IOError on error
    """
    logger.info("Starting content extraction (pipeline-friendly function)")
    html_path = Path(html_path)
    structure_path = Path(structure_path)
    output_path = Path(output_path)

    if not html_path.exists():
        logger.error(f"HTML file not found: {html_path}")
        raise FileNotFoundError(f"HTML file not found: {html_path}")
    try:
        logger.info(f"Reading HTML file: {html_path}")
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        logger.info(f"HTML file read successfully ({len(html_content)} characters)")
    except Exception as e:
        logger.error(f"Error reading HTML file: {e}")
        raise

    if not structure_path.exists():
        logger.error(f"Structure file not found: {structure_path}")
        raise FileNotFoundError(f"Structure file not found: {structure_path}")
    try:
        logger.info(f"Reading structure file: {structure_path}")
        with open(structure_path, 'r', encoding='utf-8') as f:
            structure_data = json.load(f)
        logger.info("Structure file read successfully")
    except Exception as e:
        logger.error(f"Error reading structure file: {e}")
        raise

    try:
        logger.info(f"Extracting content for type: {content_type}")
        updated_structure = extract_content_for_sections(html_content, structure_data, content_type, mediawiki_mode=mediawiki_mode, input_url=input_url)
        logger.info("Content extraction completed")
    except Exception as e:
        logger.error(f"Error extracting content: {e}")
        raise

    try:
        logger.info(f"Writing output to: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(updated_structure, f, ensure_ascii=False, indent=2)
        logger.info(f"Content extracted and saved to: {output_path}")
    except Exception as e:
        logger.error(f"Error writing output file: {e}")
        raise 
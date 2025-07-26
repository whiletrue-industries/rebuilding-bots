"""
CSV output functionality for PDF extraction pipeline.

This module provides functions for converting extracted PDF data into CSV format
with proper UTF-8 encoding and Hebrew text handling. It includes functions for
both local CSV file generation and Google Sheets data preparation.
"""

import csv
import os
from datetime import datetime
from typing import List, Dict
import argparse
import sys
import json
from botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_extraction_config import PDFExtractionConfig
from botnim.document_parser.dynamic_extractions.pdf_extraction.text_extraction import fix_hebrew_text_direction
from botnim.document_parser.dynamic_extractions.pdf_extraction.text_extraction import reverse_hebrew_line_order

def write_csv(data: List[Dict], fieldnames: List[str], source_name: str, output_dir: str = ".") -> str:
    """
    Write a list of dicts to a CSV file with UTF-8 encoding and correct column order.
    
    Creates a timestamped CSV file with proper UTF-8 encoding for Hebrew text.
    The filename includes the source name and timestamp for easy identification.
    
    Args:
        data: List of dictionaries, each representing a document's extracted data
        fieldnames: List of field names that define the column order
        source_name: Name of the source (used in filename generation)
        output_dir: Directory to save the CSV file (default: current directory)
        
    Returns:
        Path to the saved CSV file
        
    Raises:
        OSError: If the output directory doesn't exist or is not writable
        UnicodeEncodeError: If there are encoding issues with the data
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_source = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in source_name)
    filename = f"{safe_source}_{timestamp}.csv"
    output_path = os.path.join(output_dir, filename)
    
    with open(output_path, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in data:
            # Ensure all fields are present
            row_out = {k: row.get(k, "") for k in fieldnames}
            writer.writerow(row_out)
    
    return output_path

def flatten_for_csv(doc: dict, fieldnames: List[str]) -> dict:
    """
    Flatten a document dictionary for CSV output.
    
    Extracts field values from the document structure, prioritizing the 'fields'
    section over 'metadata' section. This ensures consistent data structure
    for CSV output.
    
    Args:
        doc: Document dictionary with 'fields' and 'metadata' sections
        fieldnames: List of field names to extract
        
    Returns:
        Dictionary with field names as keys and extracted values as values
        
    Example:
        >>> doc = {
        ...     "fields": {"title": "Test Title", "content": "Test Content"},
        ...     "metadata": {"source_url": "http://example.com"}
        ... }
        >>> flatten_for_csv(doc, ["title", "content", "source_url"])
        {'title': 'Test Title', 'content': 'Test Content', 'source_url': 'http://example.com'}
    """
    row = {}
    for field in fieldnames:
        # Prefer 'fields', fallback to 'metadata'
        value = doc.get("fields", {}).get(field, "")
        if not value:
            value = doc.get("metadata", {}).get(field, "")
        row[field] = value
    return row

def flatten_for_sheets(doc: dict, fieldnames: List[str]) -> List[str]:
    """
    Flatten a document dictionary for Google Sheets upload.
    
    Similar to flatten_for_csv but returns a list of values in the same order
    as fieldnames, and applies Hebrew text direction fixes for full text fields.
    
    Args:
        doc: Document dictionary with 'fields' and 'metadata' sections
        fieldnames: List of field names to extract (defines the order)
        
    Returns:
        List of string values in the same order as fieldnames
        
    Example:
        >>> doc = {
        ...     "fields": {"title": "Test Title", "טקסט_מלא": "שלום עולם"},
        ...     "metadata": {"source_url": "http://example.com"}
        ... }
        >>> flatten_for_sheets(doc, ["title", "טקסט_מלא", "source_url"])
        ['Test Title', 'עולם שלום', 'http://example.com']
    """
    row = []
    for field in fieldnames:
        # Prefer 'fields', fallback to 'metadata'
        value = doc.get("fields", {}).get(field, "")
        if not value:
            value = doc.get("metadata", {}).get(field, "")
        
        # Fix word order for full text fields
        if field in ["טקסט_מלא", "full_text"] and value:
            value = reverse_hebrew_line_order(str(value))
        
        row.append(str(value) if value is not None else "")
    
    return row

def main():
    parser = argparse.ArgumentParser(description="Write a list of dicts to a CSV file with UTF-8 encoding, using config schema.")
    parser.add_argument("--input", required=True, help="Path to input JSON file (list of dicts)")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--source", required=True, help="Source name as defined in the config")
    parser.add_argument("--output-dir", default=".", help="Directory to save the CSV (default: current dir)")
    args = parser.parse_args()

    try:
        with open(args.input, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("Input JSON must be a list of dicts.")
        config = PDFExtractionConfig.from_yaml(args.config)
        source = next((s for s in config.sources if s.name == args.source), None)
        if not source:
            raise ValueError(f"Source '{args.source}' not found in config.")
        fieldnames = [f.name for f in source.fields]
        flat_data = []
        for i, doc in enumerate(data):
            row = flatten_for_csv(doc, fieldnames)
            missing = [f for f in fieldnames if not row.get(f)]
            if missing:
                print(f"Warning: Document {i} is missing fields: {missing}", file=sys.stderr)
            flat_data.append(row)
        output_path = write_csv(flat_data, fieldnames, args.source, args.output_dir)
        print(f"CSV written to: {output_path}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 
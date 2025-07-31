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

def write_csv(data: List[Dict], fieldnames: List[str], source_name: str, output_dir: str = ".") -> str:
    """
    Write a list of dicts to a CSV file with UTF-8 encoding and correct column order.
    
    Creates an output.csv file with proper UTF-8 encoding for Hebrew text.
    The file is written to the specified output directory.
    
    Args:
        data: List of dictionaries, each representing a document's extracted data
        fieldnames: List of field names that define the column order
        source_name: Name of the source (used for logging)
        output_dir: Directory to save the CSV file (default: current directory)
        
    Returns:
        Path to the saved CSV file
        
    Raises:
        OSError: If the output directory doesn't exist or is not writable
        UnicodeEncodeError: If there are encoding issues with the data
    """
    output_path = os.path.join(output_dir, "output.csv")
    
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

 
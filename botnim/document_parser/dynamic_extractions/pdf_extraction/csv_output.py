"""
CSV input/output utilities for PDF extraction pipeline.

This module provides functions for reading and writing CSV files
following the CSV contract pattern.
"""

import csv
import os
from typing import List, Dict, Any
from pathlib import Path

def read_csv(csv_path: str) -> List[Dict[str, Any]]:
    """
    Read data from a CSV file.
    
    Args:
        csv_path: Path to CSV file
        
    Returns:
        List of dictionaries representing rows
    """
    data = []
    
    if not os.path.exists(csv_path):
        return data
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
        
        return data
    except Exception as e:
        print(f"Error reading CSV file {csv_path}: {e}")
        return []

def write_csv(data: List[Dict[str, Any]], output_path: str) -> str:
    """
    Write data to a CSV file.
    
    Args:
        data: List of dictionaries to write
        output_path: Path to output CSV file
        
    Returns:
        Path to the written CSV file
    """
    if not data:
        print("No data to write to CSV")
        return output_path
    
    try:
        # Get fieldnames from the first row
        fieldnames = list(data[0].keys())
        
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"Wrote {len(data)} records to {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Error writing CSV file {output_path}: {e}")
        return output_path

def flatten_for_csv(data: Dict[str, Any], fieldnames: List[str]) -> Dict[str, Any]:
    """
    Flatten nested data structure for CSV output.
    
    Args:
        data: Nested data structure
        fieldnames: List of field names to include
        
    Returns:
        Flattened dictionary
    """
    flattened = {}
    
    for field in fieldnames:
        if field in data:
            value = data[field]
            # Convert complex types to strings
            if isinstance(value, (dict, list)):
                flattened[field] = str(value)
            else:
                flattened[field] = value
        else:
            flattened[field] = ""
    
    return flattened

 
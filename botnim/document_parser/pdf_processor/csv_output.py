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
        # Get all unique fieldnames from all records
        all_fieldnames = set()
        for record in data:
            all_fieldnames.update(record.keys())
        
        # Ensure URL and revision columns are always present
        required_columns = ['url', 'revision']
        for col in required_columns:
            all_fieldnames.add(col)
        
        fieldnames = sorted(list(all_fieldnames))
        
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"Wrote {len(data)} records to {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Error writing CSV file {output_path}: {e}")
        return output_path

def write_csv_by_source(data: List[Dict[str, Any]], output_dir: str, source_configs: List[Dict]) -> Dict[str, str]:
    """
    Write separate CSV files for each source with only their relevant fields.
    
    Args:
        data: List of dictionaries to write
        output_dir: Directory to write CSV files
        source_configs: List of source configurations with field definitions
        
    Returns:
        Dictionary mapping source names to their CSV file paths
    """
    if not data:
        print("No data to write to CSV")
        return {}
    
    # Group data by source
    source_data = {}
    for record in data:
        source_name = record.get('source_name', 'unknown')
        if source_name not in source_data:
            source_data[source_name] = []
        source_data[source_name].append(record)
    
    # Create source config lookup
    source_config_lookup = {}
    for config in source_configs:
        source_config_lookup[config['name']] = config
    
    csv_files = {}
    
    for source_name, records in source_data.items():
        if source_name not in source_config_lookup:
            print(f"Warning: No config found for source '{source_name}', skipping")
            continue
            
        config = source_config_lookup[source_name]
        
        # Get field names from config
        config_fieldnames = [field['name'] for field in config.get('fields', [])]
        
        # Add common metadata fields including URL and revision tracking
        all_fieldnames = [
            'source_name', 'url', 'extraction_date', 'input_file',
            'revision', 'title', 'date'  # Open Budget tracking fields
        ] + config_fieldnames
        
        # Filter records to only include relevant fields
        filtered_records = []
        for record in records:
            filtered_record = {}
            for field in all_fieldnames:
                filtered_record[field] = record.get(field, '')
            filtered_records.append(filtered_record)
        
        # Create filename
        safe_source_name = source_name.replace('/', '_').replace('\\', '_').replace(':', '_')
        csv_filename = f"{safe_source_name}.csv"
        csv_path = os.path.join(output_dir, csv_filename)
        
        # Write CSV
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=all_fieldnames, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(filtered_records)
            
            print(f"Wrote {len(filtered_records)} records for '{source_name}' to {csv_path}")
            csv_files[source_name] = csv_path
            
        except Exception as e:
            print(f"Error writing CSV file for source '{source_name}': {e}")
    
    return csv_files

def flatten_for_csv(data: Dict[str, Any], fieldnames: List[str]) -> Dict[str, Any]:
    """
    Flatten nested data structure for CSV output.
    
    Args:
        data: Nested data structure with 'fields' and 'metadata' keys
        fieldnames: List of field names to include
        
    Returns:
        Flattened dictionary
    """
    flattened = {}
    
    # Extract fields from nested structure
    fields = data.get("fields", {})
    metadata = data.get("metadata", {})
    
    for field in fieldnames:
        # Check in fields first, then metadata
        if field in fields:
            value = fields[field]
        elif field in metadata:
            value = metadata[field]
        else:
            value = ""
        
        # Convert complex types to strings
        if isinstance(value, (dict, list)):
            flattened[field] = str(value)
        else:
            flattened[field] = value
    
    return flattened

 
import csv
import os
from datetime import datetime
from typing import List, Dict
import argparse
import sys
import json
from botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_extraction_config import PDFExtractionConfig

def write_csv(data: List[Dict], fieldnames: List[str], source_name: str, output_dir: str = ".") -> str:
    """
    Write a list of dicts to a CSV file with UTF-8 encoding and correct column order.
    Args:
        data: List of dicts (one per document)
        fieldnames: List of field names (column order)
        source_name: Name of the source (used in filename)
        output_dir: Directory to save the CSV (default: current dir)
    Returns:
        Path to the saved CSV file.
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
    row = {}
    for field in fieldnames:
        # Prefer 'fields', fallback to 'metadata'
        value = doc.get("fields", {}).get(field, "")
        if not value:
            value = doc.get("metadata", {}).get(field, "")
        row[field] = value
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
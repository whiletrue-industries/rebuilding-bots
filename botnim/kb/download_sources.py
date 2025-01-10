import logging
import requests
from pathlib import Path
import os

logger = logging.getLogger(__name__)

def download_and_convert_spreadsheet(source_url: str, target_dir: Path, context_name: str) -> None:
    """Download and convert Google Spreadsheet data to individual markdown files"""
    try:
        sheet_id = source_url.split('/d/')[1].split('/')[0]
        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv'
        
        response = requests.get(url)
        response.encoding = 'utf-8'  # Explicitly set response encoding
        response.raise_for_status()
        
        print("Raw response text:")
        print(response.text[:500])  # Print first 500 chars to see the structure
        
        # Use csv module to properly handle quoted fields with commas
        import csv
        from io import StringIO
        
        csv_file = StringIO(response.text)
        csv_reader = csv.reader(csv_file)
        
        # Get headers and remove empty ones
        headers = next(csv_reader)
        headers = [h.strip() for h in headers if h.strip()]
        
        # Process all content rows
        for i, columns in enumerate(csv_reader):
            # Clean up whitespace and match length with headers
            columns = [col.strip() for col in columns[:len(headers)]]
            
            if not columns[0]:  # Skip empty entries
                continue
                
            # Create markdown entry
            entry = []
            # Add each non-empty column with its header
            for header, value in zip(headers, columns):
                if value:  # Only add non-empty values
                    entry.append(f"{header}:\n{value}\n\n")  # Each field on new line for clarity
            
            # Use context name for file naming
            sanitized_name = context_name.replace(' ', '_')
            output_path = os.path.join(target_dir, f"{sanitized_name}_{i+1:03d}.md")
            with open(output_path, 'w', encoding='utf-8', newline='') as f:
                f.writelines(entry)
            
        logger.info(f"Successfully downloaded and split source to: {target_dir}")
        
    except Exception as e:
        logger.error(f"Failed to download/convert source {source_url}: {str(e)}")
        raise

def download_sources(specs_dir: Path, bot_filter: str = 'all'):
    """Download all external sources defined in bot configurations"""
    for config_file in specs_dir.glob('*/config.yaml'):
        bot_name = config_file.parent.name
        if bot_filter != 'all' and bot_name != bot_filter:
            continue
            
        import yaml
        with config_file.open() as f:
            config = yaml.safe_load(f)
            
        if config.get('context'):
            for context in config['context']:
                if 'split' in context and 'source' in context:
                    # Create directory instead of file
                    target_dir = config_file.parent / context['split'].replace('.txt', '')
                    target_dir.mkdir(exist_ok=True)
                    logger.info(f"Downloading source for {context['name']} to {target_dir}")
                    download_and_convert_spreadsheet(context['source'], target_dir, context['name']) 
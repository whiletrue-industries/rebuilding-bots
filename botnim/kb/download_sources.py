import logging
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

def download_and_convert_spreadsheet(source_url: str, target_file: Path) -> None:
    """Download and convert Google Spreadsheet data to markdown format"""
    try:
        sheet_id = source_url.split('/d/')[1].split('/')[0]
        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv'
        
        response = requests.get(url)
        response.raise_for_status()
        
        # Use csv module to properly handle quoted fields with commas
        import csv
        from io import StringIO
        
        csv_file = StringIO(response.text)
        csv_reader = csv.reader(csv_file)
        
        # Skip the header row entirely
        next(csv_reader)
        
        markdown_content = []
        
        # Process all content rows
        for columns in csv_reader:
            # Clean up whitespace
            columns = [col.strip() for col in columns]
            
            # Create markdown entry
            entry = []
            if columns[0]:  # First column is the main content
                entry.append(f"{columns[0]}\n")
            if len(columns) > 1 and columns[1]:  # Second column is the reference
                entry.append(f"קשור לסעיף {columns[1]}\n")
            
            if entry:  # Only add if there's content
                markdown_content.append('\n'.join(entry))
                markdown_content.append('\n---\n')

        target_file.write_text('\n'.join(markdown_content), encoding='utf-8')
        logger.info(f"Successfully downloaded and converted source to: {target_file}")
        
    except Exception as e:
        logger.error(f"Failed to download/convert source {source_url}: {str(e)}")
        raise

def download_sources(specs_dir: Path):
    """Download all external sources defined in bot configurations"""
    for config_file in specs_dir.glob('*/config.yaml'):
        import yaml
        with config_file.open() as f:
            config = yaml.safe_load(f)
            
        if config.get('context'):
            for context in config['context']:
                if 'split' in context and 'source' in context:
                    target_file = config_file.parent / context['split']
                    logger.info(f"Downloading source for {context['name']} to {target_file}")
                    download_and_convert_spreadsheet(context['source'], target_file) 
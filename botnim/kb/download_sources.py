import logging
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

def download_and_convert_spreadsheet(source_url: str, target_dir: Path, context_name: str) -> None:
    """Download and convert Google Spreadsheet data to individual markdown files"""
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
        
        # Process all content rows
        for i, columns in enumerate(csv_reader):
            # Clean up whitespace
            columns = [col.strip() for col in columns]
            
            if not columns[0]:  # Skip empty entries
                continue
                
            # Create markdown entry
            entry = []
            entry.append(f"{columns[0]}\n")
            if len(columns) > 1 and columns[1]:
                entry.append(f"קשור לסעיף {columns[1]}\n")
            
            # Use context name for file naming
            sanitized_name = context_name.replace(' ', '_')
            file_name = target_dir / f'{sanitized_name}_{i:03d}.md'
            file_name.write_text('\n'.join(entry), encoding='utf-8')
            
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
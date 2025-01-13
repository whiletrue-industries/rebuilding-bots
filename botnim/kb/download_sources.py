from pathlib import Path
import os
import requests
import csv
from io import StringIO
import yaml
from ..config import get_logger
from typing import List, Tuple, BinaryIO
import io

logger = get_logger(__name__)

def download_and_convert_spreadsheet(source_url: str, target_dir: Path, context_name: str) -> List[Tuple[str, BinaryIO, str]]:
    """Download and convert Google Spreadsheet data to memory buffers and cache files
    
    Args:
        source_url: URL of the Google Spreadsheet
        target_dir: Directory to store cache files
        context_name: Name of the context for file naming
        
    Returns:
        List of tuples (filename, file_buffer, content_type) for new or modified entries
    """
    try:
        sheet_id = source_url.split('/d/')[1].split('/')[0]
        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv'
        
        response = requests.get(url)
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        # Load existing files for comparison
        existing_files = {}
        if target_dir.exists():
            for file_path in target_dir.glob('*.md'):
                existing_files[file_path.name] = file_path.read_text()
        
        csv_file = StringIO(response.text)
        csv_reader = csv.reader(csv_file)
        
        headers = next(csv_reader)
        headers = [h.strip() for h in headers if h.strip()]
        
        documents = []
        target_dir.mkdir(exist_ok=True)
        
        for i, columns in enumerate(csv_reader):
            columns = [col.strip() for col in columns[:len(headers)]]
            
            if not columns[0]:  # Skip empty entries
                continue
                
            entry = []
            for header, value in zip(headers, columns):
                if value:
                    entry.append(f"{header}:\n{value}\n\n")
                    
            content = ''.join(entry)
            if content.strip():
                filename = f"{context_name}_{i+1:03d}.md"
                file_path = target_dir / filename
                
                # Always write to cache file
                file_path.write_text(content)
                
                # Only include in documents if content changed or new
                if filename not in existing_files or existing_files[filename] != content:
                    documents.append((
                        filename,
                        io.BytesIO(content.encode('utf-8')),
                        'text/markdown'
                    ))
                    logger.info(f"New/modified entry: {filename}")
        
        logger.info(f"Found {len(documents)} new/modified entries from spreadsheet")
        return documents
        
    except Exception as e:
        logger.error(f"Failed to download/convert source {source_url}: {str(e)}")
        raise

def download_sources(specs_dir: Path, bot_filter: str = 'all'):
    """Download all external sources defined in bot configurations"""
    for config_file in specs_dir.glob('*/config.yaml'):
        bot_name = config_file.parent.name
        if bot_filter != 'all' and bot_name != bot_filter:
            continue
            
        with config_file.open() as f:
            config = yaml.safe_load(f)
            
        if config.get('context'):
            for context in config['context']:
                if context.get('type') == 'spreadsheet' and 'source' in context:
                    target_dir = config_file.parent / f"{context['name']}_split"
                    target_dir.mkdir(exist_ok=True)
                    logger.info(f"Downloading source for {context['name']} to {target_dir}")
                    download_and_convert_spreadsheet(context['source'], target_dir, context['name'])
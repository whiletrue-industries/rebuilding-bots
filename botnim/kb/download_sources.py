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

def download_and_convert_spreadsheet(source_url: str, context_name: str) -> List[Tuple[str, BinaryIO, str]]:
    """Download and convert Google Spreadsheet data to memory buffers
    
    Args:
        source_url: URL of the Google Spreadsheet
        context_name: Name of the context for file naming
        
    Returns:
        List of tuples (filename, file_buffer, content_type) for entries
    """
    try:
        logger.info(f"Downloading spreadsheet from {source_url}")
        sheet_id = source_url.split('/d/')[1].split('/')[0]
        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv'
        
        response = requests.get(url)
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        logger.debug("Raw response text (first 500 chars):")
        logger.debug(response.text[:500])
        
        csv_file = StringIO(response.text)
        csv_reader = csv.reader(csv_file)
        
        headers = next(csv_reader)
        headers = [h.strip() for h in headers if h.strip()]
        
        documents = []
        
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
                
                # Create memory buffer for OpenAI
                file_obj = io.BytesIO(content.encode('utf-8'))
                file_obj.name = filename
                documents.append((
                    filename,
                    file_obj,
                    'text/markdown'
                ))
        
        logger.info(f"Processed {len(documents)} entries from spreadsheet")
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
                    download_and_convert_spreadsheet(context['source'], context['name'])
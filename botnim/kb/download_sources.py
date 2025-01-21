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

def process_document(source: str, context_name: str, split: bool = False) -> List[Tuple[str, BinaryIO, str]]:
    """Process a document from either URL or local file
    
    Args:
        source: URL or local file path
        context_name: Name of the context for file naming
        split: Whether to split the document on '---' markers
    """
    try:
        # Determine if source is URL or local file
        is_url = source.startswith(('http://', 'https://', 'gs://'))
        
        if is_url:
            if 'spreadsheet' in source:
                return download_and_convert_spreadsheet(source, context_name)
            else:
                response = requests.get(source)
                response.raise_for_status()
                content = response.text
        else:
            with open(source, 'r') as f:
                content = f.read()

        if not split:
            # Return as single document with .md extension
            filename = f"{context_name}.md"  # Explicitly add .md extension
            file_obj = io.BytesIO(content.encode('utf-8'))
            file_obj.name = filename  # Set name on file object
            return [(filename, file_obj, 'text/markdown')]
        
        # Split content on '---' markers
        sections = content.split('\n---\n')
        documents = []
        for i, section in enumerate(sections):
            if section.strip():
                filename = f"{context_name}_{i+1:03d}.md"  # Explicitly add .md extension
                file_obj = io.BytesIO(section.strip().encode('utf-8'))
                file_obj.name = filename  # Set name on file object
                documents.append((filename, file_obj, 'text/markdown'))
        
        return documents

    except Exception as e:
        logger.error(f"Failed to process source {source}: {str(e)}")
        raise

def download_sources(specs_dir: Path, bot_filter: str = 'all', debug: bool = False):
    """Download all external sources defined in bot configurations
    
    Args:
        specs_dir: Directory containing bot specifications
        bot_filter: Bot name to process, or 'all'
        debug: If True, save downloaded files locally
    """
    for config_file in specs_dir.glob('*/config.yaml'):
        bot_name = config_file.parent.name
        if bot_filter != 'all' and bot_name != bot_filter:
            continue
            
        with config_file.open() as f:
            config = yaml.safe_load(f)
            
        if config.get('context'):
            for context in config['context']:
                if 'source' in context:
                    documents = process_document(
                        context['source'],
                        context['name'],
                        context.get('split', False)
                    )
                    
                    if debug:
                        target_dir = config_file.parent / f"{context['name']}_debug"
                        target_dir.mkdir(exist_ok=True)
                        for filename, file_obj, _ in documents:
                            with open(target_dir / filename, 'wb') as f:
                                f.write(file_obj.read())
                            file_obj.seek(0)  # Reset position for later use
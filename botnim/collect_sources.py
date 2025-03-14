import io
import pathlib
import dataflows as DF
import mimetypes
import requests
import json
from pathlib import Path
import os
import glob
from .config import SPECS, get_logger

logger = get_logger(__name__)

def collect_sources_files(config_dir, context_name, source):
    files = list(config_dir.glob(source))
    file_streams = [(f.name, f.open('rb'), 'text/markdown') for f in files]
    return file_streams

def collect_sources_split(config_dir, context_name, source):
    filename = config_dir / source
    content = filename.read_text()
    content = content.split('\n---\n')
    file_streams = [io.BytesIO(c.strip().encode('utf-8')) for c in content]
    file_streams = [(f'{context_name}_{i}.md', f, 'text/markdown') for i, f in enumerate(file_streams)]
    return file_streams

def collect_sources_google_spreadsheet(context_name, source):
    resources, dp, _ = DF.Flow(
        DF.load(source, name='rows'),
    ).results()
    rows = resources[0]
    headers = [f.name for f in dp.resources[0].schema.fields]
    file_streams = []
    for idx, row in enumerate(rows):
        content = ''
        if len(headers) > 1:
            for i, header in enumerate(headers):
                if row.get(header):
                    if i > 0:
                        content += f'{header}:\n{row[header]}\n\n'
                    else:
                        content += f'{row[header]}\n\n'
        if content:
            file_streams.append((f'{context_name}_{idx}.md', io.BytesIO(content.strip().encode('utf-8')), 'text/markdown'))
    return file_streams

def collect_context_sources(context_config, config_dir, extract_metadata=False):
    """
    Collect context sources from the config directory.
    """
    logger.info(f"Collecting context sources from {config_dir} with metadata extraction: {extract_metadata}")
    logger.info(f"Context config: {context_config}")
    
    sources = []
    
    # Handle the case where the context has 'type' and 'source' directly
    if 'type' in context_config and 'source' in context_config:
        source_type = context_config['type']
        source_path = context_config['source']
        context_name = context_config['slug']
        
        logger.info(f"Processing source of type: {source_type}, path: {source_path}")
        
        try:
            if source_type == 'files':
                # Handle glob pattern for files
                path_pattern = os.path.join(config_dir, source_path)
                logger.info(f"Looking for files matching pattern: {path_pattern}")
                
                matching_files = glob.glob(path_pattern)
                logger.info(f"Found {len(matching_files)} matching files")
                
                for file_path in matching_files:
                    try:
                        with open(file_path, 'rb') as f:
                            content = f.read()
                        mime_type, _ = mimetypes.guess_type(file_path)
                        
                        metadata = None
                        if extract_metadata:
                            metadata = {
                                'source_type': 'file',
                                'filename': os.path.basename(file_path),
                                'file_path': file_path,
                                'file_size': len(content),
                                'mime_type': mime_type
                            }
                        
                        sources.append((file_path, io.BytesIO(content), mime_type, metadata))
                        logger.info(f"Added file: {file_path}")
                    except Exception as e:
                        logger.error(f"Error processing file {file_path}: {str(e)}")
            
            elif source_type == 'google-spreadsheet':
                # Handle Google Spreadsheet
                url = source_path
                logger.info(f"Processing Google Spreadsheet: {url}")
                
                try:
                    # For now, we're just storing the source configuration as JSON
                    source_json = json.dumps({
                        'url': url,
                        'context_name': context_name
                    }).encode('utf-8')
                    
                    metadata = None
                    if extract_metadata:
                        metadata = {
                            'source_type': 'google_spreadsheet',
                            'url': url,
                            'context_name': context_name
                        }
                    
                    sources.append((f"{context_name}_spreadsheet.json", io.BytesIO(source_json), 'application/json', metadata))
                    logger.info(f"Added spreadsheet: {url}")
                except Exception as e:
                    logger.error(f"Error processing spreadsheet {url}: {str(e)}")
            
            else:
                logger.warning(f"Unsupported source type: {source_type}")
        
        except Exception as e:
            logger.error(f"Error processing source of type {source_type}: {str(e)}")
    
    # Handle the case where the context has a 'sources' list
    elif 'sources' in context_config:
        for source in context_config.get('sources', []):
            source_type = source.get('type')
            logger.info(f"Processing source of type: {source_type}, config: {source}")
            
            try:
                if source_type == 'file':
                    new_sources = collect_sources_file(source, config_dir, extract_metadata)
                    logger.info(f"Collected {len(new_sources)} file sources")
                    sources.extend(new_sources)
                elif source_type == 'directory':
                    new_sources = collect_sources_directory(source, config_dir, extract_metadata)
                    logger.info(f"Collected {len(new_sources)} directory sources")
                    sources.extend(new_sources)
                elif source_type == 'url':
                    new_sources = collect_sources_url(source, extract_metadata)
                    logger.info(f"Collected {len(new_sources)} URL sources")
                    sources.extend(new_sources)
                elif source_type == 'google_spreadsheet':
                    new_sources = collect_sources_google_spreadsheet(source, extract_metadata)
                    logger.info(f"Collected {len(new_sources)} spreadsheet sources")
                    sources.extend(new_sources)
                else:
                    logger.warning(f"Unknown source type: {source_type}")
            except Exception as e:
                logger.error(f"Error collecting sources of type {source_type}: {str(e)}")
    
    else:
        logger.warning(f"Context config does not have 'type'+'source' or 'sources' field: {context_config}")
    
    logger.info(f"Collected {len(sources)} total sources")
    return sources

def collect_sources_file(source, config_dir, extract_metadata=False):
    """
    Collect sources from a file.
    """
    path = Path(config_dir) / source['path']
    logger.info(f"Collecting file source from path: {path}")
    
    if not path.exists():
        logger.error(f"File not found: {path}")
        return []
    
    try:
        with open(path, 'rb') as f:
            content = f.read()
        mime_type, _ = mimetypes.guess_type(path)
        logger.info(f"Read file {path} with size {len(content)} and mime type {mime_type}")
        
        metadata = None
        if extract_metadata:
            metadata = {
                'source_type': 'file',
                'filename': str(path.name),
                'file_path': str(path),
                'file_size': len(content),
                'mime_type': mime_type
            }
            logger.info(f"Created metadata for file {path}")
        
        return [(str(path), io.BytesIO(content), mime_type, metadata)]
    except Exception as e:
        logger.error(f"Error reading file {path}: {str(e)}")
        return []

def collect_sources_directory(source, config_dir, extract_metadata=False):
    """
    Collect sources from a directory.
    """
    path = Path(config_dir) / source['path']
    logger.info(f"Collecting directory sources from path: {path}")
    
    if not path.exists():
        logger.error(f"Directory not found: {path}")
        return []
    
    if not path.is_dir():
        logger.error(f"Path is not a directory: {path}")
        return []
    
    sources = []
    try:
        file_count = 0
        for file_path in path.glob('**/*'):
            if file_path.is_file():
                file_count += 1
                try:
                    with open(file_path, 'rb') as f:
                        content = f.read()
                    mime_type, _ = mimetypes.guess_type(file_path)
                    
                    metadata = None
                    if extract_metadata:
                        metadata = {
                            'source_type': 'directory',
                            'filename': str(file_path.name),
                            'file_path': str(file_path),
                            'relative_path': str(file_path.relative_to(path)),
                            'file_size': len(content),
                            'mime_type': mime_type
                        }
                    
                    sources.append((str(file_path), io.BytesIO(content), mime_type, metadata))
                except Exception as e:
                    logger.error(f"Error reading file {file_path}: {str(e)}")
        
        logger.info(f"Found {file_count} files in directory {path}, successfully processed {len(sources)}")
        return sources
    except Exception as e:
        logger.error(f"Error processing directory {path}: {str(e)}")
        return []

def collect_sources_url(source, extract_metadata=False):
    """
    Collect sources from a URL.
    """
    url = source['url']
    logger.info(f"Collecting URL source from: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        mime_type = response.headers.get('content-type')
        logger.info(f"Retrieved URL {url} with size {len(response.content)} and mime type {mime_type}")
        
        metadata = None
        if extract_metadata:
            metadata = {
                'source_type': 'url',
                'url': url,
                'content_length': len(response.content),
                'mime_type': mime_type,
                'headers': dict(response.headers)
            }
            logger.info(f"Created metadata for URL {url}")
        
        return [(url, io.BytesIO(response.content), mime_type, metadata)]
    except Exception as e:
        logger.error(f"Error retrieving URL {url}: {str(e)}")
        return []

def collect_sources_google_spreadsheet(source, extract_metadata=False):
    """
    Collect sources from a Google Spreadsheet.
    """
    url = source['url']
    logger.info(f"Collecting Google Spreadsheet source from: {url}")
    
    try:
        # For now, we're just storing the source configuration as JSON
        source_json = json.dumps(source).encode('utf-8')
        
        metadata = None
        if extract_metadata:
            metadata = {
                'source_type': 'google_spreadsheet',
                'url': url,
                'sheet_id': source.get('sheet_id'),
                'title': source.get('title')
            }
            logger.info(f"Created metadata for spreadsheet {url}")
        
        return [(url, io.BytesIO(source_json), 'application/json', metadata)]
    except Exception as e:
        logger.error(f"Error processing spreadsheet {url}: {str(e)}")
        return []

def collect_all_sources(context_list, config_dir):
    all_sources = []
    for context in context_list:
        all_sources.append(dict(
            **context,
            file_streams=collect_context_sources(context, config_dir)
        ))
    return all_sources
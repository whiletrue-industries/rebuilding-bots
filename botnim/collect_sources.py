import io
import dataflows as DF
import mimetypes
import json
from pathlib import Path
import os
import glob
from datetime import datetime, timezone
from openai import OpenAI
from .config import get_logger
import dataflows as DF
import re
from .dynamic_extraction import extract_structured_content, determine_document_type

logger = get_logger(__name__)

def get_metadata_for_content(content: str, file_path: Path, title: str = None, section_idx: int = None) -> dict:
    """
    Extract metadata for a given content using LLM extraction.
    
    Args:
        content (str): The content to extract metadata from
        file_path (Path): Path to the source file (used for document type detection)
        title (str, optional): Default title to use if none extracted
        section_idx (int, optional): Section index if content is part of a larger file
    
    Returns:
        dict: Metadata dictionary containing extracted information and status
    """
    # Create basic metadata structure
    metadata = {
        "title": title or (f"{file_path.stem}" + (f" (Section {section_idx+1})" if section_idx is not None else "")),
        "status": "processed"
    }
    
    # Enhanced metadata using LLM extraction
    try:
        document_type = determine_document_type(file_path)
        logger.info(f"Extracting structured content for {file_path}{' section '+str(section_idx) if section_idx is not None else ''} with document type: {document_type}")
        
        # Get the extracted data
        extracted_data = extract_structured_content(content, document_type=document_type)
        
        # Add document type
        metadata['document_type'] = document_type
        
        # Use document title from extraction if available
        if extracted_data.get('DocumentMetadata', {}).get('DocumentTitle'):
            metadata['title'] = extracted_data['DocumentMetadata']['DocumentTitle']
        
        # Add all extracted data directly to metadata (not nested)
        metadata.update(extracted_data)
            
        logger.info(f"Added enhanced metadata for {file_path}{' section '+str(section_idx) if section_idx is not None else ''}")
    except Exception as e:
        logger.error(f"Error extracting structured content from {file_path}{' section '+str(section_idx) if section_idx is not None else ''}: {str(e)}")
        metadata['status'] = 'extraction_error'
        metadata['error'] = str(e)
    
    return metadata

def collect_sources_files(config_dir, context_name, source):
    files = list(config_dir.glob(source))
    file_streams = [(f.name, f.open('rb'), 'text/markdown') for f in files]
    return file_streams

def collect_sources_split(config_dir, context_name, source, extract_metadata=False):
    """
    Collect sources from a split file with enhanced metadata extraction.
    """
    try:
        filename = Path(config_dir) / source
        logger.info(f"Processing split file: {filename}")
        
        content = filename.read_text()
        sections = content.split('\n---\n')
        logger.info(f"Split file into {len(sections)} sections")
        
        sources = []
        for idx, section in enumerate(sections):
            if section.strip():
                file_name = f'{context_name}_{idx}.md'
                section_bytes = section.strip().encode('utf-8')
                mime_type = 'text/markdown'
                
                metadata = None
                if extract_metadata:
                    metadata = get_metadata_for_content(section, filename, section_idx=idx)
                
                sources.append((file_name, io.BytesIO(section_bytes), mime_type, metadata))
                logger.info(f"Added section {idx} as {file_name}")
        
        return sources
    except Exception as e:
        logger.error(f"Error processing split file {filename}: {str(e)}")
        return []

def collect_sources_google_spreadsheet(context_name, source, extract_metadata=False):
    """
    Collect sources from a Google Spreadsheet with enhanced metadata extraction.
    """
    try:        
        url = source
        logger.info(f"Processing Google Spreadsheet with dataflows: {url}")
        
        resources, dp, _ = DF.Flow(
            DF.load(url, name='rows'),
        ).results()
        
        rows = resources[0]
        headers = [f.name for f in dp.resources[0].schema.fields]
        
        logger.info(f"Loaded spreadsheet with {len(rows)} rows and headers: {headers}")
        
        sources = []
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
                file_name = f'{context_name}_{idx}.md'
                content_bytes = content.strip().encode('utf-8')
                mime_type = 'text/markdown'
                
                metadata = None
                if extract_metadata:
                    # Get first column value for title if available
                    first_header = headers[0] if headers else None
                    title = row.get(first_header, f"Row {idx}")
                    metadata = get_metadata_for_content(content, Path(file_name), title=title)
                
                sources.append((file_name, io.BytesIO(content_bytes), mime_type, metadata))
                logger.info(f"Added spreadsheet row {idx} as {file_name}")
        
        return sources
    except ImportError:
        logger.error("dataflows library not available, cannot process Google Spreadsheet")
        return []
    except Exception as e:
        logger.error(f"Error processing spreadsheet {url}: {str(e)}")
        return []

def collect_context_sources(context_config, config_dir, extract_metadata=False):
    """
    Collect context sources from the config directory.
    """
    logger.info(f"Collecting context sources from {config_dir} with metadata extraction: {extract_metadata}")
    logger.info(f"Context config: {context_config}")
    
    # Add detailed logging about the directory and files
    try:
        config_dir_path = Path(config_dir)
        logger.info(f"Config directory exists: {config_dir_path.exists()}")
        logger.info(f"Config directory is a directory: {config_dir_path.is_dir()}")
        
        if config_dir_path.exists() and config_dir_path.is_dir():
            # List files in the directory
            files = list(config_dir_path.glob("*"))
            logger.info(f"Files in config directory: {[str(f) for f in files]}")
    except Exception as e:
        logger.error(f"Error inspecting directory: {str(e)}")
    
    sources = []
    
    # Handle the case where the context has 'type' and 'source' directly
    if 'type' in context_config and 'source' in context_config:
        source_type = context_config['type']
        source_path = context_config['source']
        context_name = context_config['slug']
        
        logger.info(f"Processing source of type: {source_type}, path: {source_path}")
        
        try:
            if source_type == 'files':
                # Check if extraction is a directory
                extraction_dir_name = context_config.get('extraction_dir', 'extraction')
                extraction_dir = os.path.join(config_dir, extraction_dir_name)
                
                if os.path.isdir(extraction_dir):
                    logger.info(f"Found extraction directory: {extraction_dir}")
                    
                    # Use the original pattern but with the correct path
                    path_pattern = os.path.join(extraction_dir, '*.md')
                    logger.info(f"Looking for files matching pattern: {path_pattern}")
                    
                    matching_files = glob.glob(path_pattern)
                    logger.info(f"Found {len(matching_files)} matching files")
                    
                    for file_path in matching_files:
                        try:
                            with open(file_path, 'rb') as f:
                                content = f.read()
                            mime_type = 'text/markdown'
                            
                            metadata = None
                            if extract_metadata:
                                metadata = get_metadata_for_content(content.decode('utf-8'), Path(file_path))
                            
                            sources.append((file_path, io.BytesIO(content), mime_type, metadata))
                            logger.info(f"Added file: {file_path}")
                        except Exception as e:
                            logger.error(f"Error processing file {file_path}: {str(e)}")
                else:
                    logger.error(f"Extraction directory not found: {extraction_dir}")
            
            elif source_type == 'google-spreadsheet':
                # Use enhanced spreadsheet collection
                sources.extend(collect_sources_google_spreadsheet(context_name, source_path, extract_metadata))
            
            elif source_type == 'split':
                # Use enhanced split collection
                sources.extend(collect_sources_split(config_dir, context_name, source_path, extract_metadata))
            
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
    Collect sources from a file with enhanced metadata extraction.
    """
    try:
        path = Path(source)
        if not path.exists():
            path = Path(config_dir) / source
            if not path.exists():
                logger.error(f"File not found: {source}")
                return []
        
        logger.info(f"Processing file: {path}")
        
        with open(path, 'rb') as f:
            content = f.read()
        
        mime_type = mimetypes.guess_type(path)[0] or 'application/octet-stream'
        logger.info(f"Detected MIME type: {mime_type}")
        
        metadata = None
        if extract_metadata:
            if mime_type in ['text/markdown', 'text/plain', 'application/json']:
                content_text = content.decode('utf-8')
                metadata = get_metadata_for_content(content_text, path)
            else:
                metadata = {
                    "title": path.stem,
                    "status": "unsupported_format"
                }
                
            logger.info(f"Created metadata for file {path}")
        
        return [(str(path), io.BytesIO(content), mime_type, metadata)]
    except Exception as e:
        logger.error(f"Error reading file {path}: {str(e)}")
        return []

def collect_all_sources(context_list, config_dir):
    all_sources = []
    for context in context_list:
        all_sources.append(dict(
            **context,
            file_streams=collect_context_sources(context, config_dir)
        ))
    return all_sources
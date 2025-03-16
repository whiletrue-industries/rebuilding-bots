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
from typing import Tuple, BinaryIO, Dict, Optional, List, Union
from .dynamic_extraction import extract_structured_content, determine_document_type

logger = get_logger(__name__)

def process_file(
    file_path: Union[str, Path],
    context_name: str,
    extract_metadata: bool = False,
    section_idx: Optional[int] = None,
    title: Optional[str] = None
) -> Optional[Tuple[str, BinaryIO, str, Optional[Dict]]]:
    """
    Process a single file or section and create a source tuple.
    
    Args:
        file_path (Union[str, Path]): Path to the file to process
        context_name (str): Name of the context (used for file naming)
        extract_metadata (bool): Whether to extract metadata
        section_idx (int, optional): If this is a section of a larger file, its index
        title (str, optional): Override title for the content
        
    Returns:
        Optional[Tuple]: A source tuple if successful, None if processing failed
    """
    try:
        file_path = Path(file_path)
        
        # Read the file content
        try:
            content = file_path.read_text(encoding='utf-8')
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            return None
            
        # Skip empty content
        if not content.strip():
            return None
            
        # Generate file name based on whether this is a section or full file
        if section_idx is not None:
            file_name = f'{context_name}_{section_idx}.md'
        else:
            file_name = str(file_path)
            
        # Extract metadata if requested
        metadata = None
        if extract_metadata:
            metadata = get_metadata_for_content(
                content=content,
                file_path=file_path,
                title=title,
                section_idx=section_idx
            )
            
        # Create and return the source tuple
        return create_source_tuple(
            content=content,
            file_name=file_name,
            metadata=metadata
        )
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        return None

def process_directory(
    directory: Union[str, Path],
    context_name: str,
    pattern: str = "*.md",
    extract_metadata: bool = False
) -> List[Tuple[str, BinaryIO, str, Optional[Dict]]]:
    """
    Process all matching files in a directory.
    
    Args:
        directory (Union[str, Path]): Directory to process
        context_name (str): Name of the context
        pattern (str): Glob pattern to match files
        extract_metadata (bool): Whether to extract metadata
        
    Returns:
        List[Tuple]: List of source tuples
    """
    directory = Path(directory)
    if not directory.is_dir():
        logger.error(f"Directory not found: {directory}")
        return []
        
    logger.info(f"Processing directory: {directory}")
    sources = []
    
    # Find all matching files
    matching_files = glob.glob(os.path.join(directory, pattern))
    logger.info(f"Found {len(matching_files)} matching files")
    
    # Process each file
    for file_path in matching_files:
        source_tuple = process_file(
            file_path=file_path,
            context_name=context_name,
            extract_metadata=extract_metadata
        )
        if source_tuple:
            sources.append(source_tuple)
            logger.info(f"Added file: {file_path}")
            
    return sources

def create_source_tuple(
    content: str,
    file_name: str,
    mime_type: str = 'text/markdown',
    metadata: Optional[Dict] = None
) -> Tuple[str, BinaryIO, str, Optional[Dict]]:
    """
    Create a standardized source tuple from content and metadata.
    
    Args:
        content (str): The text content to include in the source
        file_name (str): Name to use for the file
        mime_type (str, optional): MIME type of the content. Defaults to 'text/markdown'
        metadata (dict, optional): Metadata to attach to the source
        
    Returns:
        tuple: A tuple of (file_name, binary_content, mime_type, metadata)
    """
    # Ensure content is stripped of extra whitespace
    content = content.strip()
    
    # Convert content to bytes and wrap in BytesIO
    content_bytes = content.encode('utf-8')
    binary_content = io.BytesIO(content_bytes)
    
    return (file_name, binary_content, mime_type, metadata)

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
            source_tuple = process_file(
                file_path=filename,
                context_name=context_name,
                extract_metadata=extract_metadata,
                section_idx=idx,
                title=f"Section {idx+1}"
            )
            if source_tuple:
                sources.append(source_tuple)
                logger.info(f"Added section {idx}")
        
        return sources
    except Exception as e:
        logger.error(f"Error processing split file {filename}: {str(e)}")
        return []

def collect_sources_google_spreadsheet(context_name: str, url: str, extract_metadata: bool = False):
    """
    Collect sources from a Google Spreadsheet.
    
    Args:
        context_name (str): Name of the context (used for file naming)
        url (str): URL of the Google Spreadsheet
        extract_metadata (bool): Whether to extract metadata from content
        
    Returns:
        list: List of tuples containing (file_name, file_content, mime_type, metadata)
    """
    try:        
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
                metadata = None
                if extract_metadata:
                    # Get first column value for title if available
                    first_header = headers[0] if headers else None
                    title = row.get(first_header, f"Row {idx}")
                    metadata = get_metadata_for_content(content, Path(file_name), title=title)
                
                sources.append(create_source_tuple(
                    content=content,
                    file_name=file_name,
                    metadata=metadata
                ))
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
    
    Args:
        context_config (dict): Configuration dictionary that must contain 'type', 'source', and 'slug'
        config_dir (str): Directory containing the configuration and source files
        extract_metadata (bool): Whether to extract metadata from sources
        
    Returns:
        list: List of tuples containing (file_name, file_content, mime_type, metadata)
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
    
    # Validate required config fields
    if not all(key in context_config for key in ['type', 'source', 'slug']):
        logger.error(f"Missing required fields in context config. Required: type, source, slug. Got: {context_config.keys()}")
        return sources
    
    source_type = context_config['type']
    source_path = context_config['source']
    context_name = context_config['slug']
    
    logger.info(f"Processing source of type: {source_type}, path: {source_path}")
    
    try:
        if source_type == 'files':
            # Check if extraction is a directory
            extraction_dir_name = context_config.get('extraction_dir', 'extraction')
            extraction_dir = os.path.join(config_dir, extraction_dir_name)
            
            # Process all files in the directory
            sources.extend(process_directory(
                directory=extraction_dir,
                context_name=context_name,
                pattern="*.md",
                extract_metadata=extract_metadata
            ))
        
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
    
    logger.info(f"Collected {len(sources)} total sources")
    return sources

def collect_all_sources(context_list, config_dir):
    all_sources = []
    for context in context_list:
        all_sources.append(dict(
            **context,
            file_streams=collect_context_sources(context, config_dir)
        ))
    return all_sources
import io
from pathlib import Path
from typing import Union
import hashlib
import dataflows as DF
from kvfile.kvfile_sqlite import CachedKVFileSQLite as KVFile
import json

from .config import get_logger
from .dynamic_extraction import extract_structured_content
from .document_parser.dynamic_extractions.generate_markdown_files import generate_markdown_dict, get_base_filename, sanitize_filename


logger = get_logger(__name__)
cache: KVFile = None

def get_metadata_for_content(content: str, file_path: str, document_type: str) -> dict:
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

    # Check if metadata is already cached
    cache_key = hashlib.sha256(content.strip().encode('utf-8')).hexdigest()[:16]
    item = cache.get(cache_key, default=None)
    if item:
        logger.info(f'Cache hit for {cache_key}, cached content: {item.get("content")[:100]!r}')
        if item.get('content') == content:
            return item['metadata']

    # Create basic metadata structure
    metadata = {
        "title": file_path,
        "status": "processed"
    }

    # Enhanced metadata using LLM extraction
    try:
        logger.info(f"Extracting structured content for {file_path} with document type: {document_type}")

        # Get the extracted data
        extracted_data = extract_structured_content(content, document_type=document_type)

        # Add document type
        metadata['document_type'] = document_type

        # Use document title from extraction if available
        if extracted_data.get('DocumentMetadata', {}).get('DocumentTitle'):
            metadata['title'] = extracted_data['DocumentMetadata']['DocumentTitle']

        # Add all extracted data directly to metadata (not nested)
        metadata.update(extracted_data)

        logger.info(f"Added enhanced metadata for {file_path}")
    except Exception as e:
        logger.error(f"Error extracting structured content from {file_path}")
        metadata['status'] = 'extraction_error'
        metadata['error'] = str(e)

    # Cache the metadata
    cache.set(cache_key, {
        'content': content,
        'metadata': metadata
    })

    return metadata

def process_file_stream(filename: str, content: Union[str, io.BufferedReader], content_type) -> dict:
    if not isinstance(content, str):
        content = content.read().decode('utf-8')
    content = content.strip()
    metadata = get_metadata_for_content(content, filename, content_type)
    return (filename, io.BytesIO(content.encode('utf-8')), content_type, metadata)
    
def collect_sources_files(config_dir: Path, context_name, source):
    files = list(config_dir.glob(source))
    file_streams = [
        process_file_stream(f.name, f.open('rb'), 'text/markdown')
        for f in files
    ]
    return file_streams

def collect_sources_split(config_dir, context_name, source, offset=0):
    filename = config_dir / source
    if filename.suffix == '.json':
        # In-memory markdown generation from JSON structure
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        document_name = data.get('metadata', {}).get('document_name', '')
        if not document_name:
            input_file = data.get('metadata', {}).get('input_file', '')
            document_name = get_base_filename(input_file)
        document_name = sanitize_filename(document_name)
        structure = data.get('structure', [])
        markdown_dict = generate_markdown_dict(structure, document_name)
        file_streams = [
            process_file_stream(fname, content, 'text/markdown')
            for fname, content in markdown_dict.items()
        ]
        return file_streams
    else:
        content = filename.read_text()
        content = content.split('\n---\n')
        file_streams = [
            process_file_stream(f'{context_name}_{i+offset}.md', c, 'text/markdown')
            for i, c in enumerate(content)
        ]
        return file_streams

def collect_sources_google_spreadsheet(context_name, source, offset=0):
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
            file_streams.append(
                process_file_stream(f'{context_name}_{idx+offset}.md', content, 'text/markdown')
            )
    return file_streams

def file_streams_for_context(config_dir, context_name, context_, offset=0):
    context_type = context_['type']
    source = context_['source']
    if context_type == 'files':
        file_streams = collect_sources_files(config_dir, context_name, source)
    elif context_type == 'split':
        file_streams = collect_sources_split(config_dir, context_name, source, offset=offset)
    elif context_type == 'google-spreadsheet':
        file_streams = collect_sources_google_spreadsheet(context_name, source, offset=offset)
    else:
        raise ValueError(f'Unknown context type: {context_type}')
    return file_streams


def collect_context_sources(context_, config_dir: Path):
    global cache
    cache = KVFile(location=str(Path(__file__).parent.parent / 'cache' / 'metadata'))
    context_name = context_['name']
    if 'sources' in context_:
        file_streams = []
        for source in context_['sources']:
            file_streams.extend(file_streams_for_context(config_dir, context_name, source, offset=len(file_streams)))
    else:
        file_streams = file_streams_for_context(config_dir, context_name, context_)
    cache.close()
    return file_streams

def collect_all_sources(context_list, config_dir):
    all_sources = []
    for context in context_list:
        all_sources.append(dict(
            **context,
            file_streams=collect_context_sources(context, config_dir)
        ))
    return all_sources
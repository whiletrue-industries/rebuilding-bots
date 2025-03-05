import os
from pathlib import Path
import yaml
import json
from datetime import datetime
from datetime import timezone
import markdown
import re
import sys
from botnim.config import SPECS
from dynamic_extraction import extract_structured_content

def determine_document_type(file_path: Path) -> str:
    """
    Determine the document type by extracting it from the file name.
    The convention is that the document type appears before the underscore.
    
    Args:
        file_path (Path): Path to the source file
        
    Returns:
        str: Document type, defaults to "תקנון הכנסת" if pattern not found
    """
    try:
        filename = file_path.stem  # Get filename without extension
        if '_' in filename:
            doc_type = filename.split('_')[0]
            return doc_type.strip()
    except Exception as e:
        print(f"Warning: Could not determine document type from filename {file_path}: {e}")
    
    return ""  # Default type if pattern not found

def extract_content_metadata(file_path: Path, context_config: dict) -> dict:
    """
    Extract content and metadata from different file types.
    
    Args:
        file_path (Path): Path to the source file
        context_config (dict): The context configuration for this source
    
    Returns:
        dict: Extracted metadata including title, content, sections, etc.
    """
    file_type = file_path.suffix.lower()
    
    try:
        # Read the file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Use dynamic extraction for supported file types
        if file_type in ['.txt', '.md']:
            # Detect document type from filename
            document_type = determine_document_type(file_path)
            
            # Extract content with detected document type
            extracted_data = extract_structured_content(content, document_type=document_type)
            
            # Combine with basic metadata
            metadata = {
                'extracted_at': datetime.now(timezone.utc).isoformat(),
                'status': 'processed',
                'context_type': context_config['type'],
                'context_name': context_config['name'],
                'source_content': content,
                'document_type': document_type,  # Include detected document type in metadata
                'extracted_data': extracted_data
            }
            
            # Use document title from extraction if available
            if extracted_data.get('DocumentMetadata', {}).get('DocumentTitle'):
                metadata['title'] = extracted_data['DocumentMetadata']['DocumentTitle']
            else:
                metadata['title'] = file_path.stem
                
            return metadata
            
        else:
            # Fall back to basic extraction for unsupported types
            metadata = {
                'title': file_path.stem,
                'content': content,
                'extracted_at': datetime.now(timezone.utc).isoformat(),
                'status': 'unsupported_format',
                'context_type': context_config['type'],
                'context_name': context_config['name'],
                'error': f'Unsupported file type: {file_type}'
            }
            return metadata
            
    except Exception as e:
        return {
            'title': file_path.stem,
            'status': 'error',
            'error': str(e),
            'extracted_at': datetime.now(timezone.utc).isoformat(),
            'context_type': context_config['type'],
            'context_name': context_config['name']
        }

def process_context_source(config_dir: Path, context_config: dict):
    """
    Process a single context source based on its type.
    
    Args:
        config_dir (Path): Bot's config directory
        context_config (dict): Context configuration
    """
    context_type = context_config['type']
    source = context_config['source']
    
    if context_type == 'files':
        # Handle file-based sources
        if isinstance(source, str):
            # Handle glob pattern
            source_path = config_dir / source
            source_dir = source_path.parent
            
            for source_file in source_dir.glob(source_path.name):
                if source_file.is_file() and not source_file.name.startswith('.'):
                    # Extract content metadata
                    metadata = extract_content_metadata(source_file, context_config)
                    metadata['source_file'] = str(source_file.relative_to(source_dir))
                    
                    # Save metadata using the consolidated function
                    save_metadata(metadata, source_file, source_dir)
                    
    elif context_type == 'google-spreadsheet':
        # Create placeholder metadata for spreadsheet source
        print(f"Did not process spreadsheet: {context_config['name']}")
    
    else:
        print(f"Unsupported context type: {context_type}")

def process_bot(bot_id: str, specs_dir: Path):
    """
    Process a specific bot's source files and create metadata.
    
    Args:
        bot_id (str): ID of the bot to process
        specs_dir (Path): Path to the specs directory containing bot configurations
    """
    config_dir = specs_dir / bot_id
    
    if not config_dir.exists():
        raise ValueError(f"Bot directory not found: {bot_id}")
    
    config_file = config_dir / 'config.yaml'
    if not config_file.exists():
        raise ValueError(f"Config file not found for bot: {bot_id}")
    
    # Load bot config
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    if not config.get('context'):
        print(f"No context defined in config for bot: {bot_id}")
        return
        
    print(f"Processing bot: {bot_id}")
    
    # Process each context source
    for context_config in config['context']:
        process_context_source(config_dir, context_config)

def main(specs_dir: Path, bot_id: str = 'all'):
    """
    Main function to process bots and create metadata files.
    
    Args:
        specs_dir (Path): Path to the specs directory
        bot_id (str): Specific bot ID to process, or 'all' for all bots
    """
    if bot_id == 'all':
        # Process all bots
        for bot_dir in specs_dir.iterdir():
            if bot_dir.is_dir() and (bot_dir / 'config.yaml').exists():
                process_bot(bot_dir.name, specs_dir)
    else:
        # Process specific bot
        process_bot(bot_id, specs_dir)

def save_metadata(metadata: dict, source_file: Path, base_dir: Path) -> None:
    """
    Save metadata to a JSON file in a metadata subfolder within the source file's directory.
    
    Args:
        metadata (dict): Metadata to save
        source_file (Path): Original source file path
        base_dir (Path): Base directory for relative path calculation
    """
    # Calculate relative path from base_dir to source_file
    relative_source_path = source_file.relative_to(base_dir)
    
    # Create metadata directory in the same directory as the source file
    metadata_dir = source_file.parent / 'metadata'
    metadata_dir.mkdir(parents=True, exist_ok=True)
    
    # Create metadata file with same name as source + .metadata.json
    metadata_file = metadata_dir / f"{source_file.name}.metadata.json"
    
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    print(f"Created metadata file: {metadata_file} (Status: {metadata.get('status', 'unknown')})")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        bot_id = sys.argv[1]
        main(SPECS, bot_id)
    else:
        print("Please provide a bot id")
        sys.exit(1)

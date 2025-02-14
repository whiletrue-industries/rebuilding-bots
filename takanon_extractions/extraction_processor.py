import os
from pathlib import Path
import yaml
import json
from datetime import datetime
import markdown
import re
import sys
from botnim.config import SPECS

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
    metadata = {
        'title': file_path.stem,
        'sections': [],
        'content': '',
        'extracted_at': datetime.utcnow().isoformat(),
        'status': 'processed',
        'context_type': context_config['type'],
        'context_name': context_config['name']
    }
    
    try:
        if file_type == '.md':
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Extract headers
            headers = re.findall(r'^(#{1,6})\s+(.+)$', content, re.MULTILINE)
            metadata['sections'] = [
                {
                    'level': len(h[0]),
                    'title': h[1].strip(),
                }
                for h in headers
            ]
            
            # Convert markdown to plain text for content
            metadata['content'] = re.sub(r'[#*`_]', '', content)
            
        elif file_type in ['.txt', '.json', '.yaml', '.yml']:
            with open(file_path, 'r', encoding='utf-8') as f:
                metadata['content'] = f.read()
                
        else:
            metadata['status'] = 'unsupported_format'
            metadata['error'] = f'Unsupported file type: {file_type}'
            return metadata
            
        # Extract potential title from first line if not already set
        if metadata['title'] == file_path.stem and metadata['content']:
            first_line = metadata['content'].split('\n')[0].strip()
            if len(first_line) < 100:  # Reasonable title length
                metadata['title'] = first_line
                
        return metadata
        
    except Exception as e:
        metadata['status'] = 'error'
        metadata['error'] = str(e)
        return metadata

def process_context_source(config_dir: Path, context_config: dict, metadata_dir: Path):
    """
    Process a single context source based on its type.
    
    Args:
        config_dir (Path): Bot's config directory
        context_config (dict): Context configuration
        metadata_dir (Path): Directory to store metadata files
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
                    
                    # Create metadata file path
                    metadata_filename = f"{source_file.relative_to(source_dir)}.metadata.json"
                    metadata_file_path = metadata_dir / metadata_filename
                    
                    # Ensure parent directories exist
                    metadata_file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Save metadata
                    with open(metadata_file_path, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, ensure_ascii=False, indent=2)
                    
                    print(f"Created metadata file: {metadata_file_path} (Status: {metadata['status']})")
                    
    elif context_type == 'google-spreadsheet':
        # Create placeholder metadata for spreadsheet source
        metadata = {
            'title': context_config['name'],
            'source': source,
            'type': 'google-spreadsheet',
            'extracted_at': datetime.utcnow().isoformat(),
            'status': 'pending',
            'max_num_results': context_config.get('max_num_results'),
            'context_type': context_type,
            'context_name': context_config['name']
        }
        
        # Save spreadsheet metadata
        metadata_file_path = metadata_dir / f"spreadsheet_{Path(source).stem}.metadata.json"
        with open(metadata_file_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
            
        print(f"Created spreadsheet metadata file: {metadata_file_path}")
    
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
    
    # Create metadata directory
    metadata_dir = config_dir / 'metadata'
    metadata_dir.mkdir(exist_ok=True)
    
    # Process each context source
    for context_config in config['context']:
        process_context_source(config_dir, context_config, metadata_dir)

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

if __name__ == '__main__':
    if len(sys.argv) > 1:
        bot_id = sys.argv[1]
        main(SPECS, bot_id)
    else:
        print("Please provide a bot id")
        sys.exit(1)

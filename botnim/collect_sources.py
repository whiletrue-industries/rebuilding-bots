import io
from pathlib import Path
import dataflows as DF
from datetime import datetime, timezone
import json
import hashlib
from content_extraction import extract_structured_content

def collect_sources_files(config_dir, context_name, source):
    files = list(config_dir.glob(source))
    file_streams = [(f.name, f.open('rb').read(), 'text/markdown') for f in files]
    return file_streams

def collect_sources_split(config_dir, context_name, source):
    file = config_dir / source
    content = file.open('r').read()
    file_streams = content.split('---')
    file_streams = [io.BytesIO(f.strip().encode('utf-8')) for f in file_streams if f.strip()]
    file_streams = [(f'{context_name}_{i}.md', f, 'text/markdown') for i, f in enumerate(file_streams)]
    return file_streams

def collect_sources_google_spreadsheet(bot_id, context_name, source, force_extract=False):
    resources, dp, _ = DF.Flow(
        DF.load(source, name='rows'),
    ).results()
    rows = resources[0]
    headers = [f.name for f in dp.resources[0].schema.fields]
    file_streams = []
    
    for idx, row in enumerate(rows):
        content = ''
        if len(headers) > 1:
            for header in headers:
                if row[header]:
                    if header != headers[0]:
                        content += f'{header}:\n{row[header]}\n\n'
                    else:
                        content += f'{row[header]}\n\n'
        
        if content:
            content_str = content.strip()
            
            # Create a content hash to detect changes
            content_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()
            
            # Check if metadata already exists and if content has changed
            metadata_path = Path(f'specs/{bot_id}/extraction/metadata') / f"{context_name}_{idx}.md.metadata.json"
            should_extract = True
            
            if not force_extract and metadata_path.exists():
                try:
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        existing_metadata = json.load(f)
                        existing_hash = hashlib.md5(existing_metadata.get('source_content', '').encode('utf-8')).hexdigest()
                        
                        # Only extract if content has changed
                        if content_hash == existing_hash:
                            print(f"Skipping extraction for row {idx} - content unchanged")
                            should_extract = False
                except Exception as e:
                    print(f"Error reading existing metadata, will re-extract: {e}")
            
            # Only extract metadata if needed or forced
            if should_extract or force_extract:
                if force_extract:
                    print(f"Force extracting metadata for row {idx}")
                else:
                    print(f"\nProcessing row {idx} - content changed or new:")
                
                print("Content being sent to API:")
                print("-" * 50)
                print(content_str)
                print("-" * 50)
                
                # Extract metadata using dynamic extraction
                try:
                    extracted_data = extract_structured_content(
                        content_str,
                        document_type="spreadsheet_entry"
                    )
                    
                    # Add detailed logging of the extraction results
                    print(f"\n=== Extraction Results for Row {idx} ===")
                    print(f"Content: {content_str}")
                    print("Extracted Metadata:")
                    print(json.dumps(extracted_data, ensure_ascii=False, indent=2))
                    print("=" * 50)
                    
                    # Create metadata structure
                    metadata = {
                        'extracted_at': datetime.now(timezone.utc).isoformat(),
                        'status': 'processed',
                        'context_type': 'google-spreadsheet',
                        'context_name': context_name,
                        'source_content': content_str,
                        'document_type': 'spreadsheet_entry',
                        'content_hash': content_hash,
                        'extracted_data': extracted_data
                    }
                    
                    # Save metadata file
                    metadata_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    with open(metadata_path, 'w', encoding='utf-8') as f:
                        json.dump(metadata, f, ensure_ascii=False, indent=2)
                    
                except Exception as e:
                    print(f"Warning: Failed to extract metadata for row {idx}: {e}")
            
            # Return file stream as before
            file_streams.append((
                f'{context_name}_{idx}.md',
                io.BytesIO(content_str.encode('utf-8')),
                'text/markdown'
            ))
            
    return file_streams

def collect_context_sources(context_, config_dir: Path, force_extract=False):
    context_name = context_['name']
    context_type = context_['type']
    bot_id = config_dir.name  # Get the bot ID from the config directory
    
    if context_type == 'files':
        return collect_sources_files(config_dir, context_name, context_['source'])
    elif context_type == 'split':
        return collect_sources_split(config_dir, context_name, context_['source'])
    elif context_type == 'google-spreadsheet':
        return collect_sources_google_spreadsheet(bot_id, context_name, context_['source'], force_extract)
    else:
        raise ValueError(f'Unknown context type: {context_type}')

def collect_all_sources(context_list, config_dir, force_extract=False):
    all_sources = []
    for context_ in context_list:
        sources = collect_context_sources(context_, config_dir, force_extract)
        all_sources.extend(sources)
    return all_sources
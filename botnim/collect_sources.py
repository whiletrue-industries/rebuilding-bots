import io
from pathlib import Path
import dataflows as DF
from datetime import datetime, timezone
import json
from content_extraction import extract_structured_content

def collect_sources_files(config_dir, context_name, source):
    files = list(config_dir.glob(source))
    file_streams = []
    for f in files:
        with open(f, 'rb') as ff:
            content = ff.read()
        file_streams.append((f.name, io.BytesIO(content), f'text/{f.suffix[1:]}', None))
    return file_streams

def collect_sources_split(config_dir, context_name, source):
    with open(config_dir / source, 'r') as f:
        content = f.read()
    file_streams = content.split('---')
    file_streams = [(f'{context_name}_{i}.md', io.BytesIO(f.encode('utf-8')), 'text/markdown', None) for i, f in enumerate(file_streams)]
    return file_streams

def collect_sources_google_spreadsheet(bot_id, context_name, source, extract_metadata=False):
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
            # Initialize metadata as None by default
            metadata = None
            
            # Only perform extraction if explicitly requested
            if extract_metadata:
                print(f"\nProcessing row {idx}:")
                print("Content being sent to API:")
                print("-" * 50)
                print(content)
                print("-" * 50)
                
                # Extract metadata using dynamic extraction
                try:
                    extracted_data = extract_structured_content(
                        content,
                        document_type="spreadsheet_entry"
                    )
                    
                    # Add detailed logging of the extraction results
                    print(f"\n=== Extraction Results for Row {idx} ===")
                    print(f"Content: {content.strip()}")
                    print("Extracted Metadata:")
                    print(json.dumps(extracted_data, ensure_ascii=False, indent=2))
                    print("=" * 50)
                    
                    # Create metadata structure
                    metadata = {
                        'extracted_at': datetime.now(timezone.utc).isoformat(),
                        'status': 'processed',
                        'context_type': 'google-spreadsheet',
                        'context_name': context_name,
                        'bot_id': bot_id,  # Include bot_id for reference
                        'source_content': content,
                        'document_type': 'spreadsheet_entry',
                        'extracted_data': extracted_data
                    }
                    
                except Exception as e:
                    print(f"Warning: Failed to extract metadata for row {idx}: {e}")
                    metadata = {
                        'extracted_at': datetime.now(timezone.utc).isoformat(),
                        'status': 'error',
                        'context_type': 'google-spreadsheet',
                        'context_name': context_name,
                        'bot_id': bot_id,
                        'source_content': content,
                        'document_type': 'spreadsheet_entry',
                        'error': str(e)
                    }
            
            file_streams.append((
                f'{context_name}_{idx}.md',
                io.BytesIO(content.strip().encode('utf-8')),
                'text/markdown',
                metadata 
            ))
            
    return file_streams

def collect_context_sources(context_, config_dir: Path, extract_metadata=False):
    context_name = context_['name']
    context_type = context_['type']
    bot_id = config_dir.name  # Get the bot ID from the config directory
    
    if context_type == 'files':
        return collect_sources_files(config_dir, context_name, context_['source'])
    elif context_type == 'split':
        return collect_sources_split(config_dir, context_name, context_['source'])
    elif context_type == 'google-spreadsheet':
        return collect_sources_google_spreadsheet(bot_id, context_name, context_['source'], extract_metadata)
    else:
        raise ValueError(f'Unknown context type: {context_type}')

def collect_all_sources(context_list, config_dir):
    all_sources = []
    for context in context_list:
        all_sources.append(dict(
            **context,
            file_streams=collect_context_sources(context, config_dir)
        ))
    return all_sources
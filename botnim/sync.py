import os
import json
import io
import codecs
from pathlib import Path

import yaml
import chardet

from openai import OpenAI

from .config import SPECS


api_key = os.environ['OPENAI_API_KEY']
# Create openai client and get completion for prompt with the 'gpt4-o' model:
client = OpenAI(api_key=api_key)

def openapi_to_tools(openapi_spec):
    ret = []
    for path in openapi_spec['paths'].values():
        for method in path.values():
            operation_id = method['operationId']
            operation_desc = method['description']
            parameters = method.get('parameters', [])
            properties = dict(
                (
                    param['name'],
                    dict(
                        type=param['schema']['type'],
                        description=param['description'],
                    )
                )
                for param in parameters
            )
            required = [
                param['name']
                for param in parameters
                if param.get('required')
            ]
            func = dict(
                type='function',
                function=dict(
                    name=operation_id,
                    description=operation_desc,
                    parameters=dict(
                        type='object',
                        properties=properties,
                        required=required,
                    ),
                ),
            )
            ret.append(func)
    return ret

def update_assistant(config, config_dir, production, replace_context=False):
    tool_resources = None
    tools = None
    print(f'Updating assistant: {config["name"]}')
    # Load context, if necessary
    if config.get('context'):
        print("\nDEBUG: Starting context processing")
        print(f"Number of context items: {len(config['context'])}")
        for i, context_ in enumerate(config['context']):
            print(f"\nProcessing context #{i}: {context_['name']}")
            print(f"Context contents: {context_}")
            
            # Process Google Sheets source if present
            if 'source' in context_ and 'split' in context_:
                filename = config_dir / context_['split']
                print(f"Processing Google Sheet source to {filename}")
                
                # Extract sheet ID from URL
                sheet_url = context_['source']
                if '/spreadsheets/d/' not in sheet_url:
                    raise ValueError(f"Invalid Google Sheets URL format: {sheet_url}")
                    
                sheet_id = sheet_url.split('/spreadsheets/d/')[1].split('/')[0]
                print(f"Sheet ID: {sheet_id}")
                
                # Download the sheet as CSV
                import requests
                csv_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0'
                print(f"Downloading from: {csv_url}")
                
                response = requests.get(csv_url, timeout=30)
                response.raise_for_status()
                
                # Process CSV content
                import csv
                from io import StringIO
                
                csv_content = StringIO(response.text)
                reader = csv.reader(csv_content)
                rows = list(reader)
                
                if not rows:
                    raise ValueError("No content found in Google Sheet")
                
                print(f"Found {len(rows)} rows in sheet")
                
                # Convert to markdown with separators
                content = []
                for row in rows:
                    # Join all non-empty cells in the row
                    row_content = ' '.join(cell.strip() for cell in row if cell.strip())
                    if row_content:
                        content.append(row_content)
                
                if not content:
                    raise ValueError("No valid content found in Google Sheet")
                
                # Write to markdown file
                markdown_content = '\n\n---\n\n'.join(content)
                filename.parent.mkdir(parents=True, exist_ok=True)
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(markdown_content)
                print(f"Wrote {len(content)} sections to {filename}")
                
                # Prepare content for vector store
                content = markdown_content.split('\n---\n')
            
            name = context_['name']
            if not production:
                name += ' - פיתוח'
            vector_store = client.beta.vector_stores.list()
            vector_store_id = None
            for vs in vector_store:
                if vs.name == name:
                    if replace_context:
                        client.beta.vector_stores.delete(vs.id)
                    else:
                        vector_store_id = vs.id
                    break
            if vector_store_id is None:
                if 'files' in context_:
                    files = list(config_dir.glob(context_['files']))
                    existing_files = client.files.list()
                    # delete existing files:
                    for f in files:
                        for ef in existing_files:
                            if ef.filename == f.name:
                                client.files.delete(ef.id)
                    file_streams = [f.open('rb') for f in files]
                elif 'split' in context_:
                    filename = config_dir / context_['split']
                    print(f"\nProcessing context: {context_['name']}")
                    print(f"Target file: {filename}")
                    
                    if 'source' in context_:
                        print("Found source URL, attempting to process Google Sheet")
                        # Download public google spreadsheet file to md file in filename
                        import requests
                        import csv
                        from io import StringIO
                        
                        try:
                            # Convert Google Sheets URL to CSV export URL
                            sheet_url = context_['source']
                            print(f'Processing Google Sheet URL: {sheet_url}')
                            
                            if '/spreadsheets/d/' not in sheet_url:
                                raise ValueError(f"Invalid Google Sheets URL format: {sheet_url}")
                                
                            sheet_id = sheet_url.split('/spreadsheets/d/')[1].split('/')[0]
                            print(f'Extracted spreadsheet ID: {sheet_id}')
                            csv_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0'
                            print(f'Accessing CSV URL: {csv_url}')
                            
                            try:
                                response = requests.get(csv_url, timeout=30)
                                response.raise_for_status()
                            except requests.exceptions.RequestException as e:
                                print(f"Failed to fetch Google Sheet: {str(e)}")
                                if hasattr(e.response, 'status_code'):
                                    print(f"Status code: {e.response.status_code}")
                                if hasattr(e.response, 'text'):
                                    print(f"Response text: {e.response.text[:500]}")
                                raise
                            print(f'Response status: {response.status_code}')
                            print(f'Response length: {len(response.content)} bytes')
                            print(f'Response headers: {response.headers}')
                            print(f'Content-Type: {response.headers.get("content-type", "not specified")}')
                            
                            # Handle CSV content with explicit encoding checks
                            try:
                                # First try UTF-8 with BOM
                                content = response.content.decode('utf-8-sig', errors='strict')
                            except UnicodeDecodeError:
                                try:
                                    # Then try UTF-8 without BOM
                                    content = response.content.decode('utf-8', errors='strict')
                                except UnicodeDecodeError:
                                    try:
                                        # Then try windows-1255
                                        content = response.content.decode('windows-1255', errors='strict')
                                    except UnicodeDecodeError:
                                        # Last resort - detect encoding
                                        import chardet
                                        detected = chardet.detect(response.content)
                                        print(f"Detected encoding: {detected}")
                                        if detected['confidence'] > 0.8:
                                            content = response.content.decode(detected['encoding'])
                                        else:
                                            raise ValueError(f"Could not confidently detect encoding: {detected}")

                            # Normalize Unicode form
                            import unicodedata
                            content = unicodedata.normalize('NFKC', content)
                            
                            # Verify content has Hebrew characters
                            if not any('\u0590' <= c <= '\u05FF' for c in content):
                                print("Warning: Raw content:", response.content[:100])
                                print("Warning: Decoded content:", content[:100])
                                raise ValueError("No Hebrew characters found in content")
                            
                            print(f'Successfully decoded content with Hebrew characters')
                            print(f'Sample content: {content[:200]}')
                            
                            if content is None:
                                raise ValueError("Could not decode content with any encoding")
                                
                            print(f'Decoded content length: {len(content)} bytes')
                            if len(content) < 10:  # Arbitrary small number
                                raise ValueError(f"Content suspiciously short: {content}")
                                
                            print(f'Content preview:\n{content[:200]}')
                            
                        except requests.RequestException as e:
                            print(f"Failed to fetch spreadsheet: {e}")
                            raise
                        
                        try:
                            # Parse CSV content
                            csv_content = StringIO(content)
                            reader = csv.reader(csv_content)
                            rows = list(reader)
                            
                            print(f'Raw response content type: {type(response.content)}')
                            print(f'Raw response preview: {response.content[:100]}')
                            
                            if not rows:
                                print("WARNING: No rows found in CSV content")
                                print(f"Full raw content:\n{content}")
                                raise ValueError("No rows found in CSV content")
                            
                            print(f'Total rows found: {len(rows)}')
                            print('First few rows:')
                            for i, row in enumerate(rows[:3]):
                                print(f'Row {i}: {row}')
                                
                            # Validate that we have actual content
                            has_content = False
                            for row in rows:
                                if any(cell.strip() for cell in row):
                                    has_content = True
                                    break
                            
                            if not has_content:
                                raise ValueError("CSV contains no non-empty cells")
                            
                            # Validate CSV structure
                            if any(not isinstance(row, list) for row in rows):
                                raise ValueError("Invalid CSV structure detected")
                        except csv.Error as e:
                            print(f"CSV parsing error: {e}")
                            print(f"Full content causing error:\n{content}")
                            raise
                        except Exception as e:
                            print(f"Error processing CSV content: {e}")
                            print(f"Raw content preview: {response.text[:500]}")
                            raise
                        
                        # Process all columns that have content
                        data_rows = []
                        print("\nProcessing CSV rows...")
                        
                        # Always process all rows - don't skip header
                        for i, row in enumerate(rows):
                            try:
                                row_content = []
                                print(f"\nProcessing row {i}: {row}")
                                
                                # Process each cell in the row
                                for j, cell in enumerate(row):
                                    if not isinstance(cell, str):
                                        print(f"Converting non-string cell at row {i}, col {j}: {type(cell)}")
                                        cell = str(cell) if cell is not None else ""
                                    
                                    cleaned = cell.strip()
                                    if cleaned:
                                        print(f"Valid content in col {j}: {cleaned[:50]}...")
                                        row_content.append(cleaned)
                                
                                # Combine all non-empty cells
                                if row_content:
                                    combined_content = ' '.join(row_content)
                                    print(f"Adding row {i}: {combined_content[:100]}...")
                                    data_rows.append(combined_content)
                                    
                            except Exception as e:
                                print(f"Error processing row {i}: {e}")
                                continue
                        
                        print(f'Number of data rows processed: {len(data_rows)}')
                        if data_rows:
                            print('Sample of processed rows:')
                            for row in data_rows[:3]:
                                print(f'- {row[:100]}...')
                        else:
                            print("WARNING: No data rows were processed!")
                        
                        # Convert to markdown with --- separators
                        markdown_content = '\n---\n'.join(data_rows) if data_rows else ''
                        print(f"Final markdown content length: {len(markdown_content)}")
                        
                        if not markdown_content.strip():
                            raise ValueError("No valid content generated for markdown file")
                            
                        # Ensure directory exists
                        filename.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Process and write content with robust encoding handling
                        import unicodedata
                        
                        # Ensure we have string content
                        if isinstance(markdown_content, bytes):
                            try:
                                markdown_content = markdown_content.decode('utf-8-sig')
                            except UnicodeDecodeError:
                                markdown_content = markdown_content.decode('utf-8', errors='replace')
                        
                        # Normalize to NFKC form for maximum compatibility
                        markdown_content = unicodedata.normalize('NFKC', markdown_content)
                        
                        # Write content in UTF-8 with BOM
                        filename.parent.mkdir(parents=True, exist_ok=True)
                        with open(filename, 'wb') as f:
                            # Write UTF-8 BOM
                            f.write(codecs.BOM_UTF8)
                            # Write normalized content
                            f.write(markdown_content.encode('utf-8', errors='replace'))
                        
                        # Verify written content
                        with open(filename, 'r', encoding='utf-8-sig') as f:
                            verification = f.read()
                            if not verification.strip():
                                raise ValueError("Written file is empty")
                            if not any('\u0590' <= c <= '\u05FF' for c in verification):
                                print("Warning: Written content:", verification[:200])
                                raise ValueError("No Hebrew characters found in verified content")
                        
                        print(f'Successfully wrote and verified Hebrew content to {filename}')
                        print(f'Sample of written content: {verification[:200]}')
                        print(f'Successfully wrote content to {filename}')
                            
                        # Verify file was written and has content
                        if not os.path.exists(filename):
                            raise ValueError(f"Failed to create file: {filename}")
                                
                        file_size = os.path.getsize(filename)
                        print(f"Verified file exists with size: {file_size} bytes")
                            
                        if file_size == 0:
                            raise ValueError(f"Created file is empty: {filename}")
                            
                        print("Reading file content for verification...")
                        with open(filename, 'r', encoding='utf-8') as f:
                            file_content = f.read()
                        print(f"File content length: {len(file_content)} characters")
                            
                        # Split content for vector store processing
                        content = file_content.split('\n---\n') if file_content.strip() else []
                        print(f"Split into {len(content)} sections")
                    print(f"Processing {len(content)} content sections...")
                    file_streams = []
                    for i, c in enumerate(content):
                        if not c.strip():
                            print(f"Skipping empty section {i}")
                            continue
                        encoded = c.strip().encode('utf-8')
                        print(f"Section {i}: {len(encoded)} bytes")
                        file_streams.append((f'{name}_{i}.md', io.BytesIO(encoded), 'text/markdown'))
                    print(f"Created {len(file_streams)} file streams")
                print(f"Creating new vector store with name: {name}")
                vector_store = client.beta.vector_stores.create(name=name)
                print(f"Created vector store with ID: {vector_store.id}")
                while len(file_streams) > 0:
                    file_batch = client.beta.vector_stores.file_batches.upload_and_poll(
                        vector_store_id=vector_store.id, files=file_streams[:32]
                    )
                    print(f'VECTOR STORE {name} batch: uploaded {file_batch.file_counts.completed}, ' +\
                          f'failed {file_batch.file_counts.failed}, ' + \
                          f'pending {file_batch.file_counts.in_progress}, ' + \
                          f'remaining {len(file_streams)}')
                    file_streams = file_streams[32:]
                vector_store_id = vector_store.id
            tool_resources = dict(
                file_search=dict(
                    vector_store_ids=[vector_store_id],
                ),
            )
        tools = [dict(
            type='file_search',
            file_search=dict(
                max_num_results=context_.get('max_num_results', 20),
            ),
        )]

    # List all the assistants in the organization:
    assistants = client.beta.assistants.list()
    assistant_id = None
    assistant_name = config['name']
    if not production:
        assistant_name += ' - פיתוח'
    for assistant in assistants:
        if assistant.name == assistant_name:
            assistant_id = assistant.id
            break
    print(f'Assistant ID: {assistant_id}')
    asst_params = dict(
        name=assistant_name,
        description=config['description'],
        model='gpt-4o',
        instructions=config['instructions'],
        temperature=0.00001,
    )
    if config.get('tools'):
        tools = tools or []
        for tool in config['tools']:
            if tool == 'code-interpreter':
                tools.append(dict(type='code_interpreter'))
            else:
                openapi_spec = (SPECS / 'openapi' / tool).with_suffix('.yaml').open()
                openapi_spec = yaml.safe_load(openapi_spec)
                openapi_tools = openapi_to_tools(openapi_spec)
                # print(f'OpenAPI Tool: {tool}')
                tools.extend(openapi_tools)
    if tools:
        asst_params['tools'] = tools
    if tool_resources:
        asst_params['tool_resources'] = tool_resources
    import pprint
    pprint.pprint(asst_params)
    if assistant_id is None:
        # Create a new assistant:
        assistant = client.beta.assistants.create(**asst_params)
        assistant_id = assistant.id
        print(f'Assistant created: {assistant_id}')
        # ...
    else:
        # Update the existing assistant:
        assistant = client.beta.assistants.update(assistant_id, **asst_params)
        print(f'Assistant updated: {assistant_id}')
        # ...


def sync_agents(environment, bots, replace_context=False):
    production = environment == 'production'
    for config_fn in SPECS.glob('*/config.yaml'):
        config_dir = config_fn.parent
        bot_id = config_dir.name
        if bots in ['all', bot_id]:
            with config_fn.open() as config_f:
                config = yaml.safe_load(config_f)
                config['instructions'] = (config_dir / config['instructions']).read_text()
                update_assistant(config, config_dir, production, replace_context=replace_context)

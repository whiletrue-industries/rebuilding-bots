import os
import json
import io
from pathlib import Path

import logging
import yaml
from openai import OpenAI

from .config import SPECS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



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

def _upload_file_batches(client, vector_store_id, file_streams):
    """Helper function to upload file batches to vector store"""
    while len(file_streams) > 0:
        try:
            file_batch = client.beta.vector_stores.file_batches.upload_and_poll(
                vector_store_id=vector_store_id, 
                files=file_streams[:32]
            )
            logger.info(f'Batch uploaded: completed {file_batch.file_counts.completed}, ' +\
                       f'failed {file_batch.file_counts.failed}, ' +\
                       f'pending {file_batch.file_counts.in_progress}, ' +\
                       f'remaining {len(file_streams)}')
            file_streams = file_streams[32:]
        except Exception as e:
            logger.error(f'Error uploading file batch: {str(e)}')
            raise

def update_assistant(config, config_dir, production, replace_context=False):
    tool_resources = None
    tools = None
    vector_store_id = None
    print(f'Updating assistant: {config["name"]}')
    # Load context, if necessary
    if config.get('context'):
        # Find the main context and common knowledge context
        main_context = None
        common_knowledge = None
        for context_ in config['context']:
            if 'common-knowledge.md' in str(context_.get('split', '')):
                common_knowledge = context_
            else:
                main_context = context_

        # Determine vector store name based on main context and environment
        base_name = (main_context or common_knowledge)['name']
        staging_name = base_name + ' - פיתוח'
        prod_name = base_name
        target_name = prod_name if production else staging_name

        # Handle vector store creation/update
        vector_store = client.beta.vector_stores.list()
        vector_store_id = None
        for vs in vector_store:
            if vs.name == target_name:
                if replace_context:
                    client.beta.vector_stores.delete(vs.id)
                else:
                    vector_store_id = vs.id
                break
        if vector_store_id is None:
            vector_store = client.beta.vector_stores.create(name=target_name)
            vector_store_id = vector_store.id
            
            # Process main context files first if they exist
            if main_context:
                file_streams = []
                if 'files' in main_context:
                    files = list(config_dir.glob(main_context['files']))
                    existing_files = client.files.list()
                    for f in files:
                        for ef in existing_files:
                            if ef.filename == f.name:
                                client.files.delete(ef.id)
                    file_streams = [f.open('rb') for f in files]
                
                # Upload main context files
                if file_streams:
                    _upload_file_batches(client, vector_store_id, file_streams)

                # Now process common knowledge
                if common_knowledge:
                    filename = config_dir / common_knowledge['split']
                    if 'source' in common_knowledge:
                        # Download data from the public Google Spreadsheet
                        import requests
                        sheet_id = common_knowledge['source'].split('/d/')[1].split('/')[0]
                        logger.info(f'Sheet ID: {sheet_id}')
                        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv'
                        response = requests.get(url)
                        logger.info(f'Response status code: {response.status_code}')
                        logger.info(f'Response text: {response.text[:100]}...')  # Log first 100 characters
                        data = response.text

                        # Convert CSV data to markdown format
                        markdown_content = []
                        rows = data.strip().split('\n')
                        logger.info(f'Number of rows: {len(rows)}')

                        for row in rows:
                            markdown_content.append(f'{row.strip()}')
                            markdown_content.append('\n---\n')

                        markdown_content = '\n'.join(markdown_content)
                        logger.info(f'Markdown content: {markdown_content[:100]}...')  # Log first 100 characters
                        logger.info(f'Writing to file: {filename}')
                        filename.write_text(markdown_content, encoding='utf-8')
                        logger.info(f'File written successfully')
                    content = filename.read_text()
                    content = content.split('\n---\n')
                    file_streams = []
                    for i, c in enumerate(content):
                        if c.strip():
                            file_stream = io.BytesIO(c.strip().encode('utf-8'))
                            file_streams.append((f'common_knowledge_{i}.md', file_stream, 'text/markdown'))
                        else:
                            logger.warning(f'Skipping empty file: common_knowledge_{i}.md')
                    
                    if file_streams:
                        _upload_file_batches(client, vector_store_id, file_streams)
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

import os
import json
import io
from pathlib import Path

import logging
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        for context_ in config['context']:
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
                    if 'source' in context_:
                        # Download data from the public Google Spreadsheet
                        import requests
                        sheet_id = context_['source'].split('/d/')[1].split('/')[0]
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
                        headers = rows[0].split(',')
                        logger.info(f'Headers: {headers}')
                        data_rows = rows[1:]
                        logger.info(f'Number of data rows: {len(data_rows)}')

                        for row in data_rows:
                            values = row.split(',')
                            markdown_content.append('---')
                            for header, value in zip(headers, values):
                                markdown_content.append(f'{header.strip()}: {value.strip()}')

                        markdown_content = '\n'.join(markdown_content)
                        logger.info(f'Markdown content: {markdown_content[:100]}...')  # Log first 100 characters
                        logger.info(f'Writing to file: {filename}')
                        filename.write_text(markdown_content, encoding='utf-8')
                        logger.info(f'File written successfully')
                    content = filename.read_text()
                    content = content.split('\n---\n')
                    file_streams = [io.BytesIO(c.strip().encode('utf-8')) for c in content]
                    file_streams = [(f'{name}_{i}.md', f, 'text/markdown') for i, f in enumerate(file_streams)]
                vector_store = client.beta.vector_stores.create(name=name)
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

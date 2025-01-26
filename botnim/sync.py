import os
import json
import io
from pathlib import Path

import yaml

from openai import OpenAI

from .config import SPECS
from .collect_sources import collect_context_sources


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
            context_name = context_['name']
            if not production:
                context_name += ' - פיתוח'
            vector_store = client.beta.vector_stores.list()
            vector_store_id = None
            for vs in vector_store:
                if vs.name == context_name:
                    if replace_context:
                        client.beta.vector_stores.delete(vs.id)
                    else:
                        vector_store_id = vs.id
                    break
            if vector_store_id is None:
                file_streams = collect_context_sources(context_, config_dir)
                file_streams = [((fname if production else '_' + fname), f, t) for fname, f, t in file_streams]
                existing_files = client.files.list()
                # delete existing files:
                deleted = 0
                for fname, _, _ in file_streams:
                    for ef in existing_files:
                        if ef.filename == fname:
                            client.files.delete(ef.id)
                            deleted += 1
                vector_store = client.beta.vector_stores.create(name=context_name)
                while len(file_streams) > 0:
                    file_batch = client.beta.vector_stores.file_batches.upload_and_poll(
                        vector_store_id=vector_store.id, files=file_streams[:32]
                    )
                    print(f'VECTOR STORE {context_name} batch: ' + \
                          f'deleted {deleted}, ' + \
                          f'uploaded {file_batch.file_counts.completed}, ' + \
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
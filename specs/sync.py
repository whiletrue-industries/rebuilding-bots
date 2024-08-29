import os
import json
from pathlib import Path

import yaml

from openai import OpenAI

import dotenv

SPECS = Path(__file__).parent

dotenv.load_dotenv(SPECS / '.env')

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

def update_assistant(config, config_dir):
    tool_resources = None
    tools = None
    # Load context, if necessary
    if config.get('context'):
        for context_ in config['context']:
            name = context_['name']
            vector_store = client.beta.vector_stores.list()
            vector_store_id = None
            for vs in vector_store:
                if vs.name == name:
                    vector_store_id = vs.id
                    break
            if vector_store_id is None:
                files = config_dir.glob(context_['files'])
                file_streams = [f.open('rb') for f in files]
                vector_store = client.beta.vector_stores.create(name=name)
                file_batch = client.beta.vector_stores.file_batches.upload_and_poll(
                    vector_store_id=vector_store.id, files=file_streams
                )
                vector_store_id = vector_store.id
            tool_resources = dict(
                file_search=dict(
                    vector_store_ids=[vector_store_id],
                ),
            )
        tools = [dict(type='file_search')]

    # List all the assistants in the organization:
    assistants = client.beta.assistants.list()
    assistant_id = None
    for assistant in assistants:
        if assistant.name == config['name']:
            assistant_id = assistant.id
            break
    print(f'Assistant ID: {assistant_id}')
    asst_params = dict(
        name=config['name'],
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


if __name__ == '__main__':
    for config_fn in SPECS.glob('*/config.yaml'):
        config_dir = config_fn.parent
        with config_fn.open() as config_f:
            config = yaml.safe_load(config_f)
            config['instructions'] = (config_dir / config['instructions']).read_text()
            update_assistant(config, config_dir)
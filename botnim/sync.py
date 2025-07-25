import os
import json
import io
from pathlib import Path
import yaml
from openai import OpenAI
from .config import SPECS, is_production
from .vector_store import VectorStoreOpenAI, VectorStoreES


def get_client(environment='production'):
    """
    Get the OpenAI client based on the specified environment.
    
    Args:
        environment (str): The environment to use, either 'production', 'staging', or 'local'.
    
    Returns:
        OpenAI: The OpenAI client for the specified environment.
    """
    if environment == "production":
        api_key = os.environ['OPENAI_API_KEY_PRODUCTION']
    elif environment == "staging":
        api_key = os.environ['OPENAI_API_KEY_STAGING']
    elif environment == "local":
        api_key = os.environ['OPENAI_API_KEY_STAGING']  # Use staging key for local development
    else:
        raise ValueError(f"Invalid environment: {environment}. Use 'production', 'staging', or 'local'.")
    client = OpenAI(api_key=api_key)
    return client


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

def update_assistant(client, config, config_dir, backend, environment, replace_context=False, reindex=False):
    tool_resources = None
    tools = None
    print(f'Updating assistant: {config["name"]}')
    # Load context, if necessary
    if config.get('context'):
        ## create vector store based on backend parameter
        if backend == 'openai':
            vs = VectorStoreOpenAI(config, config_dir, production, client)
        ## Elasticsearch
        elif backend == 'es':
            vs = VectorStoreES(config, config_dir, environment=environment)
        else:
            raise ValueError(f"Unsupported backend: {backend}")
        # Update the vector store with the context
        tools, tool_resources = vs.vector_store_update(config['context'], replace_context=replace_context, reindex=reindex)
    
    # List all the assistants in the organization:
    assistants = client.beta.assistants.list()
    assistant_id = None
    assistant_name = config['name']
    if not is_production(environment):
        assistant_name += ' - פיתוח'
    
    print(f'Looking for assistant named: {assistant_name}')
    for assistant in assistants:
        print(f'Found assistant: {assistant.name} (ID: {assistant.id})')
        if assistant.name == assistant_name:
            assistant_id = assistant.id
            break
    
    print(f'Assistant ID: {assistant_id}')
    asst_params = dict(
        name=assistant_name,
        description=config['description'],
        model='gpt-4.1',
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


def sync_agents(environment, bots, backend='openai', replace_context=False, reindex=False):
    production = is_production(environment)
    client = get_client(environment)
    for config_fn in SPECS.glob('*/config.yaml'):
        config_dir = config_fn.parent
        bot_id = config_dir.name
        if bots in ['all', bot_id]:
            with config_fn.open() as config_f:
                config = yaml.safe_load(config_f)
                config['instructions'] = (config_dir / config['instructions']).read_text()
                if is_production(environment):
                    config['instructions'] = config['instructions'].replace('__dev', '')
                update_assistant(client, config, config_dir, backend, environment,
                                 replace_context=replace_context, reindex=reindex)

import os
import yaml
from pathlib import Path
from openai import OpenAI
from .kb.openai import OpenAIVectorStore
from .kb.manager import ContextManager
from .config import SPECS, get_logger

logger = get_logger(__name__)

# Initialize OpenAI client with explicit API key
api_key = os.environ.get('OPENAI_API_KEY')
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set")
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
    """Update or create an assistant with the given configuration
    
    Args:
        config: Bot configuration
        config_dir: Directory containing bot files
        production: Whether this is a production environment
        replace_context: If True, update both assistant and context. If False, only update assistant.
    """
    # Initialize vector store backend and context manager
    vs_backend = OpenAIVectorStore(production)
    context_manager = ContextManager(config_dir, vs_backend)
    
    # Find or create the main assistant first
    assistant_name = context_manager._add_environment_suffix(config['name'])
    assistant_id = None
    for assistant in client.beta.assistants.list():
        if assistant.name == assistant_name:
            assistant_id = assistant.id
            break

    # Prepare assistant configuration
    assistant_params = {
        'name': assistant_name,
        'description': config['description'],
        'model': 'gpt-4',
        'instructions': config['instructions'],
        'temperature': 0.00001,
    }
    
    # Handle tools configuration
    tools = []
    if config.get('tools'):
        for tool in config['tools']:
            if tool == 'code-interpreter':
                tools.append({"type": "code_interpreter"})
            else:
                openapi_spec = (SPECS / 'openapi' / tool).with_suffix('.yaml').open()
                openapi_spec = yaml.safe_load(openapi_spec)
                openapi_tools = openapi_to_tools(openapi_spec)
                tools.extend(openapi_tools)
    
    if tools:
        assistant_params['tools'] = tools

    if assistant_id is None:
        # Create new assistant
        assistant = client.beta.assistants.create(**assistant_params)
        assistant_id = assistant.id
        logger.info(f'Assistant created: {assistant_id}')
        # Always set up context for new assistants
        replace_context = True
    else:
        # Update existing assistant
        assistant = client.beta.assistants.update(
            assistant_id=assistant_id,
            **assistant_params
        )
        logger.info(f'Assistant configuration updated: {assistant_id}')
    
    # Set up context if configured and requested
    if config.get('context') and replace_context:
        tools_config = context_manager.setup_contexts(config['context'])
        if tools_config:
            # Merge tools configurations
            all_tools = tools + tools_config['tools']
            tool_resources = tools_config['tool_resources']
            
            # Update assistant with combined tools configuration
            assistant = client.beta.assistants.update(
                assistant_id=assistant_id,
                tools=all_tools,
                tool_resources=tool_resources
            )
            logger.info(f'Assistant updated with tools: {assistant_id}')
    elif not replace_context:
        logger.info(f'Skipping context update for assistant: {assistant_id}')

    return assistant_id

def sync_agents(environment, bots, replace_context=False):
    """Sync all or specific bots with their configurations
    
    Args:
        environment: 'production' or 'staging'
        bots: Bot ID or 'all'
        replace_context: If True, update both assistant and context. If False, only update assistant.
    """
    production = environment == 'production'
    for config_fn in SPECS.glob('*/config.yaml'):
        config_dir = config_fn.parent
        bot_id = config_dir.name
        if bots in ['all', bot_id]:
            with config_fn.open() as f:
                config = yaml.safe_load(f)
                config['instructions'] = (config_dir / config['instructions']).read_text()
                update_assistant(config, config_dir, production, replace_context)

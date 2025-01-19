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
    logger.info(f"Starting update_assistant with replace_context={replace_context}")
    
    # Initialize knowledge base backend and context manager
    kb_backend = OpenAIVectorStore(production)
    context_manager = ContextManager(config_dir, kb_backend)
    
    # Find or create the main assistant first
    assistant_name = context_manager._add_environment_suffix(config['name'])
    assistant_id = None
    existing_assistant = None
    for assistant in client.beta.assistants.list():
        if assistant.name == assistant_name:
            assistant_id = assistant.id
            existing_assistant = assistant
            logger.info(f"Found existing assistant: {assistant_id}")
            break

    if assistant_id is None:
        # Create new assistant
        assistant = client.beta.assistants.create(
            name=assistant_name,
            description=config['description'],
            model='gpt-4o',
            instructions=config['instructions'],
            temperature=0.00001,
        )
        assistant_id = assistant.id
        logger.info(f'Created new assistant: {assistant_id}')
        replace_context = True
        logger.info('Forcing replace_context=True for new assistant')
    else:
        # Update existing assistant's configuration
        assistant = client.beta.assistants.update(
            assistant_id=assistant_id,
            description=config['description'],
            instructions=config['instructions'],
            temperature=0.00001,
        )
        logger.info(f'Updated assistant configuration: {assistant_id}')
    
    # Set up context if configured
    vector_store_id = None
    if config.get('context'):
        logger.info(f"Context configuration found, replace_context={replace_context}")
        if replace_context:
            logger.info("Setting up new contexts...")
            vector_store_id = context_manager.setup_contexts(config['context'])
            
            # Update assistant with new vector store
            assistant = client.beta.assistants.update(
                assistant_id=assistant_id,
                tools=[{"type": "file_search"}],
                tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
            )
            logger.info(f'Assistant updated with new vector store: {assistant_id}')
        else:
            # Keep existing vector store connection
            try:
                if (existing_assistant and 
                    hasattr(existing_assistant, 'tool_resources') and 
                    existing_assistant.tool_resources and
                    hasattr(existing_assistant.tool_resources, 'file_search') and
                    existing_assistant.tool_resources.file_search and
                    existing_assistant.tool_resources.file_search.vector_store_ids):
                    vector_store_id = existing_assistant.tool_resources.file_search.vector_store_ids[0]
                    logger.info(f'Keeping existing vector store: {vector_store_id}')
                else:
                    logger.warning(f'No existing vector store found for assistant: {assistant_id}')
            except Exception as e:
                logger.warning(f'Error accessing existing vector store: {e}')
                logger.warning(f'Assistant tool_resources: {existing_assistant.tool_resources}')

    return assistant_id, vector_store_id

def sync_agents(environment, bots, replace_context=False):
    """Sync all or specific bots with their configurations"""
    production = environment == 'production'
    for config_fn in SPECS.glob('*/config.yaml'):
        config_dir = config_fn.parent
        bot_id = config_dir.name
        if bots in ['all', bot_id]:
            with config_fn.open() as config_f:
                config = yaml.safe_load(config_f)
                config['instructions'] = (config_dir / config['instructions']).read_text()
                update_assistant(config, config_dir, production, replace_context)

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
    """Update or create an assistant with the given configuration"""
    # Initialize knowledge base backend and context manager
    kb_backend = OpenAIVectorStore(production)
    context_manager = ContextManager(config_dir, kb_backend)
    
    # Prepare assistant parameters
    assistant_name = config['name'] + (' - פיתוח' if not production else '')
    asst_params = {
        'name': assistant_name,
        'description': config['description'],
        'model': 'gpt-4o',
        'instructions': config['instructions'],
        'temperature': 0.00001,
    }
    
    # Find or create the main assistant first
    assistant_id = None
    for assistant in client.beta.assistants.list():
        if assistant.name == assistant_name:
            assistant_id = assistant.id
            break

    if assistant_id is None:
        # Create new assistant without tools yet
        assistant = client.beta.assistants.create(**asst_params)
        assistant_id = assistant.id
        logger.info(f'Assistant created: {assistant_id}')
    
    vector_store_id = None
    # Process context to get/create vector store
    if config.get('context'):
        context = config['context'][0]  # We'll only use the first context
        # Create vector store with context name
        vector_store_id = kb_backend.create(context['name'])
        
        # Now update the assistant with file search and vector store
        assistant = client.beta.assistants.update(
            assistant_id=assistant_id,
            **asst_params,
            tools=[{"type": "file_search"}],
            tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
        )
        logger.info(f'Assistant updated with vector store: {assistant_id}')

    # Collect and upload documents if we have a vector store
    if vector_store_id and config.get('context'):
        all_documents = []
        for context in config['context']:
            documents = context_manager.collect_documents(context)
            if documents:
                all_documents.extend(documents)
            else:
                logger.warning(f"No documents found for context: {context.get('name', 'unnamed')}")
        
        if all_documents:
            kb_backend.upload_documents(vector_store_id, all_documents)

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
                update_assistant(config, config_dir, production, replace_context=replace_context)

from pathlib import Path
import dotenv
import logging
import os

ROOT = Path(__file__).parent.parent
SPECS = ROOT / 'specs'
AVAILABLE_BOTS = [d.name for d in SPECS.iterdir() if d.is_dir() and (d / 'config.yaml').exists()]

dotenv.load_dotenv(ROOT / '.env')

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the given name"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
    return logger


# Embedding model settings
DEFAULT_EMBEDDING_MODEL = 'text-embedding-3-small'
DEFAULT_EMBEDDING_SIZE = 1536
DEFAULT_BATCH_SIZE = 50

# Constants
VALID_ENVIRONMENTS = ['production', 'staging']
DEFAULT_ENVIRONMENT = 'staging'

def validate_environment(environment: str) -> str:
    """
    Validate that the environment is one of the valid options.
    
    Args:
        environment (str): The environment to validate
        
    Returns:
        str: The validated environment
        
    Raises:
        ValueError: If the environment is not valid
    """
    if environment not in VALID_ENVIRONMENTS:
        raise ValueError(f"Invalid environment: {environment}. Must be one of: {', '.join(VALID_ENVIRONMENTS)}")
    return environment

def is_production(environment: str) -> bool:
    """
    Check if the environment is production.
    
    Args:
        environment (str): The environment to check
        
    Returns:
        bool: True if the environment is production, False otherwise
    """
    validate_environment(environment)
    return environment == 'production'

def get_index_name(bot_slug: str, context_name: str, environment: str) -> str:
    """
    Get the standardized index name for Elasticsearch.
    
    Args:
        bot_slug (str): The bot slug
        context_name (str): The context name
        environment (str): The environment
        
    Returns:
        str: The standardized index name
    """
    validate_environment(environment)
    base_name = f"{bot_slug}__{context_name}".lower().replace(' ', '_')
    if not is_production(environment):
        base_name += '__dev'
    return base_name

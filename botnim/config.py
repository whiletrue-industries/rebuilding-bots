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
    # if not logger.handlers:
    #     handler = logging.StreamHandler()
    #     handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    #     logger.addHandler(handler)
    return logger


# Embedding model settings
DEFAULT_EMBEDDING_MODEL = 'text-embedding-3-small'
DEFAULT_EMBEDDING_SIZE = 1536
DEFAULT_BATCH_SIZE = 50

# Constants
VALID_ENVIRONMENTS = ['production', 'staging']
DEFAULT_ENVIRONMENT = 'staging'

def is_production(environment: str) -> bool:
    """
    Check if the environment is production.
    Args:
        environment (str): The environment to check
    Returns:
        bool: True if the environment is production, False otherwise
    """
    return environment == 'production'

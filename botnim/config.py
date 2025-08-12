from pathlib import Path
import dotenv
import logging
import os
from typing import Optional, Dict, Any
from dataclasses import dataclass
from openai import OpenAI


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
VALID_ENVIRONMENTS = ['production', 'staging', 'local']
DEFAULT_ENVIRONMENT = 'staging'

def get_openai_client(environment: str = 'staging') -> OpenAI:
    """Get OpenAI client for the given environment"""
    api_key = os.environ.get(f'OPENAI_API_KEY_{environment.upper()}')
    if not api_key:
        api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise ValueError(f'Missing OPENAI_API_KEY_{environment.upper()} or OPENAI_API_KEY environment variable')
    return OpenAI(api_key=api_key)

def is_production(environment: str) -> bool:
    """
    Check if the environment is production.
    Args:
        environment (str): The environment to check
    Returns:
        bool: True if the environment is production, False otherwise
    """
    return environment == 'production'

@dataclass
class ElasticsearchConfig:
    """Centralized Elasticsearch configuration for different environments"""
    host: str
    username: str
    password: str
    ca_cert: Optional[str] = None
    timeout: int = 30
    
    @classmethod
    def from_environment(cls, environment: str) -> 'ElasticsearchConfig':
        """
        Create Elasticsearch configuration from environment variables.
        
        Args:
            environment: The environment name ('production', 'staging', 'local')
            
        Returns:
            ElasticsearchConfig instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        env_suffix = environment.upper()
        
        # Get environment-specific variables
        host = os.getenv(f'ES_HOST_{env_suffix}')
        username = os.getenv(f'ES_USERNAME_{env_suffix}')
        password = os.getenv(f'ES_PASSWORD_{env_suffix}') or os.getenv(f'ELASTIC_PASSWORD_{env_suffix}')
        ca_cert = os.getenv(f'ES_CA_CERT_{env_suffix}')
        
        # Validate required fields
        missing_vars = []
        if not host:
            missing_vars.append(f'ES_HOST_{env_suffix}')
        if not username:
            missing_vars.append(f'ES_USERNAME_{env_suffix}')
        if not password:
            missing_vars.append(f'ES_PASSWORD_{env_suffix} or ELASTIC_PASSWORD_{env_suffix}')
            
        if missing_vars:
            raise ValueError(f"Missing required Elasticsearch environment variables for {environment} environment: {', '.join(missing_vars)}")
        
        return cls(
            host=host,  # type: ignore - validated above
            username=username,  # type: ignore - validated above
            password=password,  # type: ignore - validated above
            ca_cert=ca_cert
        )
    
    def to_elasticsearch_kwargs(self) -> Dict[str, Any]:
        """
        Convert configuration to Elasticsearch client kwargs.
        
        Returns:
            Dictionary of kwargs for Elasticsearch client initialization
        """
        kwargs = {
            'hosts': [self.host],
            'basic_auth': (self.username, self.password),
            'request_timeout': self.timeout,
        }
        
        # Only add TLS options if using HTTPS
        if self.host.startswith('https://'):
            kwargs.update({
                'verify_certs': False,
                'ca_certs': self.ca_cert,
                'ssl_show_warn': True  # Could be made configurable
            })
        
        return kwargs

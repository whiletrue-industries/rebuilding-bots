from contextlib import contextmanager
from pathlib import Path
import contextvars
import dotenv
import logging
import os
from typing import Optional, Dict, Any
from dataclasses import dataclass

from openai import OpenAI, AsyncOpenAI

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

# Set to True for the duration of a daily-refresh / fap-sync execution to
# route OpenAI calls to the dedicated fap-sync key when one is configured.
# Implemented as a contextvars.ContextVar so the flag rides asyncio.Task
# boundaries automatically — and any OpenAI client constructed inside the
# context window has its api_key baked in at construction time, so worker
# threads that consume that client (e.g. process_pdfs.ThreadPoolExecutor)
# inherit the right key by way of the client object, not by re-reading
# environ inside each worker.
_IN_FAP_SYNC: contextvars.ContextVar[bool] = contextvars.ContextVar(
    'botnim_in_fap_sync', default=False
)


@contextmanager
def fap_sync_context():
    """Enter a fap-sync execution context.

    While the context is active, ``_resolve_openai_api_key(env)`` prefers
    the ``OPENAI_API_KEY_<ENV>_FAP_SYNC`` env var over the regular
    ``OPENAI_API_KEY_<ENV>``. Lets the daily refresh use a dedicated key
    (cost isolation, rate-limit segregation) without altering the key
    used by chat retrieval, sanity, or any other runtime path on the
    same process.

    Falls back transparently to the regular env-suffixed key if the
    fap-sync-specific var is unset — so an env that hasn't opted in to
    key separation keeps the old behaviour.
    """
    token = _IN_FAP_SYNC.set(True)
    try:
        yield
    finally:
        _IN_FAP_SYNC.reset(token)


def _resolve_openai_api_key(environment: str | None = None) -> str:
    """Resolve OPENAI_API_KEY for the given environment.

    Lookup order:
      0. ``OPENAI_API_KEY_{ENV}_FAP_SYNC`` — only consulted when the current
         execution context is inside :func:`fap_sync_context`. Lets the
         daily refresh use a dedicated key without affecting runtime paths.
      1. ``OPENAI_API_KEY_{ENV}`` for the requested environment (when given).
      2. The unprefixed ``OPENAI_API_KEY`` (works on any task that wires the
         secret directly under that name).
      3. Any ``OPENAI_API_KEY_{PRODUCTION,STAGING,LOCAL}`` the host happens to
         have. This last-resort fallback keeps callers without explicit env
         context (e.g. ``dynamic_extraction.get_openai_client()``) functional
         on whichever ECS task they happen to run on, instead of hardcoding
         a staging-only fallback that fails on prod.
    """
    in_fap_sync = _IN_FAP_SYNC.get()
    candidates: list[str] = []
    if environment and in_fap_sync:
        candidates.append(f'OPENAI_API_KEY_{environment.upper()}_FAP_SYNC')
    if environment:
        candidates.append(f'OPENAI_API_KEY_{environment.upper()}')
    candidates.append('OPENAI_API_KEY')
    for env_name in ('PRODUCTION', 'STAGING', 'LOCAL'):
        if in_fap_sync:
            fap_var = f'OPENAI_API_KEY_{env_name}_FAP_SYNC'
            if fap_var not in candidates:
                candidates.append(fap_var)
        var = f'OPENAI_API_KEY_{env_name}'
        if var not in candidates:
            candidates.append(var)
    for var in candidates:
        api_key = os.environ.get(var)
        if api_key:
            return api_key
    raise ValueError(
        'Missing OpenAI API key — none of '
        + ', '.join(candidates)
        + ' is set in the environment'
    )


def get_openai_client(environment: str | None = None) -> OpenAI:
    """Get OpenAI client for the given environment."""
    return OpenAI(api_key=_resolve_openai_api_key(environment))


def get_async_openai_client(environment: str | None = None) -> AsyncOpenAI:
    """Get async OpenAI client for the given environment.

    Used by the concurrent sync pipeline (see botnim/_concurrency.py) so
    embedding and chat.completions calls can run under asyncio.gather with
    a bounded semaphore.
    """
    return AsyncOpenAI(api_key=_resolve_openai_api_key(environment))


# Embedding model settings
DEFAULT_EMBEDDING_MODEL = 'text-embedding-3-small'
DEFAULT_EMBEDDING_SIZE = 1536
DEFAULT_BATCH_SIZE = 50

# Constants
VALID_ENVIRONMENTS = ['production', 'staging', 'local']
# Use the running container's ENVIRONMENT env var so prod ECS serves prod
# config to callers that omit ?environment=. Falls back to 'staging' for
# local dev. The result is still validated against VALID_ENVIRONMENTS by
# callers, so a typo in the env var becomes a 400 instead of a silent miss.
DEFAULT_ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')

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

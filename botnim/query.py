import os
from pathlib import Path
from typing import List, Dict
from dataclasses import dataclass
from botnim.vector_store.vector_store_es import VectorStoreES
from botnim.config import DEFAULT_EMBEDDING_MODEL, get_logger, SPECS
import yaml

logger = get_logger(__name__)

@dataclass
class SearchResult:
    """Data class for search results"""
    score: float
    id: str
    content: str
    full_content: str

class QueryClient:
    """Class to handle vector store queries"""
    def __init__(self, environment: str, bot_name: str, context_name: str):
        self.environment = environment
        self.bot_name = bot_name
        self.context_name = context_name
        self.vector_store = self._initialize_vector_store(self._load_config())

    def _load_config(self) -> dict:
        """Load configuration from the specs directory"""
        specs_dir = SPECS / self.bot_name / 'config.yaml'
        if not specs_dir.exists():
            logger.warning(f"No config found for {self.bot_name}, using default config")
            return {"name": f"{self.bot_name}_assistant"}
            
        with open(specs_dir) as f:
            config = yaml.safe_load(f)
            return config

    def _initialize_vector_store(self, config) -> VectorStoreES:
        """Initialize the vector store connection"""
        return VectorStoreES(
            config=config,
            config_dir=Path('.'),
            es_host=os.getenv('ES_HOST', 'https://localhost:9200'),
            es_username=os.getenv('ES_USERNAME', 'elastic'),
            es_password=os.getenv('ES_PASSWORD'),
            es_timeout=30,
            production=self.environment == 'production'
        )

    def search(self, query_text: str, num_results: int = 7) -> List[SearchResult]:
        """
        Search the vector store with the given text
        
        Args:
            query_text (str): The text to search for
            num_results (int): Number of results to return
        
        Returns:
            List[SearchResult]: List of search results
        """
        try:
            # Get embedding using the vector store's OpenAI client
            response = self.vector_store.openai_client.embeddings.create(
                input=query_text,
                model=DEFAULT_EMBEDDING_MODEL,
            )
            embedding = response.data[0].embedding

            # Execute search directly with elasticsearch client
            results = self.vector_store.search(
                self.context_name,
                query_text, embedding,
                num_results=num_results
            )
            
            # Format results
            return [
                SearchResult(
                    score=hit['_score'],
                    id=hit['_id'],
                    content=hit['_source']['content'].strip().split('\n')[0],
                    full_content=hit['_source']['content']
                )
                for hit in results['hits']['hits']
            ]
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise

    def list_indexes(self) -> List[str]:
        """List all available indexes in the Elasticsearch database"""
        try:
            indices = self.vector_store.es_client.indices.get_alias(index=self.bot_name + "*")
            return list(indices.keys())
        except Exception as e:
            logger.error(f"Failed to list indexes: {str(e)}")
            raise

    def get_index_mapping(self) -> Dict:
        """Get the mapping (fields) for the current index"""
        try:
            index_name = self.vector_store._index_name_for_context(self.context_name)
            mapping = self.vector_store.es_client.indices.get_mapping(index=index_name)
            return mapping[index_name]['mappings']['properties']
        except Exception as e:
            logger.error(f"Failed to get index mapping: {str(e)}")
            raise

def run_query(query_text: str, environment: str, bot_name: str, context_name: str, num_results: int = 7) -> List[SearchResult]:
    """
    Run a query against the vector store
    
    Args:
        query_text (str): The text to search for
        bot_name (str): Name of the bot to use
        num_results (int): Number of results to return
        
    Returns:
        List[SearchResult]: List of search results
    """
    client = QueryClient(environment, bot_name, context_name)
    return client.search(query_text, num_results)

def get_available_indexes(environment: str, bot_name: str) -> List[str]:
    """
    Get list of available indexes
    
    Args:
        bot_name (str): Name of the bot to use
        
    Returns:
        List[str]: List of available index names
    """
    client = QueryClient(environment, bot_name, '')
    indexes = client.list_indexes()
    if bot_name:
        indexes = [index for index in indexes if index.startswith(bot_name)]
    if environment != 'production':
        indexes = [index for index in indexes if index.endswith('__dev')]
    return indexes

def format_result(result: SearchResult) -> str:
    """Format a single search result for display"""
    return f"{result.score:5.2f}: {result.id:30s}   [{result.content}]"

def get_index_fields(environment: str, bot_name: str, context_name: str) -> Dict:
    """
    Get the fields/mapping for the bot's index
    
    Args:
        bot_name (str): Name of the bot to use
        
    Returns:
        Dict: Index mapping showing all fields and their types
    """
    client = QueryClient(environment, bot_name, context_name)
    return client.get_index_mapping()

def format_mapping(mapping: Dict, indent: int = 0) -> str:
    """Format the mapping for display"""
    result = []
    for field_name, field_info in mapping.items():
        field_type = field_info.get('type', 'object')
        properties = field_info.get('properties', {})
        
        # Format current field
        indent_str = "  " * indent
        result.append(f"{indent_str}{field_name}: {field_type}")
        
        # Recursively format nested fields
        if properties:
            result.append(format_mapping(properties, indent + 1))
    
    return "\n".join(result)


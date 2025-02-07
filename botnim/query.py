import os
from pathlib import Path
from typing import List, Dict
from dataclasses import dataclass
from dotenv import load_dotenv
from openai import OpenAI
from botnim.vector_store.vector_store_es import VectorStoreES
from botnim.config import get_logger, SPECS
import yaml

logger = get_logger(__name__)
load_dotenv()

@dataclass
class SearchResult:
    """Data class for search results"""
    score: float
    id: str
    content: str
    full_content: str

class QueryClient:
    """Class to handle vector store queries"""
    def __init__(self, bot_name: str):
        self.bot_name = bot_name
        self.config = self._load_config()
        self.vector_store = self._initialize_vector_store()

    def _load_config(self) -> dict:
        """Load configuration from the specs directory"""
        specs_dir = SPECS / self.bot_name / 'config.yaml'
        if not specs_dir.exists():
            logger.warning(f"No config found for {self.bot_name}, using default config")
            return {"name": f"{self.bot_name}_assistant"}
            
        with open(specs_dir) as f:
            config = yaml.safe_load(f)
            # Store original name before any environment suffix is added
            config['original_name'] = config['name']
            return config

    def _initialize_vector_store(self) -> VectorStoreES:
        """Initialize the vector store connection"""
        return VectorStoreES(
            config=self.config,
            es_host=os.getenv('ES_HOST', 'https://localhost:9200'),
            es_username=os.getenv('ES_USERNAME', 'elastic'),
            es_password=os.getenv('ES_PASSWORD'),
            es_timeout=30,
            verify_certs=False
        )

    def _get_index_name(self) -> str:
        """Get the correct index name including environment suffix"""
        try:
            # First get all available indices
            indices = self.vector_store.es_client.indices.get_alias(index="*")
            logger.debug(f"Available indices: {list(indices.keys())}")
            
            # Get base name from config and prepare possible variations
            base_name = self.config['name'].replace(' ', '_').lower()
            dev_suffix = "_-_פיתוח"
            possible_names = [
                base_name,
                base_name + dev_suffix,
                base_name.replace('_', '')
            ]
            
            # Find matching index
            for index_name in indices:
                normalized_index = index_name.lower()
                if any(possible in normalized_index for possible in possible_names):
                    logger.debug(f"Found matching index: {index_name}")
                    return index_name
            
            # If no match found, return the default constructed name
            default_name = base_name + dev_suffix
            logger.debug(f"No matching index found, using default: {default_name}")
            return default_name
            
        except Exception as e:
            logger.error(f"Error getting index name: {str(e)}")
            # Fall back to default name construction
            return self.config['name'].replace(' ', '_').lower() + "_-_פיתוח"

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
                model="text-embedding-3-small",
            )
            embedding = response.data[0].embedding
            
            # Get correct index name
            index_name = self._get_index_name()
            logger.debug(f"Searching in index: {index_name}")
            
            # Execute search directly with elasticsearch client
            results = self.vector_store.es_client.search(
                index=index_name,
                query=self.vector_store._build_search_query(query_text, embedding, num_results),
                size=num_results,
                _source=['content']
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
            indices = self.vector_store.es_client.indices.get_alias(index="*")
            return list(indices.keys())
        except Exception as e:
            logger.error(f"Failed to list indexes: {str(e)}")
            raise

    def get_index_mapping(self) -> Dict:
        """Get the mapping (fields) for the current index"""
        try:
            index_name = self._get_index_name()
            mapping = self.vector_store.es_client.indices.get_mapping(index=index_name)
            return mapping[index_name]['mappings']['properties']
        except Exception as e:
            logger.error(f"Failed to get index mapping: {str(e)}")
            raise

def get_available_bots() -> List[str]:
    """Get list of available bots from specs directory"""
    return [d.name for d in SPECS.iterdir() if d.is_dir() and (d / 'config.yaml').exists()]

def run_query(query_text: str, bot_name: str = "takanon", num_results: int = 7) -> List[SearchResult]:
    """
    Run a query against the vector store
    
    Args:
        query_text (str): The text to search for
        bot_name (str): Name of the bot to use
        num_results (int): Number of results to return
        
    Returns:
        List[SearchResult]: List of search results
    """
    client = QueryClient(bot_name)
    return client.search(query_text, num_results)

def get_available_indexes(bot_name: str = "takanon") -> List[str]:
    """
    Get list of available indexes
    
    Args:
        bot_name (str): Name of the bot to use
        
    Returns:
        List[str]: List of available index names
    """
    client = QueryClient(bot_name)
    return client.list_indexes()

def format_result(result: SearchResult) -> str:
    """Format a single search result for display"""
    return f"{result.score:5.2f}: {result.id:30s}   [{result.content}]"

def get_index_fields(bot_name: str = "takanon") -> Dict:
    """
    Get the fields/mapping for the bot's index
    
    Args:
        bot_name (str): Name of the bot to use
        
    Returns:
        Dict: Index mapping showing all fields and their types
    """
    client = QueryClient(bot_name)
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


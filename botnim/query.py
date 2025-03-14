import os
from pathlib import Path
from typing import List, Dict, Union
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
        self.config = self._load_config()
        self.vector_store = self._initialize_vector_store(self.config)

    def _load_config(self) -> dict:
        """Load configuration from the specs directory"""
        specs_dir = SPECS / self.bot_name / 'config.yaml'
        if not specs_dir.exists():
            logger.warning(f"No config found for {self.bot_name}, using default config")
            return {"name": f"{self.bot_name}_assistant", "slug": self.bot_name}
            
        with open(specs_dir) as f:
            config = yaml.safe_load(f)
            return config

    def _initialize_vector_store(self, config) -> VectorStoreES:
        """Initialize the vector store connection"""
        return VectorStoreES(
            config=config,
            config_dir=Path('.'),
            es_host=None,
            es_username=None,
            es_password=None,
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
            # Use the standardized pattern for index name search
            search_pattern = f"{self.bot_name}__*"
            if not self.environment == 'production':
                search_pattern += '__dev'
            indices = self.vector_store.es_client.indices.get_alias(index=search_pattern)
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

    def get_default_num_results(self, context_name: str) -> int:
        """
        Get the default number of results for a context
        
        Args:
            context_name (str): Name of the context
            
        Returns:
            int: Default number of results
        """
        # Find the specific context configuration
        context_config = next(
            (ctx for ctx in self.config.get('context', []) if ctx['name'] == context_name),
            {}
        )
        
        # Get default num_results from context config or use a reasonable default
        return context_config.get('default_num_results', 7)

    def query(self, query_text: str, bot_name: str = None, context_name: str = None, num_results: int = None) -> List[SearchResult]:
        """
        Query the vector store with the given text
        
        Args:
            query_text (str): The text to search for
            bot_name (str, optional): Name of the bot to use (defaults to self.bot_name)
            context_name (str, optional): Name of the context to search in (defaults to self.context_name)
            num_results (int, optional): Number of results to return, or None to use context default
        
        Returns:
            List[SearchResult]: List of search results
        """
        # Use instance values if not provided
        bot_name = bot_name or self.bot_name
        context_name = context_name or self.context_name
        
        if num_results is None:
            # Use the method to get default num_results
            num_results = self.get_default_num_results(context_name)
        
        return self.search(query_text, num_results)

def run_query(environment: str, bot_name: str, context_name: str, query: str, num_results: int = None, format: str = "text"):
    """
    Run a query against the vector store
    
    Args:
        environment (str): Environment to use (production or staging)
        bot_name (str): Name of the bot to use
        context_name (str): Name of the context to search in
        query (str): The search query text
        num_results (int, optional): Number of results to return, or None to use context default
        format (str, optional): Output format - "dict" or "text"
        
    Returns:
        Union[List[Dict], str]: Search results in the requested format
    """
    try:
        logger.info(f"Running vector search with query: {query}, bot: {bot_name}, context: {context_name}, num_results: {num_results}")
        
        client = QueryClient(environment, bot_name, context_name)
        results = client.query(query_text=query, num_results=num_results)
        
        # Log the results
        logger.info(f"Search results: {results}")
        
        # Format results if requested
        if format == "text":
            formatted_results = format_search_results(results)
            logger.info(f"Formatted results: {formatted_results}")
            return formatted_results
        return results
    except Exception as e:
        logger.error(f"Error in run_query: {str(e)}")
        # Return a meaningful error message instead of raising
        return f"Error performing search: {str(e)}"

def format_search_results(results: List[SearchResult], format_type: str = 'text') -> Union[str, List[Dict]]:
    """
    Format search results in different output formats
    
    Args:
        results (List[SearchResult]): The search results to format
        format_type (str): The output format type ('text' or 'dict')
        
    Returns:
        Union[str, List[Dict]]: Formatted search results in the requested format
    """
    if format_type == 'dict':
        # Return as list of dictionaries for programmatic use
        return [
            {
                'score': result.score,
                'id': result.id,
                'content': result.full_content
            }
            for result in results
        ]
    else:  # Default to text format
        # Format results for human-readable text output
        formatted_results = []
        for result in results:
            formatted_results.append(
                f"[Score: {result.score:.2f}]\n"
                f"ID: {result.id}\n"
                f"Content:\n{result.full_content}\n"
                f"{'-' * 40}"
            )
        return "\n\n".join(formatted_results)

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

def format_result(result: SearchResult, show_full: bool = True) -> str:
    """
    Format a single search result for display
    
    Args:
        result: SearchResult object to format
        show_full: Whether to include the full content in the output
    """
    summary = f"{result.score:5.2f}: {result.id:30s}   [{result.content}]"
    if show_full:
        return f"{summary}\n\nFull content:\n{result.full_content}\n{'-' * 80}\n"
    return summary

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


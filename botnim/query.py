import os
from pathlib import Path
from typing import List, Dict, Union, Optional, Any
from dataclasses import dataclass
from botnim.vector_store.vector_store_es import VectorStoreES
from botnim.config import DEFAULT_EMBEDDING_MODEL, get_logger, SPECS, is_production
from botnim.vector_store.search_config import SearchModeConfig
from botnim.vector_store.search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE
import yaml
import json

logger = get_logger(__name__)

@dataclass
class SearchResult:
    """Data class for search results"""
    score: float
    id: str
    content: str
    full_content: str
    metadata: dict = None
    _explanation: dict = None  # Elasticsearch explanation
    text_score: float = None  # Text similarity score
    vector_score: float = None  # Vector similarity score
    
    @property
    def explanation(self) -> Optional[Dict[str, Any]]:
        """Get formatted explanation including both text and vector scores"""
        if not self._explanation:
            return None
            
        # Extract individual scores from combined explanation
        details = self._explanation.get('details', [])
        text_details = next((d for d in details if d['description'] == 'Text similarity score (BM25)'), {})
        vector_details = next((d for d in details if d['description'] == 'Vector similarity score'), {})
        
        self.text_score = text_details.get('value', 0)
        self.vector_score = vector_details.get('value', 0)
        
        return self._explanation

class QueryClient:
    """Class to handle vector store queries"""
    def __init__(self, store_id: str):
        self.store_id = store_id
        self.bot_name, self.context_name, self.environment = VectorStoreES.parse_index_name(store_id)
        self.config = self._load_config()
        self.vector_store = self._initialize_vector_store(self.config)

    def _load_config(self) -> dict:
        """Load configuration from the specs directory"""
        specs_dir = SPECS / self.bot_name / 'config.yaml'
        if not specs_dir.exists():
            logger.warning(f"No config found for {self.bot_name}, using default config")
            self.context_config = {}
            return {"name": f"{self.bot_name}_assistant", "slug": self.bot_name}
            
        with open(specs_dir) as f:
            config = yaml.safe_load(f)
            # Find the specific context configuration and store it as an instance property
            self.context_config = next(
                (ctx for ctx in config.get('context', []) if ctx['name'] == self.context_name),
                {}
            )
            return config

    def _initialize_vector_store(self, config) -> VectorStoreES:
        """Initialize the vector store connection"""
        return VectorStoreES(
            config=config,
            config_dir=Path('.'),
            es_timeout=30,
            production=is_production(self.environment),
        )

    def search(self, query_text: str, num_results: int=None, explain: bool=False, search_mode: Optional[SearchModeConfig] = None) -> List[SearchResult]:
        """
        Search the vector store with the given text
        
        Args:
            query_text (str): The text to search for
            num_results (int, optional): Number of results to return, or None to use context default
            explain (bool): Whether to include scoring explanation in results
            search_mode (Optional[SearchModeConfig]): Search mode configuration (required for custom modes)
        
        Returns:
            List[SearchResult]: List of search results with enhanced explanations
        """
        try:
            # Use the provided search_mode config or default
            search_mode_config = search_mode or DEFAULT_SEARCH_MODE

            # Use num_results from the search mode config if not provided
            if num_results is None:
                num_results = search_mode_config.num_results
            if num_results is None:
                num_results = self.context_config.get('default_num_results', 7)

            # Get embedding using the vector store's OpenAI client
            response = self.vector_store.openai_client.embeddings.create(
                input=query_text,
                model=DEFAULT_EMBEDDING_MODEL,
            )
            embedding = response.data[0].embedding

            # Execute search with explanations
            results = self.vector_store.search(
                self.context_name,
                query_text, embedding,
                num_results=num_results,
                explain=explain,
                search_mode=search_mode_config
            )
            
            # Format results with enhanced explanations
            return [
                SearchResult(
                    score=hit['_score'],
                    id=hit['_id'],
                    content=hit['_source']['content'].strip().split('\n')[0],
                    full_content=hit['_source']['content'],
                    metadata=hit['_source'].get('metadata', None),
                    _explanation=hit.get('_explanation', None) if explain else None
                )
                for hit in results['hits']['hits']
            ]
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
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

def run_query(*, store_id: str, query_text: str, num_results: int=7, format: str='dict', explain: bool=False, search_mode: Optional[SearchModeConfig] = None) -> Union[List[Dict], str]:
    """
    Run a query against the vector store
    
    Args:
        store_id (str): The ID of the vector store
        query_text (str): The text to search for
        num_results (int): Number of results to return
        format (str): Format of the results ('dict', 'text', 'text-short')
        explain (bool): Whether to include scoring explanation in results
        search_mode (Optional[SearchModeConfig]): Search mode configuration (required for custom modes)
        
    Returns:
        Union[List[Dict], str]: Search results in the requested format
    """
    try:
        logger.info(f"Running vector search with query: {query_text}, store_id: {store_id}, num_results: {num_results}, format: {format}, search_mode: {search_mode.name if search_mode else None}")

        client = QueryClient(store_id)
        results = client.search(query_text=query_text, num_results=num_results, explain=explain, search_mode=search_mode)

        # Log the results
        logger.info(f"Search results: {results}")

        # Format results if requested
        formatted_results = format_search_results(results, format, explain)
        if format.startswith('text'):
            logger.info(f"Formatted results: {formatted_results}")
        return formatted_results
    except Exception as e:
        logger.error(f"Error in run_query: {str(e)}")
        # Return a meaningful error message instead of raising
        return f"Error performing search: {str(e)}"

def format_search_results(results: List[SearchResult], format: str, explain: bool) -> str:
    """
    Format search results as a human-readable text string

    Args:
        results (List[SearchResult]): The search results to format
        format (str): Format of the results ('dict', 'text', 'text-short')
        explain (bool): Whether to include scoring explanation in results

    Returns:
        str: Formatted search results as a text string
    """
    # Format results for human-readable text output
    formatted_results = []
    join = format.startswith('text')
    for result in results:
        if format == 'text-short':
            formatted_results.append(
                f"{result.full_content}\n"
                f"{'-' * 10}"
            )
        elif format == 'text':
            metadata_str = ''
            if result.metadata:
                metadata_str = f"Metadata:\n{json.dumps(result.metadata, indent=2, ensure_ascii=False)}\n"
            
            explanation_str = ''
            if explain and hasattr(result, '_explanation'):
                explanation_str = f"\nScoring Explanation:\n{json.dumps(result._explanation, indent=2, ensure_ascii=False)}\n"
            
            formatted_results.append(
                f"[Score: {result.score:.2f}]\n"
                f"ID: {result.id}\n"
                f"Content:\n{result.full_content}\n"
                f"{metadata_str}"
                f"{explanation_str}"
                f"{'-' * 40}"
            )
        elif format == 'dict':
            result_dict = dict(
                id=result.id,
                score=result.score,
                content=result.full_content,
                metadata=result.metadata
            )
            if explain and hasattr(result, '_explanation'):
                result_dict['_explanation'] = result._explanation
            formatted_results.append(result_dict)
    
    if join:
        formatted_results = '\n'.join(formatted_results)
    return formatted_results or 'No results found.'

def get_available_indexes(environment: str, bot_name: str) -> List[str]:
    """
    Get list of available indexes
    
    Args:
        bot_name (str): Name of the bot to use  
        
    Returns:
        List[str]: List of available index names
    """
    client = VectorStoreES('', '.')
    search_pattern = f"*"
    if not is_production(environment):
        search_pattern += '__dev'
    indices = client.es_client.indices.get_alias(index=search_pattern)
    indices =list(indices.keys())
    indices = [index for index in indices if '__' in index]
    if bot_name:
        indices = [index for index in indices if index.startswith(bot_name)]
    if is_production(environment):
        indices = [index for index in indices if not index.endswith('__dev')]
    else:
        indices = [index for index in indices if index.endswith('__dev')]
    return indices

def get_index_fields(environment: str, bot_name: str, context_name: str) -> Dict:
    """
    Get the fields/mapping for the bot's index
    
    Args:
        bot_name (str): Name of the bot to use
        
    Returns:
        Dict: Index mapping showing all fields and their types
    """
    store_id = VectorStoreES.encode_index_name(bot_name, context_name, is_production(environment))
    client = QueryClient(store_id)
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


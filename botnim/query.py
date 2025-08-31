from pathlib import Path
from typing import List, Dict, Union, Optional, Any
from dataclasses import dataclass
from botnim.vector_store.vector_store_es import VectorStoreES
from botnim.config import DEFAULT_EMBEDDING_MODEL, get_logger, SPECS, is_production
from botnim.vector_store.search_config import SearchModeConfig
from botnim.vector_store.search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE
import yaml
import json
import re


logger = get_logger(__name__)

# Compiled regex patterns for performance
TRAILING_NUMBERS_PATTERN = re.compile(r'_\d+$')

# Constants for text formatting
DEFAULT_TRUNCATE_LENGTH = 150

# Hardcoded field configuration for metadata browse mode
METADATA_BROWSE_FIELDS = {
    # Core fields that appear at top level (no duplication)
    'core': {
        'document_type': 'computed',  # Intelligently extracted from content using _extract_document_type()
        'document_id': 'result.id',
        'relevance_score': 'result.score', 
        'title': 'extracted_data.DocumentTitle',
        'summary': 'extracted_data.Summary',
        'source_url': 'metadata.source_url'
    },
    
    # Date fields (pick first available)
    'date_fields': ['תאריך', 'תאריך_מכתב', 'PublicationDate'],
    
    # Specific metadata fields to include (only if they exist)
    'metadata_fields': [
        # Document identification
        'מספר_מסמך',        # Document number
        'סוג_מסמך',         # Document type  
        'סוג_הפניה',        # Type of inquiry
        'תקופת_כנסת',       # Knesset term
        'סדר_החלטה',        # Decision order
        
        # People/entities involved
        'שולח',            # Sender
        'נמען',            # Recipient  
        'נושא_אדם_מעורב',   # Person involved
        'משתתפים',         # Participants
        
        # Content classification
        'נושא_כללי',        # General subject
        'נושא_ספציפי',      # Specific subject
        'אופי_תלונה',       # Nature of complaint
        'סוג_החלטה',        # Decision type
        
        # Key content summaries
        'המלצות_הנחיות',    # Recommendations/Guidelines
        'ממצאי_הוועדה',     # Committee findings
        'נימוקים',          # Reasoning
        'תוצאה_עונש',       # Punishment/outcome
        'תנאים',           # Conditions
        'רקע',             # Background
        
        # References (only include if different from source_url)
        'קישור_למקור',      # Source link
        'OfficialSource',   # Official source
        
        # Additional context
        'הערות',           # Notes
    ],
    
    # Special handling field
    'full_text_field': 'טקסט_מלא'
}


def _truncate_with_ellipsis(text: str, max_length: int = DEFAULT_TRUNCATE_LENGTH) -> str:
    """Helper function to truncate text with ellipsis if needed"""
    if not text or len(text) <= max_length:
        return text
    return text[:max_length] + "..."


def _extract_document_type(extracted_data: Dict[str, Any]) -> str:
    """
    Extract document type from the extracted data.
    Uses existing document type fields from the extraction process.
    """
    if not extracted_data:
        return "unknown"
    
    # Use the document type field from PDF extraction (if available)
    document_type = extracted_data.get('סוג_מסמך', '')
    if document_type:
        return document_type
    
    # Fallback to "document" if no type was extracted
    return ""

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
            environment=self.environment,
        )

    def search(self, query_text: str, num_results: int=None, explain: bool=False, search_mode: SearchModeConfig = DEFAULT_SEARCH_MODE) -> List[SearchResult]:
        """
        Search the vector store with the given text
        
        Args:
            query_text (str): The text to search for
            num_results (int, optional): Number of results to return, or None to use context default
            explain (bool): Whether to include scoring explanation in results
            search_mode (SearchModeConfig): Search mode configuration (required for custom modes)
        
        Returns:
            List[SearchResult]: List of search results with enhanced explanations
        """
        try:
            # Use num_results from the search mode config if not provided
            if num_results is None:
                num_results = search_mode.num_results
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
                query_text, search_mode, embedding,
                num_results=num_results,
                explain=explain
            )
            
            # Format results with enhanced explanations
            # For METADATA_BROWSE mode, we'll format differently in the format_search_results function
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

def run_query(*, store_id: str, query_text: str, num_results: int=7, format: str='dict', explain: bool=False, search_mode: SearchModeConfig = DEFAULT_SEARCH_MODE) -> Union[List[Dict], str]:
    """
    Run a query against the vector store
    
    Args:
        store_id (str): The ID of the vector store
        query_text (str): The text to search for
        num_results (int): Number of results to return
        format (str): Format of the results ('dict', 'text', 'text-short', 'yaml')
        explain (bool): Whether to include scoring explanation in results
        search_mode (SearchModeConfig): Search mode configuration (required for custom modes)
        
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
        formatted_results = format_search_results(results, format, explain, search_mode)
        if format.startswith('text') or format == 'yaml':
            logger.info(f"Formatted results: {formatted_results}")
        return formatted_results
    except Exception as e:
        logger.error(f"Error in run_query: {str(e)}")
        # Return a meaningful error message instead of raising
        return f"Error performing search: {str(e)}"

def _format_metadata_browse_results(results: List[SearchResult]) -> Dict[str, Any]:
    """
    Format search results specifically for METADATA_BROWSE mode
    Returns structured metadata for browsing instead of full content
    """

    
    def _extract_metadata_fields(result: SearchResult) -> Dict[str, Any]:
        """Extract and structure metadata fields for browse display - optimized with hardcoded fields"""
        metadata = result.metadata or {}
        extracted_data = metadata.get('extracted_data', {})
        
        # 1. Build core fields (no duplication) 
        browse_item = {
            "document_type": _extract_document_type(extracted_data),
            "document_id": result.id,
            "relevance_score": round(result.score, 2),
            "title": extracted_data.get('DocumentTitle', 'ללא כותרת'),
            "summary": extracted_data.get('Summary', '')
        }
        
        # 2. Add source URL (prefer metadata.source_url, fallback to קישור_למקור)
        source_url = metadata.get('source_url', '') or extracted_data.get('קישור_למקור', '')
        if source_url:
            browse_item["source_url"] = source_url
        
        # 3. Extract only relevant metadata fields
        relevant_metadata = {}
        
        # Add date (pick first available from configured date fields)
        for date_field in METADATA_BROWSE_FIELDS['date_fields']:
            if extracted_data.get(date_field):
                relevant_metadata['date'] = extracted_data[date_field]
                break
        
        # Add all available metadata fields (dynamically, excluding core fields already shown)
        core_fields = {'DocumentTitle', 'Summary', 'title', 'status', 'document_type'}
        for field, value in extracted_data.items():
            # Skip core fields that are already displayed at top level
            if field in core_fields:
                continue
                
            # Skip קישור_למקור if it's the same as source_url to avoid duplication
            if field == 'קישור_למקור' and value == browse_item.get('source_url'):
                continue
                
            # Only include fields that have meaningful content
            if value:
                # Truncate long text fields (but preserve arrays and objects as-is)
                if isinstance(value, str) and len(value) > 200:
                    relevant_metadata[field] = value[:200] + "..."
                else:
                    relevant_metadata[field] = value
        
        # 4. Special handling for full text field
        full_text_field = METADATA_BROWSE_FIELDS['full_text_field']
        if extracted_data.get(full_text_field):
            char_count = len(extracted_data[full_text_field])
            if char_count > 0:
                relevant_metadata[full_text_field] = f"Available ({char_count:,} characters)"
        
        # 5. Only add metadata section if we have relevant fields
        if relevant_metadata:
            browse_item["metadata"] = relevant_metadata
        
        return browse_item
    
    # Format the response
    browse_response = {
        "search_mode": "METADATA_BROWSE",
        "total_results": len(results),
        "documents": [_extract_metadata_fields(result) for result in results]
    }
    
    return browse_response

def _format_metadata_browse_text(results: List[SearchResult]) -> str:
    """
    Format search results for METADATA_BROWSE mode as human-readable text
    Uses the structured data from _format_metadata_browse_results for consistency
    """
    if not results:
        return "No results found."
    
    # Get structured data 
    browse_data = _format_metadata_browse_results(results)
    documents = browse_data.get('documents', [])
    
    formatted_results = []
    header = f"🔍 **BROWSE MODE: {browse_data.get('total_results', 0)} documents found**\n"
    header += "=" * 60 + "\n\n"
    
    for i, doc in enumerate(documents, 1):
        title = doc.get('title', 'ללא כותרת')
        doc_type = doc.get('document_type', 'unknown')
        relevance = doc.get('relevance_score', 0)
        
        # Format individual result
        result_text = f"📄 **{i}. {title}**\n"
        result_text += f"   **Type:** {doc_type}\n"
        result_text += f"   **Relevance:** {relevance:.2f}\n"
        
        # Add date if available (now using standardized date field)
        metadata = doc.get('metadata', {})
        date = metadata.get('date')
        if date:
            result_text += f"   **Date:** {date}\n"
            
        # Add summary (using the new standardized summary field)
        summary = doc.get('summary', '')
        if summary:
            summary_truncated = _truncate_with_ellipsis(summary)
            result_text += f"   **Summary:** {summary_truncated}\n"
        
        # Add all additional metadata fields (excluding already shown fields)
        excluded_fields = {'date', 'טקסט_מלא'}  # Already handled separately
        for field_name, field_value in metadata.items():
            if field_name not in excluded_fields and field_value:
                # Format field name for display (convert to title case)
                display_name = field_name.replace('_', ' ').title()
                
                # Handle different value types
                if isinstance(field_value, list):
                    if field_value:  # Only show non-empty lists
                        value_str = ', '.join(str(v) for v in field_value[:3])  # Show first 3 items
                        if len(field_value) > 3:
                            value_str += f" (+{len(field_value)-3} more)"
                        result_text += f"   **{display_name}:** {value_str}\n"
                elif isinstance(field_value, str):
                    truncated_value = _truncate_with_ellipsis(field_value, 150)
                    result_text += f"   **{display_name}:** {truncated_value}\n"
                else:
                    result_text += f"   **{display_name}:** {field_value}\n"
            
        # Add full text availability if indicated
        if metadata.get('טקסט_מלא') and 'Available' in metadata.get('טקסט_מלא', ''):
            result_text += f"   **Full Text:** {metadata.get('טקסט_מלא')}\n"
            
        # Add source link if available
        source_url = doc.get('source_url', '')
        if source_url:
            result_text += f"   **Link:** {source_url}\n"
            
        result_text += f"   **ID:** {doc.get('document_id', '')}\n"
        
        formatted_results.append(result_text)
    
    footer = f"\n{'=' * 60}\n"
    footer += f"💡 **Tip:** Use regular search mode or specify document ID to get full content of any document.\n"
    
    return header + "\n".join(formatted_results) + footer

def format_search_results(results: List[SearchResult], format: str, explain: bool, search_mode: SearchModeConfig = None) -> str:
    """
    Format search results as a human-readable text string

    Args:
        results (List[SearchResult]): The search results to format
        format (str): Format of the results ('dict', 'text', 'text-short', 'yaml')
        explain (bool): Whether to include scoring explanation in results

    Returns:
        str: Formatted search results as a text string
    """
    # Check if we're in METADATA_BROWSE mode for special formatting
    is_browse_mode = search_mode and search_mode.name == "METADATA_BROWSE"
    
    # Format results for human-readable text output
    formatted_results = []
    join = format.startswith('text')
    
    # Special handling for METADATA_BROWSE mode
    if is_browse_mode and format == 'dict':
        return _format_metadata_browse_results(results)
    elif is_browse_mode and format == 'yaml':
        browse_results = _format_metadata_browse_results(results)
        return yaml.dump(browse_results, allow_unicode=True, width=1000000, sort_keys=False)
    elif is_browse_mode and format == 'text':
        return _format_metadata_browse_text(results)
    
    for result in results:
        if format == 'text-short':
            formatted_results.append(
                f"{result.full_content}"
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
        elif format == 'yaml':
            # For YAML, split header/text for each result
            parts = result.full_content.split('\n\n', 1)
            result_dict = dict(
                header=parts[0].strip(),
                text=parts[1].strip() if len(parts) > 1 else '',
            )
            formatted_results.append(result_dict)
    
    if join:
        formatted_results = '\n\n\n------------\n\n'.join(formatted_results)
    if format == 'yaml':
        return yaml.dump(formatted_results, allow_unicode=True, width=1000000, sort_keys=True)
    return formatted_results or 'No results found.'

def get_available_indexes(environment: str, bot_name: str) -> List[str]:
    """
    Get list of available indexes
    
    Args:
        environment (str): Environment to use ('local', 'staging', 'production')
        bot_name (str): Name of the bot to use  
        
    Returns:
        List[str]: List of available index names
    """
    client = VectorStoreES('', '.', environment=environment)
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
    store_id = VectorStoreES.encode_index_name(bot_name, context_name, environment)
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


import os
from pathlib import Path
from typing import List, Dict, Any
import json
from datetime import datetime
import yaml

from elasticsearch import Elasticsearch
from openai import OpenAI
from ..config import get_logger, is_production, validate_environment
from ..config import DEFAULT_EMBEDDING_MODEL, DEFAULT_EMBEDDING_SIZE

from .vector_store_base import VectorStoreBase


logger = get_logger(__name__)

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

class VectorStoreES(VectorStoreBase):
    """
    Vector store for Elasticsearch
    """	
    def __init__(self, config, config_dir, production=False, es_timeout=30):
        super().__init__(config, config_dir, production=production)
        
        # Initialize Elasticsearch client
        es_host = os.getenv('ES_HOST', 'https://localhost:9200')
        es_username = os.getenv('ES_USERNAME')
        es_password = os.getenv('ELASTIC_PASSWORD') or os.getenv('ES_PASSWORD')
        
        es_kwargs = {
            'hosts': [es_host],
            'basic_auth': (es_username, es_password),
            'request_timeout': es_timeout,
            'verify_certs': False,
            'ca_certs': os.getenv('ES_CA_CERT'),
            'ssl_show_warn': production
        }
        logger.info(f"Connecting to Elasticsearch at {es_host}")

        self.es_client = Elasticsearch(**es_kwargs)
        self.openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.init = False
        
        # Verify connection
        try:
            if not self.es_client.ping():
                raise ConnectionError("Could not ping Elasticsearch")
            info = self.es_client.info()
            logger.info(f"Connected to Elasticsearch version {info['version']['number']}")
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {str(e)}")
            raise ConnectionError(f"Could not connect to Elasticsearch: {str(e)}")

    def _index_name_for_context(self, context_name: str) -> str:
        """Standardize index name construction"""
        environment = 'production' if self.production else 'staging'
        return get_index_name(self.config['slug'], context_name, environment)

    def _build_search_query(self, query_text: str, embedding: List[float], 
                          num_results: int = 7) -> Dict[str, Any]:
        """Build the hybrid search query"""
        text_match = {
            "multi_match": {            # fields to search in
                "query": query_text,
                "fields": ["content"],
                "boost": 0.2,           # boost for text match vs vector match
                "type": 'best_fields',  # type of search: cross_fields, bool, simple, phrase, phrase_prefix
                "operator": 'or',       # operator: or, and 
            }
        }
        
        vector_match = {
            "knn": {
                "field": "vector",      # field to search in
                "query_vector": embedding,  # embedding to search for
                "k": num_results,       # number of results to get
                "num_candidates": 20,   # number of candidates to consider
                "boost": 0.5           # boost for vector match
            }
        }
        
        return {
            "bool": {
                "should": [text_match, vector_match],
                "minimum_should_match": 1,
            }
        }

    def search(self, context_name: str, query_text: str, embedding: List[float], 
               num_results: int = 7) -> Dict[str, Any]:
        """
        Search the vector store with the given text and embedding
        
        Args:
            query_text (str): The text to search for
            embedding (List[float]): The embedding vector to search with
            num_results (int): Number of results to return
            
        Returns:
            Dict[str, Any]: Elasticsearch search results
        """
        query = self._build_search_query(query_text, embedding, num_results)
        index_name = self._index_name_for_context(context_name)
        
        return self.es_client.search(
            index=index_name,
            query=query,
            size=num_results,
            _source=['content', 'metadata']
        )

    def get_or_create_vector_store(self, context, context_name, replace_context):
        """Get or create a vector store for the given context.
        Resets initialization state for each new context to allow multiple contexts.
        """
        # Reset init state for each new context
        self.init = False
        
        index_name = self._index_name_for_context(context_name)
        
        # Delete existing index if replace_context is True
        if replace_context and self.es_client.indices.exists(index=index_name):
            self.es_client.indices.delete(index=index_name)
            logger.info(f"Deleted existing index: {index_name}")
        
        # Create new index if it doesn't exist
        if not self.es_client.indices.exists(index=index_name):
            # Create index with proper mappings
            mapping = {
                "mappings": {
                    "properties": {
                        "content": {"type": "text"},
                        "vector": {
                            "type": "dense_vector",
                            "dims": DEFAULT_EMBEDDING_SIZE,
                            "index": True,
                            "similarity": "cosine"
                        },
                        "metadata": {
                            "type": "object",
                            "dynamic": True,  # Allow dynamic fields in metadata
                            "properties": {
                                "title": {"type": "text"},
                                "document_type": {"type": "keyword"},
                                "extracted_at": {"type": "date"},
                                "status": {"type": "keyword"},
                                "context_type": {"type": "keyword"},
                                "context_name": {"type": "keyword"},
                                "extracted_data": {
                                    "type": "object",
                                    "dynamic": True  # Allow dynamic fields in extracted_data
                                }
                            }
                        }
                    }
                },
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "default": {
                                "type": "standard"
                            }
                        }
                    }
                }
            }
            self.es_client.indices.create(index=index_name, body=mapping)
            logger.info(f"Created new index: {index_name}")
        
        self.init = True
        return index_name

    def upload_files(self, context, context_name, vector_store, file_streams, callback):
        """Upload files to vector store"""
        count = 0
        for filename, content_file, file_type in file_streams:
            try:
                # Skip metadata files to prevent recursion
                if filename.endswith('.metadata.json'):
                    logger.debug(f"Skipping metadata file: {filename}")
                    continue

                # Read content
                content = content_file.read().decode('utf-8')
                
                # Generate embedding
                response = self.openai_client.embeddings.create(
                    input=content,
                    model=DEFAULT_EMBEDDING_MODEL,
                )
                vector = response.data[0].embedding
                
                # Prepare base document
                document = {
                    "content": content,
                    "vector": vector,
                }
                
                # Try to load metadata from the metadata file
                clean_filename = filename[1:] if filename.startswith('_') else filename
                metadata_path = Path('specs/takanon/extraction/metadata') / f"{clean_filename}.metadata.json"
                
                try:
                    if metadata_path.exists() and not metadata_path.name.endswith('.metadata.json.metadata.json'):
                        logger.info(f"Found metadata file at {metadata_path}")
                        with open(metadata_path, 'r', encoding='utf-8') as f:
                            loaded_metadata = json.load(f)
                            document["metadata"] = loaded_metadata
                            logger.info(f"Loaded metadata from file for {filename}")
                    else:
                        logger.warning(f"No metadata file found at {metadata_path}")
                        document["metadata"] = {
                            "title": Path(filename).stem,
                            "document_type": str(file_type),
                            "extracted_at": datetime.now().isoformat(),
                            "status": "no_metadata",
                            "context_type": context.get("type", ""),
                            "context_name": context_name,
                            "extracted_data": {}
                        }
                except Exception as e:
                    logger.warning(f"Failed to load metadata for {filename}: {str(e)}")
                    document["metadata"] = {
                        "title": Path(filename).stem,
                        "document_type": str(file_type),
                        "extracted_at": datetime.now().isoformat(),
                        "status": "error",
                        "context_type": context.get("type", ""),
                        "context_name": context_name,
                        "extracted_data": {"error": str(e)}
                    }
                
                logger.info(f"Final document metadata for {filename}: {document['metadata']}")
                
                # Index document
                self.es_client.index(
                    index=vector_store,
                    id=filename,
                    document=document
                )
                
            except Exception as e:
                logger.error(f"Failed to process file {filename}: {str(e)}")
            
            count += 1
            if count % 32 == 0 and callable(callback):
                callback(count)

    def delete_existing_files(self, context_, vector_store, file_names):
        try:
            # Delete documents by their IDs (filenames)
            body = {
                "query": {
                    "ids": {
                        "values": file_names
                    }
                }
            }
            result = self.es_client.delete_by_query(
                index=vector_store,  # Use the index name directly
                body=body
            )
            return result['deleted']
        except Exception as e:
            logger.error(f"Failed to delete files: {str(e)}")
            return 0

    def update_tools(self, context_, vector_store):
        """Create a search tool for this context and add it to self.tools"""
        tool = self.create_search_tool(
            bot_name=self.config['slug'],
            context_name=context_['name'],
            environment='production' if self.production else 'staging'
        )
        self.tools.append(tool)

    def update_tool_resources(self, context_, vector_store):
        # For Elasticsearch, we don't need to set tool_resources
        self.tool_resources = None

    def verify_document_metadata(self, index_name: str, document_id: str) -> Dict:
        """Verify metadata exists for a specific document"""
        try:
            result = self.es_client.get(
                index=index_name,
                id=document_id,
                _source=['metadata']
            )
            return result['_source'].get('metadata', {})
        except Exception as e:
            logger.error(f"Failed to verify metadata for document {document_id}: {str(e)}")
            return {}

    def create_search_tool(self, bot_name: str, context_name: str, environment: str) -> Dict:
        """Creates a search tool configuration for a specific context"""
        
        # Load the bot's config to get context details
        config_path = Path(self.config_dir) / 'config.yaml'
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        # Find the specific context configuration to get its slug
        context_config = next(
            (ctx for ctx in config.get('context', []) if ctx['name'] == context_name),
            {'slug': context_name.lower().replace(' ', '_')}  # fallback to sanitized name
        )
        
        # Use the search_ prefix for the tool name
        # Format: search_botname__contextslug
        # Don't add environment to the tool name - it's handled by the index name construction
        tool_name = f"search_{bot_name}__{context_config['slug']}"
        
        # Add __dev suffix only in development mode (non-production)
        # This matches how the index names are actually created
        if environment != "production":
            tool_name += "__dev"
        
        return {
            "type": "function",
            "function": {
                "name": tool_name,
                "description": context_config.get('description', 
                    f"Search the {config['name']}'s {context_name} knowledge base using semantic search"),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": context_config.get('search_description', 
                                "The search query text")
                        },
                        "num_results": {
                            "type": "integer",
                            "description": "Number of results to return",
                            "default": 7
                        }
                    },
                    "required": ["query"]
                }
            }
        }
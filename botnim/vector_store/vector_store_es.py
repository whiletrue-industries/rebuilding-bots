import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
import hashlib
from abc import ABC, abstractmethod

from kvfile.kvfile_sqlite import CachedKVFileSQLite as KVFile
from elasticsearch import Elasticsearch
from openai import OpenAI
from ..config import DEFAULT_ENVIRONMENT, get_logger, ElasticsearchConfig, is_production
from ..config import DEFAULT_EMBEDDING_MODEL, DEFAULT_EMBEDDING_SIZE

from .vector_store_base import VectorStoreBase
from .vector_score_explainer import explain_vector_scores, combine_text_and_vector_scores
from .search_config import SearchModeConfig
from .search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE

logger = get_logger(__name__)

class VectorStoreES(VectorStoreBase):
    """
    Vector store for Elasticsearch
    """	
    def __init__(self, config, config_dir,
                 es_host=None, es_username=None, es_password=None, 
                 es_timeout=30, environment=None):
        # Environment must be explicitly specified
        if environment is None:
            raise ValueError("Environment must be explicitly specified. Use 'local', 'staging', or 'production'")
        
        env_name = environment.lower()
        if env_name not in ['local', 'staging', 'production']:
            raise ValueError(f"Invalid environment: {environment}. Must be one of: local, staging, production")
        
        # Derive production from environment
        production = is_production(env_name)
        super().__init__(config, config_dir, production=production)
        
        # Store environment for internal use
        self.environment = env_name
        
        # Use centralized configuration management
        es_config = ElasticsearchConfig.from_environment(env_name)
        
        # Override with provided parameters if any
        if es_host:
            es_config.host = es_host
        if es_username:
            es_config.username = es_username
        if es_password:
            es_config.password = es_password
        if es_timeout:
            es_config.timeout = es_timeout
            
        # Convert to Elasticsearch kwargs
        es_kwargs = es_config.to_elasticsearch_kwargs()
        
        logger.info(f"Connecting to Elasticsearch at {es_kwargs['hosts'][0]} for {env_name} environment")

        self.es_client = Elasticsearch(**es_kwargs)
        openai_api_key = os.getenv('OPENAI_API_KEY_PRODUCTION') if production else os.getenv('OPENAI_API_KEY_STAGING')
        self.openai_client = OpenAI(api_key=openai_api_key)

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
        return self.encode_index_name(
            bot_name=self.config['slug'],
            context_name=context_name,
            environment=self.environment
        )

    @staticmethod
    def encode_index_name(bot_name: str, context_name: str, environment: str) -> str:
        """Encode index name to get context name"""
        parts = [bot_name, context_name]
        if not is_production(environment):
            parts.append('dev')
        return '__'.join(parts)

    @staticmethod
    def parse_index_name(index_name: str) -> str:
        """Parse index name to get context name"""
        parts = index_name.split('__')
        if len(parts) > 1:
            bot_name = parts[0]
            context_name = parts[1]
            environment = 'staging' if len(parts) == 3 and parts[2] == 'dev' else 'production'
            return bot_name, context_name, environment
        else:
            return '', '', DEFAULT_ENVIRONMENT
        

    def _build_search_query(
        self,
        query_text: str,
        search_mode: SearchModeConfig,
        embedding: Optional[List[float]] = None,
        num_results: int = 7
    ) -> Dict[str, Any]:
        """
        Builds an Elasticsearch query based on the search mode configuration.
        Args:
            query_text: The search query string
            search_mode: Search mode configuration (required)
            embedding: Optional embedding vector for vector search
            num_results: Number of results to return
        Returns:
            Dict containing the Elasticsearch query
        """
        field_queries = []
        for field_config in search_mode.fields:
            field_es_path = field_config.field_path or f"metadata.extracted_data.{field_config.name.capitalize()}"
            if field_config.use_phrase_match:
                weight = field_config.weight.exact_match
                boost = weight * field_config.boost_factor
                if boost > 0:
                    field_queries.append({
                        "match_phrase": {
                            field_es_path: {
                                "query": query_text,
                                "boost": boost
                            }
                        }
                    })
            else:
                weight = field_config.weight.partial_match
                boost = weight * field_config.boost_factor
                if boost > 0:
                    match_query_body = {
                        "query": query_text,
                        "boost": boost
                    }
                    if field_config.fuzzy_matching:
                        match_query_body["fuzziness"] = "AUTO"
                    field_queries.append({
                        "match": {
                            field_es_path: match_query_body
                        }
                    })

        # Build the base query structure
        if search_mode.use_vector_search and embedding:
            # For ES 8.x compatibility: Use top-level knn with filter instead of nested knn in bool
            search_query = {
                "size": num_results,
                "knn": {
                    "field": "vectors.vector",
                    "query_vector": embedding,
                    "k": num_results,
                    "num_candidates": 100,
                    "filter": {
                        "term": {"vectors.source": "content"}
                    }
                }
            }
            
            # Add text queries if they exist
            if field_queries:
                search_query["query"] = {
                    "bool": {
                        "should": field_queries,
                        "minimum_should_match": 1
                    }
                }
        else:
            # Text-only search
            search_query = {
                "size": num_results,
                "query": {
                    "bool": {
                        "should": field_queries,
                        "minimum_should_match": 1
                    }
                }
            }
        
        return search_query

    def verify_document_vectors(self, index_name: str, document_id: str) -> Dict:
        """Verify vectors stored for a specific document"""
        try:
            result = self.es_client.get(
                index=index_name,
                id=document_id,
                source=['vectors']
            )
            vectors = result['_source'].get('vectors', [])
            logger.debug(f"Found {len(vectors)} vectors for document {document_id}")
            return vectors
        except Exception as e:
            logger.error(f"Failed to verify vectors for document {document_id}: {str(e)}")
            return []

    def search(self, context_name: str, query_text: str, search_mode: SearchModeConfig, embedding: List[float], num_results: int = 7, explain: bool = False) -> Dict[str, Any]:
        """
        Search the vector store with the given text and embedding
        
        Args:
            context_name (str): Name of the context to search in
            query_text (str): The text to search for
            search_mode (SearchModeConfig): Search mode configuration
            embedding (List[float]): The embedding vector to search with
            num_results (int): Number of results to return
            explain (bool): Whether to include scoring explanation in results
            
        Returns:
            Dict[str, Any]: Elasticsearch search results
        """
        query_dict = self._build_search_query(
            query_text=query_text,
            search_mode=search_mode,
            embedding=embedding,
            num_results=num_results
        )

        index_name = self._index_name_for_context(context_name)
        
        logger.info(f"Executing search on index: {index_name}")
        logger.debug(f"Query structure: {json.dumps(query_dict, indent=2)}")
        
        # Get search results with explanation if requested
        # Use **query_dict directly in the search call (ES 8.x compatible)
        results = self.es_client.search(
            index=index_name,
            **query_dict,
            source=['content', 'metadata', 'vectors'],
            explain=explain
        )
        
        logger.info(f"Retrieved {len(results['hits']['hits'])} results")
        
        # Add vector similarity explanations if explain=True
        if explain:
            for hit in results['hits']['hits']:
                logger.debug(f"Processing explanation for document: {hit['_id']}")
                
                if 'vectors' in hit['_source']:
                    logger.debug(f"Found {len(hit['_source']['vectors'])} vectors in document {hit['_id']}")
                    
                    # Calculate vector similarity scores
                    vector_score = explain_vector_scores(
                        embedding,
                        hit['_source']['vectors']
                    )
                    
                    # Get text similarity explanation
                    text_score = hit.get('_explanation', {})
                    
                    hit['_explanation'] = combine_text_and_vector_scores(
                        text_score=text_score,
                        vector_score=vector_score
                    )
                    
                    logger.debug(f"Generated explanation for document {hit['_id']}")
                    
                else:
                    logger.warning(f"No vectors found in document: {hit['_id']}")
                
                # Remove vectors from source to avoid returning large embeddings
                if 'vectors' in hit['_source']:
                    del hit['_source']['vectors']
        
        return results

    def get_or_create_vector_store(self, context, context_name, replace_context):
        """Get or create a vector store for the given context.
        """
        
        index_name = self._index_name_for_context(context_name)
        
        # Delete existing index if replace_context is True
        if replace_context and self.es_client.indices.exists(index=index_name):
            logger.info(f"Replacing existing index: {index_name}")
            self.es_client.indices.delete(index=index_name)
        
        # Create new index if it doesn't exist
        if not self.es_client.indices.exists(index=index_name):
            logger.info(f"Creating index: {index_name}")
            # Create index with proper mappings
            mapping = {
                "mappings": {
                    "properties": {
                        "content": {"type": "text"},
                        "vectors": {
                            "type": "nested",
                            "properties": {
                                "vector": {
                                    "type": "dense_vector",
                                    "dims": DEFAULT_EMBEDDING_SIZE,
                                    "index": True,
                                    "similarity": "cosine"
                                },
                                "source": {
                                    "type": "keyword"
                                }
                            }
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
                                    "dynamic": True,  # Allow dynamic fields in extracted_data
                                    "properties": {
                                        "DocumentTitle": {
                                            "type": "text",
                                            "fields": {
                                                "keyword": {
                                                    "type": "keyword",
                                                    "ignore_above": 256
                                                }
                                            }
                                        }
                                    }
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
            self.es_client.indices.create(index=index_name, **mapping)
            logger.info(f"Index created successfully: {index_name}")
        
        return index_name

    def upload_files(self, context, context_name, vector_store, file_streams, callback):
        """Upload files to vector store"""
        count = 0
        embedding_cache = KVFile(location=str(Path(__file__).parent.parent.parent / 'cache' / 'embedding'))
        for filename, content_file, file_type, metadata in file_streams:
            try:
                # Skip metadata files to prevent recursion
                if not filename.endswith('.md'):
                    logger.debug(f"Skipping non-markdown file: {filename}")
                    continue

                # Read content
                content = content_file.read().decode('utf-8')
                cache_key = hashlib.sha256(content.strip().encode('utf-8')).hexdigest()[:16]
                vector = embedding_cache.get(cache_key, default=None)
                if not vector:                                
                    # Generate content embedding
                    response = self.openai_client.embeddings.create(
                        input=content,
                        model=DEFAULT_EMBEDDING_MODEL,
                    )
                    vector = response.data[0].embedding
                    # Cache the vector
                    embedding_cache.set(cache_key, vector)
                
                # Prepare base document with content vector
                document = {
                    "content": content,
                    "vectors": [{
                        "vector": vector,
                        "source": "content"
                    }]
                }
                
                # Add metadata to document
                if metadata:
                    logger.info(f"Using extracted metadata for {filename}")
                    document['metadata'] = {
                        'title': Path(filename).stem,
                        'document_type': str(file_type),
                        'extracted_at': datetime.now().isoformat(),
                        'status': 'extracted',
                        'context_type': context.get("type", ""),
                        'context_name': context_name,
                        'extracted_data': metadata
                    }

                    # Generate description embedding if available
                    description = metadata.get('Description')  # Direct access to Description field
                    if description:
                        try:
                            logger.debug(f"Generating description embedding for {filename}")
                            description_response = self.openai_client.embeddings.create(
                                input=description,
                                model=DEFAULT_EMBEDDING_MODEL,
                            )
                            description_vector = description_response.data[0].embedding
                            
                            document['vectors'].append({
                                "vector": description_vector,
                                "source": "description"
                            })
                            logger.debug(f"Added description vector for {filename}")
                            
                        except Exception as e:
                            logger.error(f"Failed to generate description embedding for {filename}: {str(e)}")
                            # Continue processing without description vector
                    else:
                        logger.debug(f"No description found in metadata for {filename}")

                
                # Index document
                result = self.es_client.index(
                    index=vector_store,
                    id=filename,
                    document=document
                )
                logger.debug(f"Indexed document {filename} successfully")
                
            except Exception as e:
                logger.error(f"Failed to process file {filename}: {str(e)}")
            
            count += 1
            if count % 32 == 0 and callable(callback):
                callback(count)

        # Final callback for remaining files
        if count % 32 != 0 and callable(callback):
            callback(count)

        embedding_cache.close()
        logger.info(f"Completed upload of {count} files to {vector_store}")
        
    def delete_existing_files(self, context_, vector_store, file_names):
        try:
            # Delete documents by their IDs (filenames)
            query = {
                "ids": {
                    "values": file_names
                }
            }
            result = self.es_client.delete_by_query(
                index=vector_store,  # Use the index name directly
                query=query
            )
            return result['deleted']
        except Exception as e:
            logger.error(f"Failed to delete files: {str(e)}")
            return 0

    def _get_tool_description_from_config(self, context_) -> str:
        """Get tool description from context configuration"""
        description = context_.get('description', '')
        examples = context_.get('examples', '')
        
        if description and examples:
            return f"{description}. Examples: {examples}"
        elif description:
            return description
        else:
            # Fallback if no description in config
            context_name = context_.get('slug', 'unknown')
            return f"Semantic search the '{context_name}' vector store"

    def _get_search_mode_description(self, context_) -> str:
        """Get search mode description based on context configuration"""
        # Default search mode description for all contexts
        base_description = "Search mode. "
        
        # Build the description based on context type and available modes
        context_slug = context_.get('slug', '')
        
        # Determine which modes are most relevant for this context type
        if any(keyword in context_slug for keyword in ['legal_text', 'common_knowledge']):
            # Legal text contexts support SECTION_NUMBER mode
            modes = [
                "'SECTION_NUMBER': Specialized search for finding legal text sections by their number (e.g. 'סעיף 12'). Requires both section number and resource name (default 3 results)",
                "'REGULAR': Semantic + full text search across all main fields (default 7 results)", 
                "'METADATA_BROWSE': Browse documents with structured metadata summaries instead of full content (25 results)"
            ]
        elif any(keyword in context_slug for keyword in ['legal_advisor_opinions', 'legal_advisor_letters', 'committee_decisions', 'ethics_decisions']):
            # Document-based contexts prioritize METADATA_BROWSE
            modes = [
                "'METADATA_BROWSE': Browse documents with structured metadata summaries instead of full content (25 results)",
                "'REGULAR': Semantic + full text search across all main fields (7 results)"
            ]
        else:
            # Default mode list for unknown contexts
            modes = [
                "'REGULAR': Semantic + full text search across all main fields (default 7 results)",
                "'METADATA_BROWSE': Browse documents with structured metadata summaries instead of full content (25 results)"
            ]
        
        return base_description + ". ".join(modes) + "."

    def update_tools(self, context_, vector_store):
        # vector_store is now just the index name string
        
        # Get tool description from configuration instead of hardcoded values
        tool_description = self._get_tool_description_from_config(context_)
        search_mode_description = self._get_search_mode_description(context_)
        
        self.tools.append({
            "type": "function",
            "function": {
                "name": f"search_{vector_store}",
                "description": tool_description,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The query string to use for semantic/free text search"
                        },
                        "search_mode": {
                            "type": "string",
                            "description": search_mode_description,
                            "enum": [mode.name for mode in SEARCH_MODES.values()],
                            "default": DEFAULT_SEARCH_MODE.name
                        },
                        "num_results": {
                            "type": "integer",
                            "description": "Number of results to return. Leave empty to use the default for the search mode.",
                            "default": 7
                        }
                    },
                    "required": ["query"]
                }
            }
        })

    def update_tool_resources(self, context_, vector_store):
        # For Elasticsearch, we don't need to set tool_resources - which is OpenAI's vector store
        self.tool_resources = None

    def verify_document_metadata(self, index_name: str, document_id: str) -> Dict:
        """Verify metadata exists for a specific document"""
        try:
            result = self.es_client.get(
                index=index_name,
                id=document_id,
                source=['metadata']
            )
            return result['_source'].get('metadata', {})
        except Exception as e:
            logger.error(f"Failed to verify metadata for document {document_id}: {str(e)}")
            return {}

    def verify_metadata(self, document_id: str) -> Dict:
        """Verify metadata for a document"""
        try:
            result = self.es_client.get(
                index=self._index_name_for_context(self.context_name),
                id=document_id,
                source=['metadata']
            )
            return result['_source'].get('metadata', {})
        except Exception as e:
            logger.error(f"Failed to verify metadata for document {document_id}: {str(e)}")
            return {}
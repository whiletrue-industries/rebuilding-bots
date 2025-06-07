import os
from pathlib import Path
from typing import List, Dict, Any
import json
from datetime import datetime
import hashlib

from kvfile.kvfile_sqlite import CachedKVFileSQLite as KVFile
from elasticsearch import Elasticsearch
from openai import OpenAI
from ..config import DEFAULT_ENVIRONMENT, get_logger
from ..config import DEFAULT_EMBEDDING_MODEL, DEFAULT_EMBEDDING_SIZE

from .vector_store_base import VectorStoreBase
from .vector_score_explainer import explain_vector_scores, combine_text_and_vector_scores

logger = get_logger(__name__)

class VectorStoreES(VectorStoreBase):
    """
    Vector store for Elasticsearch
    """	
    def __init__(self, config, config_dir,
                 es_host=None, es_username=None, es_password=None, 
                 es_timeout=30, production=False):
        super().__init__(config, config_dir, production=production)
        
        # Initialize Elasticsearch client
        es_kwargs = {
            'hosts': [es_host or os.getenv('ES_HOST', 'https://localhost:9200')],
            'basic_auth': (es_username or os.getenv('ES_USERNAME'),
                           es_password or os.getenv('ELASTIC_PASSWORD') or os.getenv('ES_PASSWORD')),
            'request_timeout': es_timeout,
            'verify_certs': False,
            'ca_certs': os.getenv('ES_CA_CERT'),
            'ssl_show_warn': production
        }
        logger.info(f"Connecting to Elasticsearch at {es_kwargs['hosts'][0]}")

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
            production=self.production
        )

    @staticmethod
    def encode_index_name(bot_name: str, context_name: str, production: bool) -> str:
        """Encode index name to get context name"""
        parts = [bot_name, context_name]
        if not production:
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
        

    def _build_search_query(self, query_text: str, embedding: List[float], 
                          num_results: int = 7) -> Dict[str, Any]:
        """Build the hybrid search query"""
        text_match = {
            "multi_match": {            # fields to search in
                "query": query_text,
                "fields": [
                    "content",
                    "metadata.title",
                    "metadata.extracted_data.DocumentTitle",
                    "metadata.extracted_data.DocumentTitle.keyword^3",  # Boost keyword matches
                    "metadata.extracted_data.OfficialSource^10",
                    "metadata.extracted_data.OfficialRoles.Role",
                    "metadata.extracted_data.Description",
                    "metadata.extracted_data.AdditionalKeywords",
                    "metadata.extracted_data.Topics",
                ],
                "boost": 0.4,           # boost for text match vs vector match
                "type": 'cross_fields',  # type of search: cross_fields, bool, simple, phrase, phrase_prefix
                "operator": 'or',       # operator: or, and 
            }
        }
        
        # Enhanced priority for document title matches
        # This creates several potential exact title matches by taking the first 1-3 words of the query
        title_matches = []
        words = query_text.split()
        
        # Try exact match with full query
        title_matches.append({
            "term": {
                "metadata.extracted_data.DocumentTitle.keyword": {
                    "value": query_text,
                    "boost": 10.0
                }
            }
        })
        
        # Try with first 1-3 words (to match document titles like "חוק הכנסת")
        if len(words) >= 2:
            potential_title = " ".join(words[0:2])
            title_matches.append({
                "term": {
                    "metadata.extracted_data.DocumentTitle.keyword": {
                        "value": potential_title,
                        "boost": 20.0  # Higher boost for shorter, exact title matches
                    }
                }
            })
            
        if len(words) >= 3:
            potential_title = " ".join(words[0:3])
            title_matches.append({
                "term": {
                    "metadata.extracted_data.DocumentTitle.keyword": {
                        "value": potential_title,
                        "boost": 15.0
                    }
                }
            })
        
        # Add a prefix match as fallback
        title_matches.append({
            "prefix": {
                "metadata.extracted_data.DocumentTitle.keyword": {
                    "value": words[0] if words else "",
                    "boost": 5.0
                }
            }
        })
        
        vector_match = {
            "bool": {
                "should": [
                    {
                        "nested": {
                            "path": "vectors",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term": {"vectors.source": "content"}},
                                        {
                                            "knn": {
                                                "field": "vectors.vector",
                                                "query_vector": embedding,
                                                "k": num_results,
                                                "num_candidates": 20
                                            }
                                        }
                                    ]
                                }
                            },
                            "boost": 1.0
                        }
                    },
                    {
                        "nested": {
                            "path": "vectors",
                            "query": {
                                "bool": {
                                    "must": [
                                        {"term": {"vectors.source": "description"}},
                                        {
                                            "knn": {
                                                "field": "vectors.vector",
                                                "query_vector": embedding,
                                                "k": num_results,
                                                "num_candidates": 20
                                            }
                                        }
                                    ]
                                }
                            },
                            "boost": 0.8
                        }
                    }
                ]
            }
        }
        
        # Build final query with all components
        query = {
            "bool": {
                "should": [text_match, vector_match] + title_matches,
                "minimum_should_match": 1,
            }
        }
        
        return query

    def verify_document_vectors(self, index_name: str, document_id: str) -> Dict:
        """Verify vectors stored for a specific document"""
        try:
            result = self.es_client.get(
                index=index_name,
                id=document_id,
                _source=['vectors']
            )
            vectors = result['_source'].get('vectors', [])
            logger.info(f"Found {len(vectors)} vectors for document {document_id}")
            logger.info(f"Vector sources: {[vec.get('source', 'unknown') for vec in vectors]}")
            return vectors
        except Exception as e:
            logger.error(f"Failed to verify vectors for document {document_id}: {str(e)}")
            return []

    def search(self, context_name: str, query_text: str, embedding: List[float], 
               num_results: int = 7, explain: bool = False) -> Dict[str, Any]:
        """
        Search the vector store with the given text and embedding
        
        Args:
            query_text (str): The text to search for
            embedding (List[float]): The embedding vector to search with
            num_results (int): Number of results to return
            explain (bool): Whether to include scoring explanation in results
            
        Returns:
            Dict[str, Any]: Elasticsearch search results
        """
        query = self._build_search_query(query_text, embedding, num_results)
        index_name = self._index_name_for_context(context_name)
        
        logger.info(f"Executing search on index: {index_name}")
        logger.debug(f"Query structure: {json.dumps(query, indent=2)}")
        
        # Get search results with explanation if requested
        results = self.es_client.search(
            index=index_name,
            query=query,
            size=num_results,
            _source=['content', 'metadata', 'vectors'],
            explain=explain
        )
        
        logger.info(f"Retrieved {len(results['hits']['hits'])} results")
        
        # Add vector similarity explanations if explain=True
        if explain:
            for hit in results['hits']['hits']:
                logger.info(f"Processing hit: {hit['_id']}")
                
                # Verify stored vectors
                stored_vectors = self.verify_document_vectors(index_name, hit['_id'])
                logger.info(f"Stored vectors: {[vec.get('source', 'unknown') for vec in stored_vectors]}")
                
                if 'vectors' in hit['_source']:
                    logger.info(f"Found {len(hit['_source']['vectors'])} vectors in document")
                    logger.debug(f"Vector sources: {[vec.get('source', 'unknown') for vec in hit['_source']['vectors']]}")
                    
                    # Calculate vector similarity scores
                    vector_score = explain_vector_scores(
                        embedding,
                        hit['_source']['vectors']
                    )
                    
                    # Get text similarity explanation
                    text_score = hit.get('_explanation', {})
                    
                    # Combine scores
                    hit['_explanation'] = combine_text_and_vector_scores(
                        text_score=text_score,
                        vector_score=vector_score
                    )
                    
                    logger.info(f"Final explanation for {hit['_id']}: {json.dumps(hit['_explanation'], indent=2)}")
                    
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
            logger.info(f"Deleting existing index due to replace_context flag: {index_name}")
            self.es_client.indices.delete(index=index_name)
            logger.info(f"Deleted existing index: {index_name}")
        
        # Create new index if it doesn't exist
        if not self.es_client.indices.exists(index=index_name):
            logger.info(f"Creating new index with updated mapping: {index_name}")
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
            self.es_client.indices.create(index=index_name, body=mapping)
            logger.info(f"Created new index: {index_name}")
        
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
                            logger.info(f"Found description for {filename}: {description[:100]}...")
                            logger.debug(f"Generating description embedding for {filename}")
                            try:
                                description_response = self.openai_client.embeddings.create(
                                    input=description,
                                    model=DEFAULT_EMBEDDING_MODEL,
                                )
                                description_vector = description_response.data[0].embedding
                                logger.debug(f"Generated description vector of length {len(description_vector)}")
                                
                                document['vectors'].append({
                                    "vector": description_vector,
                                    "source": "description"
                                })
                                logger.info(f"Successfully added description vector for {filename}")
                                
                                # Verify the vector was added
                                if not any(v.get('source') == 'description' for v in document['vectors']):
                                    logger.error(f"Description vector was not properly added to document vectors for {filename}")
                            except Exception as e:
                                logger.error(f"Failed to generate description embedding for {filename}: {str(e)}")
                                logger.error(f"Description text: {description[:200]}...")
                                raise
                        except Exception as e:
                            logger.error(f"Error processing description for {filename}: {str(e)}")
                            logger.error(f"Full error details: {str(e)}")
                    else:
                        logger.warning(f"No description found in metadata for {filename}")
                        logger.debug(f"Available metadata fields: {list(metadata.keys())}")
                
                #TODO: REMOVED FOR NOW
                # # Try to load metadata from the metadata file
                # clean_filename = filename[1:] if filename.startswith('_') else filename
                # metadata_path = Path('specs/takanon/extraction/metadata') / f"{clean_filename}.metadata.json"
                
                # try:
                #     if metadata_path.exists() and not metadata_path.name.endswith('.metadata.json.metadata.json'):
                #         logger.info(f"Found metadata file at {metadata_path}")
                #         with open(metadata_path, 'r', encoding='utf-8') as f:
                #             loaded_metadata = json.load(f)
                #             document["metadata"] = loaded_metadata
                #             logger.info(f"Loaded metadata from file for {filename}")
                #     else:
                #         logger.warning(f"No metadata file found at {metadata_path}")
                #         document["metadata"] = {
                #             "title": Path(filename).stem,
                #             "document_type": str(file_type),
                #             "extracted_at": datetime.now().isoformat(),
                #             "status": "no_metadata",
                #             "context_type": context.get("type", ""),
                #             "context_name": context_name,
                #             "extracted_data": {}
                #         }
                # except Exception as e:
                #     logger.warning(f"Failed to load metadata for {filename}: {str(e)}")
                #     document["metadata"] = {
                #         "title": Path(filename).stem,
                #         "document_type": str(file_type),
                #         "extracted_at": datetime.now().isoformat(),
                #         "status": "error",
                #         "context_type": context.get("type", ""),
                #         "context_name": context_name,
                #         "extracted_data": {"error": str(e)}
                #     }
                
                # logger.info(f"Final document metadata for {filename}: {document['metadata']}")
                
                # Index document
                result = self.es_client.index(
                    index=vector_store,
                    id=filename,
                    document=document
                )
                logger.debug(f"Index result: {result}")
                
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
        # vector_store is now just the index name string
        self.tools.append({
            "type": "function",
            "function": {
                "name": f"search_{vector_store}",
                "description": f"Semantic search the '{vector_store}' vector store",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The query string to use for searching"
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
                _source=['metadata']
            )
            return result['_source'].get('metadata', {})
        except Exception as e:
            logger.error(f"Failed to verify metadata for document {document_id}: {str(e)}")
            return {}
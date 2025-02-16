import os
from pathlib import Path
from typing import List, Dict, Any

from elasticsearch import Elasticsearch
from openai import OpenAI
from botnim.config import get_logger
from botnim.config import DEFAULT_EMBEDDING_MODEL, DEFAULT_EMBEDDING_SIZE

from .vector_store_base import VectorStoreBase

logger = get_logger(__name__)

class VectorStoreES(VectorStoreBase):
    """
    Vector store for Elasticsearch
    """	
    def __init__(self, config, config_dir, es_host, es_username, es_password, 
                 es_timeout=30, production=False):
        super().__init__(config, config_dir, production=production)
        
        # Initialize Elasticsearch client
        es_kwargs = {
            'hosts': [es_host],
            'basic_auth': (es_username, es_password or os.getenv('ELASTIC_PASSWORD')),
            'request_timeout': es_timeout,
            'verify_certs': production,
            'ssl_show_warn': production
        }
        print(es_kwargs)

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
        return self.env_name_slug(f"{self.config['slug']}__{context_name}".lower().replace(' ', '_'))

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
            _source=['content']
        )

    def get_or_create_vector_store(self, context, context_name, replace_context):
        ret = None # return value 
        index_name = self._index_name_for_context(context_name)
        
        # Check if index exists
        if self.es_client.indices.exists(index=index_name):
            if replace_context and not self.init:
                self.es_client.indices.delete(index=index_name)
            else:
                ret = {'id': index_name, 'name': index_name}
        
        if not ret: # if index does not exist
            assert not self.init, 'Attempt to create a new vector store after initialization'
            # Create index with proper mapping
            mapping = {
                "mappings": {
                    "properties": {
                        "content": {"type": "text"},
                        "vector": {
                            "type": "dense_vector",
                            "dims": DEFAULT_EMBEDDING_SIZE,
                            "index": True,
                            "similarity": "cosine"
                        }
                    }
                }
            }
            self.es_client.indices.create(index=index_name, body=mapping)
            ret = {'id': index_name, 'name': index_name}
            
        self.init = True
        return ret

    def upload_files(self, context, context_name, vector_store, file_streams, callback):
        count = 0
        for filename, content_file, _ in file_streams:
            try:
                # Read content
                content = content_file.read().decode('utf-8')

                # Generate embedding
                response = self.openai_client.embeddings.create(
                    input=content,
                    model=DEFAULT_EMBEDDING_MODEL,
                )
                vector = response.data[0].embedding

                # Index document
                self.es_client.index(
                    index=vector_store['id'],
                    id=filename,
                    document={
                        "content": content,
                        "vector": vector
                    }
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
                    "terms": {
                        "_id": file_names
                    }
                }
            }
            result = self.es_client.delete_by_query(
                index=vector_store['id'],
                body=body
            )
            return result['deleted']
        except Exception as e:
            logger.error(f"Failed to delete files: {str(e)}")
            return 0

    def update_tools(self, context_, vector_store):
        id = vector_store['id']
        if len(self.tools) == 0:
            self.tools.append({
                "type": "function",
                "function": {
                    "name": f"search_{id}",
                    "description": f"Semantic search the '{id}' vector store",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "The query string to use for searching"
                            }
                        },
                        "required": ["query"]
                    }
                }
            })

    def update_tool_resources(self, context_, vector_store):
        # For Elasticsearch, we don't need to set tool_resources - which is OpenAI's vector store
        self.tool_resources = None

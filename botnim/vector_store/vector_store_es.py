from .vector_store_base import VectorStoreBase
from elasticsearch import Elasticsearch
from openai import OpenAI
import os
from botnim.config import get_logger
from pathlib import Path
from typing import List, Dict, Any

logger = get_logger(__name__)

class VectorStoreES(VectorStoreBase):
    """
    Vector store for Elasticsearch
    """	
    def __init__(self, config, es_host, es_username, es_password, 
                 es_timeout=30, verify_certs=False):
        super().__init__(config, Path("."), production=not verify_certs)
        
        # Initialize Elasticsearch client
        es_kwargs = {
            'hosts': [es_host],
            'basic_auth': (es_username, es_password or os.getenv('ELASTIC_PASSWORD')),
            'request_timeout': es_timeout,
            'verify_certs': verify_certs,
            'ssl_show_warn': False
        }
        
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

    def search(self, query_text: str, embedding: List[float], 
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
        index_name = self.env_name(self.config['name']).lower().replace(' ', '_')
        
        return self.es_client.search(
            index=index_name,
            query=query,
            size=num_results,
            _source=['content']
        )

    def get_or_create_vector_store(self, context, context_name, replace_context):
        ret = None # return value 
        vs_name = self.env_name(self.config['name']).lower().replace(' ', '_')
        
        # Check if index exists
        if self.es_client.indices.exists(index=vs_name):
            if replace_context and not self.init:
                self.es_client.indices.delete(index=vs_name)
            else:
                ret = {'id': vs_name, 'name': vs_name}
        
        if not ret: # if index does not exist
            assert not self.init, 'Attempt to create a new vector store after initialization'
            # Create index with proper mapping
            mapping = {
                "mappings": {
                    "properties": {
                        "content": {"type": "text"},
                        "vector": {
                            "type": "dense_vector",
                            "dims": 1536,  # OpenAI embedding size
                            "index": True,
                            "similarity": "cosine"
                        }
                    }
                }
            }
            self.es_client.indices.create(index=vs_name, body=mapping)
            ret = {'id': vs_name, 'name': vs_name}
            
        self.init = True
        return ret

    def upload_files(self, context, context_name, vector_store, file_streams, callback):
        count = 0
        while len(file_streams) > 0:
            current = file_streams[:32]
            
            for filename, content_file, content_type in current:
                try:
                    # Read content
                    content = content_file.read().decode('utf-8')
                    
                    # Generate embedding
                    response = self.openai_client.embeddings.create(
                        input=content,
                        model="text-embedding-ada-002",
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
                    
            count += len(current)
            if callable(callback):
                callback(count)
            file_streams = file_streams[32:]

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
        if len(self.tools) == 0:
            self.tools.append(dict(
                type='file_search',
                file_search=dict(
                    max_num_results=context_.get('max_num_results', 20),
                ),
            ))

    def update_tool_resources(self, context_, vector_store):
        if self.tool_resources is None:
            self.tool_resources = dict(
                file_search=dict(
                    vector_store_ids=[vector_store['id']],
                ),
            )

    
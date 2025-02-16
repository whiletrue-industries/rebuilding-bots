from .vector_store_base import VectorStoreBase
from elasticsearch import Elasticsearch
from openai import OpenAI
import os
from botnim.config import get_logger
from botnim.config import DEFAULT_EMBEDDING_MODEL 

logger = get_logger(__name__)

class VectorStoreES(VectorStoreBase):
    """
    Vector store for Elasticsearch
    """	
    def __init__(self, config, config_dir, production, 
                 es_host, es_username, es_password, 
                 es_timeout=30, verify_certs=False):
        super().__init__(config, config_dir, production)
        
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

    def get_or_create_vector_store(self, context, context_name, replace_context):
        ret = None # return value 
        vs_name = f"{self.env_name(self.config['name'])}_{context_name}".lower().replace(' ', '_')
        
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
        # Process files in batches of 32
        for i in range(0, len(file_streams), 32):
            batch = file_streams[i:i+32]
            
            for filename, content_file, content_type in batch:
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
            
            count += len(batch)
            if callable(callback):
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

    
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

from elasticsearch import Elasticsearch
from openai import OpenAI

# Add the project root to Python path to import our config
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from botnim.config import ElasticsearchConfig

ES_INDEX = 'test'
OPENAI_EMBEDDING_SIZE = 1536
OPENAI_TEXT_EMBEDDING_MODEL = 'text-embedding-3-small'

DATA_SOURCES_PATH = Path(__file__).parent.parent.parent / 'specs' / 'takanon' / 'extraction'
CHUNK_SIZE = 256
CHUNK_OVERLAP = 64
RECREATE_INDEX = True

def get_es_connection_params(environment='staging'):
    """Get Elasticsearch connection parameters using centralized config"""
    try:
        es_config = ElasticsearchConfig.from_environment(environment)
        
        # Get OpenAI API key
        if environment == 'production':
            openai_api_key = os.getenv('OPENAI_API_KEY_PRODUCTION')
        else:  # staging or local
            openai_api_key = os.getenv('OPENAI_API_KEY_STAGING')
        
        if not openai_api_key:
            raise ValueError(f"Missing OPENAI_API_KEY_{environment.upper()}")
        
        return es_config, openai_api_key
        
    except ValueError as e:
        raise ValueError(f"Configuration error for {environment} environment: {e}")

if __name__ == '__main__':
    # Parse command line arguments
    environment = 'staging'  # default
    if len(sys.argv) > 1:
        if sys.argv[1] in ['production', 'staging', 'local']:
            environment = sys.argv[1]
        else:
            print("Usage: python demo-load-data-to-es.py [production|staging|local]")
            print("Default: staging")
            sys.exit(1)
    
    print(f"Using {environment} environment...")
    
    # Get connection parameters using centralized config
    es_config, openai_api_key = get_es_connection_params(environment)
    
    # Use the centralized configuration to create Elasticsearch client
    es_kwargs = es_config.to_elasticsearch_kwargs()
    es_client = Elasticsearch(**es_kwargs)

    if RECREATE_INDEX:
        if es_client.indices.exists(index=ES_INDEX):
            es_client.indices.delete(index=ES_INDEX)
        es_client.indices.create(index=ES_INDEX, mappings={
            'properties': {
                'content': {'type': 'text'},
                'chunk_embeddings': {
                    'type': 'nested',
                    'properties': {
                        'embedding': {
                            'type': 'dense_vector',
                            'dims': OPENAI_EMBEDDING_SIZE,
                            'index': True,
                            'similarity': 'cosine'
                        }
                    }
                }
            }
        })
    
    openai_client = OpenAI(api_key=openai_api_key)

    for file in DATA_SOURCES_PATH.glob('*.md'):
        with open(file, 'r') as f:
            content = f.read()
            # iterate on chunks
            embeddings = []
            for i in range(0, len(content), CHUNK_SIZE - CHUNK_OVERLAP):
                chunk = content[i:i+CHUNK_SIZE]
                response = openai_client.embeddings.create(
                    input=chunk,
                    model=OPENAI_TEXT_EMBEDDING_MODEL,
                )
                embedding = response.data[0].embedding
                embeddings.append(embedding)

            # print(embeddings)
            id = file.stem
            body = {
                'content': content,
                'chunk_embeddings': [
                    {'embedding': e} for e in embeddings
                ]
            }
            ret = es_client.update(index=ES_INDEX, id=id, doc=body, doc_as_upsert=True)
            print('Indexed:', file.stem)
            
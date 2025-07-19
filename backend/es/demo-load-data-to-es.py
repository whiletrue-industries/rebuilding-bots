import os
import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

from elasticsearch import Elasticsearch
from openai import OpenAI

ES_INDEX = 'test'
OPENAI_EMBEDDING_SIZE = 1536
OPENAI_TEXT_EMBEDDING_MODEL = 'text-embedding-3-small'

DATA_SOURCES_PATH = Path(__file__).parent.parent.parent / 'specs' / 'takanon' / 'extraction'
CHUNK_SIZE = 256
CHUNK_OVERLAP = 64
RECREATE_INDEX = True

def get_es_connection_params(environment='staging'):
    """Get Elasticsearch connection parameters based on environment"""
    if environment == 'production':
        es_host = os.getenv('ES_HOST_PRODUCTION')
        es_username = os.getenv('ES_USERNAME_PRODUCTION', 'elastic')
        es_password = os.getenv('ES_PASSWORD_PRODUCTION') or os.getenv('ELASTIC_PASSWORD_PRODUCTION')
        openai_api_key = os.getenv('OPENAI_API_KEY_PRODUCTION')
    else:  # staging
        es_host = os.getenv('ES_HOST_STAGING')
        es_username = os.getenv('ES_USERNAME_STAGING', 'elastic')
        es_password = os.getenv('ES_PASSWORD_STAGING') or os.getenv('ELASTIC_PASSWORD_STAGING')
        openai_api_key = os.getenv('OPENAI_API_KEY_STAGING')
    
    # Check if required variables are set
    missing_vars = []
    if not es_host:
        missing_vars.append(f'ES_HOST_{environment.upper()}')
    if not es_password:
        missing_vars.append(f'ES_PASSWORD_{environment.upper()} or ELASTIC_PASSWORD_{environment.upper()}')
    if not openai_api_key:
        missing_vars.append(f'OPENAI_API_KEY_{environment.upper()}')
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables for {environment} environment: {', '.join(missing_vars)}")
    
    return es_host, es_username, es_password, openai_api_key

if __name__ == '__main__':
    # Parse command line arguments
    environment = 'staging'  # default
    if len(sys.argv) > 1:
        if sys.argv[1] in ['production', 'staging']:
            environment = sys.argv[1]
        else:
            print("Usage: python demo-load-data-to-es.py [production|staging]")
            print("Default: staging")
            sys.exit(1)
    
    print(f"Using {environment} environment...")
    
    # Get connection parameters
    es_host, es_username, es_password, openai_api_key = get_es_connection_params(environment)
    
    # For local development, use the cert file if it exists
    ca_certs = './certs/ca/ca.crt' if os.path.exists('./certs/ca/ca.crt') else None
    
    es_client = Elasticsearch(
        es_host,
        basic_auth=(es_username, es_password),
        ca_certs=ca_certs,
        request_timeout=30,
        verify_certs=ca_certs is not None
    )

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
            
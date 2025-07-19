import sys
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

from elasticsearch import Elasticsearch
from openai import OpenAI

ES_INDEX = 'test'
OPENAI_EMBEDDING_SIZE = 1536
OPENAI_TEXT_EMBEDDING_MODEL = 'text-embedding-3-small'
NUM_RESULTS = 7

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
    if len(sys.argv) < 2:
        print("Usage: python demo-query-es.py <query> [production|staging]")
        print("Default environment: staging")
        sys.exit(1)
    
    query = sys.argv[1]
    environment = 'staging'  # default
    if len(sys.argv) > 2:
        if sys.argv[2] in ['production', 'staging']:
            environment = sys.argv[2]
        else:
            print("Usage: python demo-query-es.py <query> [production|staging]")
            print("Default environment: staging")
            sys.exit(1)
    
    print(f"Using {environment} environment...")
    
    # Get connection parameters
    es_host, es_username, es_password, openai_api_key = get_es_connection_params(environment)
    
    # For local development, use the cert file if it exists
    ca_certs = './certs/ca/ca.crt' if os.path.exists('./certs/ca/ca.crt') else None
    
    es_kwargs = {
        'hosts': [es_host],
        'basic_auth': (es_username, es_password),
        'request_timeout': 30,
        'verify_certs': ca_certs is not None
    }
    if ca_certs:
        es_kwargs['ca_certs'] = ca_certs
    
    es_client = Elasticsearch(**es_kwargs)

    openai_client = OpenAI(api_key=openai_api_key)

    response = openai_client.embeddings.create(
        input=query,
        model=OPENAI_TEXT_EMBEDDING_MODEL,
    )
    embedding = response.data[0].embedding

    text_match=dict(
        multi_match=dict(
            query=query,
            fields=["content"],
            boost=0.2,
            type='cross_fields',
            operator='or',
        ),
    )
    chunks=dict(
        knn=dict(
            field="chunk_embeddings.embedding",
            query_vector=embedding,
            k=10,
            num_candidates=50,
            boost=0.5
        )
    )
    query=dict(
        bool=dict(
            should=[text_match, chunks],
            minimum_should_match=1,
        ),
    )

    results = es_client.search(index=ES_INDEX, query=query, size=NUM_RESULTS)
    for result in results['hits']['hits']:
        print('{:5.2f}: {:30s}   [{}]'.format(result['_score'], result['_id'], result['_source']['content'].strip().split('\n')[0]))
    # print(results)

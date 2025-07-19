import sys
import os
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
NUM_RESULTS = 7

def get_es_connection_params(environment):
    """Get Elasticsearch connection parameters using centralized config"""
    es_config = ElasticsearchConfig.from_environment(environment)
    
    # Get OpenAI API key
    if environment == 'production':
        openai_api_key = os.getenv('OPENAI_API_KEY_PRODUCTION')
    else:  # staging or local
        openai_api_key = os.getenv('OPENAI_API_KEY_STAGING')
    
    if not openai_api_key:
        raise ValueError(f"Missing OPENAI_API_KEY_{environment.upper()}")
    
    return es_config, openai_api_key

if __name__ == '__main__':
    # Parse command line arguments - both query and environment are required
    if len(sys.argv) < 3:
        print("Usage: python demo-query-es.py <query> <environment>")
        print("Environment must be one of: production, staging, local")
        sys.exit(1)
    
    query = sys.argv[1]
    environment = sys.argv[2]
    if environment not in ['production', 'staging', 'local']:
        print("Usage: python demo-query-es.py <query> <environment>")
        print("Environment must be one of: production, staging, local")
        sys.exit(1)
    
    print(f"Using {environment} environment...")
    
    # Get connection parameters using centralized config
    es_config, openai_api_key = get_es_connection_params(environment)
    
    # Use the centralized configuration to create Elasticsearch client
    es_kwargs = es_config.to_elasticsearch_kwargs()
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

import sys
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

from elasticsearch import Elasticsearch
from openai import OpenAI


ES_INDEX = 'test'
ELASTIC_PASSWORD = os.getenv('ELASTIC_PASSWORD')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_EMBEDDING_SIZE = 1536
OPENAI_TEXT_EMBEDDING_MODEL = 'text-embedding-3-small'
NUM_RESULTS = 7

if __name__ == '__main__':

    es_client = Elasticsearch(
        f'https://localhost:9200/',
        basic_auth=('elastic', ELASTIC_PASSWORD),
        ca_certs='./certs/ca/ca.crt', request_timeout=30
    )

    openai_client = OpenAI(api_key=OPENAI_API_KEY)

    query = sys.argv[1]

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

    results = es_client.search(index=ES_INDEX, query=query, size=NUM_RESULTS, _source=['content'])
    for result in results['hits']['hits']:
        print('{:5.2f}: {:30s}   [{}]'.format(result['_score'], result['_id'], result['_source']['content'].strip().split('\n')[0]))
    # print(results)

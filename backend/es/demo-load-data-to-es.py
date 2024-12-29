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

DATA_SOURCES_PATH = Path(__file__).parent.parent.parent / 'specs' / 'takanon' / 'extraction'
CHUNK_SIZE = 256
CHUNK_OVERLAP = 64
RECREATE_INDEX = True

if __name__ == '__main__':

    es_client = Elasticsearch(
        f'https://localhost:9200/',
        basic_auth=('elastic', ELASTIC_PASSWORD),
        ca_certs='./certs/ca/ca.crt', request_timeout=30
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
    
    openai_client = OpenAI(api_key=OPENAI_API_KEY)

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
            
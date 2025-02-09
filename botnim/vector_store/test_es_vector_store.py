import pytest
import os
from pathlib import Path
from dotenv import load_dotenv
from io import BytesIO

from botnim.vector_store.vector_store_es import VectorStoreES
from botnim.config import get_logger
from botnim.config import DEFAULT_EMBEDDING_MODEL, DEFAULT_EMBEDDING_SIZE

logger = get_logger(__name__)
load_dotenv()

@pytest.fixture
def es_client_config():
    """Common Elasticsearch client configuration for tests"""
    return {
        'es_host': 'https://localhost:9200',
        'es_username': os.getenv('ES_USERNAME'),
        'es_password': os.getenv('ES_PASSWORD'),
        'verify_certs': False
    }

@pytest.fixture
def vector_store(es_client_config):
    """Initialize vector store for testing"""
    config = {"name": "test_assistant"}
    config_dir = Path(".")
    production = False
    
    vs = VectorStoreES(
        config=config,
        config_dir=config_dir,
        production=production,
        es_host=es_client_config['es_host'],
        es_username=es_client_config['es_username'],
        es_password=es_client_config['es_password']
    )
    return vs

@pytest.fixture(autouse=True)
def cleanup(vector_store):
    """Cleanup test indices after each test"""
    yield
    try:
        # Use the same index name format as get_or_create_vector_store
        test_index = f"{vector_store.env_name('test_assistant')}_test_context".lower().replace(' ', '_')
        if vector_store.es_client.indices.exists(index=test_index):
            vector_store.es_client.indices.delete(index=test_index)
            logger.info(f"Cleaned up test index: {test_index}")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

def test_initialization(es_client_config):
    """Test VectorStoreES initialization"""
    vs = VectorStoreES(
        config={"name": "test_assistant"},
        config_dir=Path("."),
        production=False,
        es_host=es_client_config['es_host'],
        es_username=es_client_config['es_username'],
        es_password=es_client_config['es_password']
    )
    
    assert vs.es_client is not None
    assert vs.openai_client is not None
    assert vs.init is False

def test_get_or_create_vector_store(vector_store):
    """Test creating and getting vector store"""
    # Test creation
    context = {}
    result = vector_store.get_or_create_vector_store(context, "test_context", True)
    
    assert result is not None
    assert 'id' in result
    assert 'name' in result
    assert vector_store.es_client.indices.exists(index=result['id'])
    
    # Test getting existing
    result2 = vector_store.get_or_create_vector_store(context, "test_context", False)
    assert result2['id'] == result['id']

def test_upload_files(vector_store):
    """Test uploading files to vector store"""
    # Create vector store
    vs_info = vector_store.get_or_create_vector_store({}, "test_context", True)
    
    # Prepare test documents
    test_docs = [
        ("doc1.txt", "This is test document 1", "text/plain"),
        ("doc2.txt", "This is test document 2", "text/plain")
    ]
    
    docs_to_upload = [
        (filename, BytesIO(content.encode('utf-8')), content_type)
        for filename, content, content_type in test_docs
    ]
    
    # Upload files
    vector_store.upload_files({}, "test_context", vs_info, docs_to_upload, None)
    
    # Force refresh index
    vector_store.es_client.indices.refresh(index=vs_info['id'])
    
    # Verify documents were uploaded
    for filename, content, _ in test_docs:
        doc = vector_store.es_client.get(index=vs_info['id'], id=filename)
        assert doc['_source']['content'] == content
        assert len(doc['_source']['vector']) == DEFAULT_EMBEDDING_SIZE

def test_delete_existing_files(vector_store):
    """Test deleting files from vector store"""
    # Create and populate vector store
    vs_info = vector_store.get_or_create_vector_store({}, "test_context", True)
    
    test_docs = [
        ("doc1.txt", "Test document 1", "text/plain"),
        ("doc2.txt", "Test document 2", "text/plain")
    ]
    
    docs_to_upload = [
        (filename, BytesIO(content.encode('utf-8')), content_type)
        for filename, content, content_type in test_docs
    ]
    
    vector_store.upload_files({}, "test_context", vs_info, docs_to_upload, None)
    vector_store.es_client.indices.refresh(index=vs_info['id'])
    
    # Delete one document
    deleted_count = vector_store.delete_existing_files(
        {}, vs_info, ["doc1.txt"]
    )
    
    vector_store.es_client.indices.refresh(index=vs_info['id'])
    
    assert deleted_count == 1
    with pytest.raises(Exception):
        vector_store.es_client.get(index=vs_info['id'], id="doc1.txt")

def test_update_tools(vector_store):
    """Test updating tools"""
    context = {"max_num_results": 10}
    vs_info = vector_store.get_or_create_vector_store(context, "test_context", True)
    
    vector_store.update_tools(context, vs_info)
    
    assert len(vector_store.tools) == 1
    tool = vector_store.tools[0]
    assert tool['type'] == 'function'
    assert tool['function']['name'] == 'search_common_knowledge'
    assert 'query' in tool['function']['parameters']['properties']
    assert tool['function']['parameters']['required'] == ['query']

def test_update_tool_resources(vector_store):
    """Test updating tool resources"""
    context = {}
    vs_info = vector_store.get_or_create_vector_store(context, "test_context", True)
    
    vector_store.update_tool_resources(context, vs_info)
    
    # For ES implementation, tool_resources should be None
    assert vector_store.tool_resources is None

def test_semantic_search(vector_store):
    """Test semantic search functionality"""
    # First create and populate vector store
    vs_info = vector_store.get_or_create_vector_store({}, "test_context", True)
    
    # Upload some test documents
    test_docs = [
        ("doc1.txt", "Python is a high-level programming language", "text/plain"),
        ("doc2.txt", "JavaScript runs in web browsers", "text/plain"),
        ("doc3.txt", "Docker helps with containerization", "text/plain")
    ]
    
def test_semantic_search(vector_store):
    """Test semantic search functionality"""
    # First create and populate vector store
    vs_info = vector_store.get_or_create_vector_store({}, "test_context", True)
    
    # Upload some test documents
    test_docs = [
        ("doc1.txt", "Python is a high-level programming language", "text/plain"),
        ("doc2.txt", "JavaScript runs in web browsers", "text/plain"),
        ("doc3.txt", "Docker helps with containerization", "text/plain")
    ]
    
    docs_to_upload = [
        (filename, BytesIO(content.encode('utf-8')), content_type)
        for filename, content, content_type in test_docs
    ]
    
    vector_store.upload_files({}, "test_context", vs_info, docs_to_upload, None)
    vector_store.es_client.indices.refresh(index=vs_info['id'])
    
    # Test search
    query = "What programming languages are mentioned?"
    response = vector_store.openai_client.embeddings.create(
        input=query,
        model=DEFAULT_EMBEDDING_MODEL
    )
    query_vector = response.data[0].embedding
    
    search_body = {
        "query": {
            "script_score": {
                "query": {"match_all": {}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'vector') + 1.0",
                    "params": {"query_vector": query_vector}
                }
            }
        }
    }
    
    search_body['size'] = 2  # Include size in the body
    results = vector_store.es_client.search(
        index=vs_info['id'],
        body=search_body
    )
    
    # Verify results
    assert len(results['hits']['hits']) > 0
    # Python document should be in top results
    assert any("Python" in hit['_source']['content'] for hit in results['hits']['hits'])

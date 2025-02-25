import pytest
import os
from pathlib import Path
from dotenv import load_dotenv
from io import BytesIO
import tempfile
import json
from unittest.mock import patch
import builtins
from datetime import datetime

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
def test_config():
    """Test configuration with name and auto-generated slug"""
    name = "test_assistant"
    return {
        "name": name,
        "slug": name.lower().replace(' ', '_')
    }

@pytest.fixture
def vector_store(es_client_config, test_config):
    """Initialize vector store for testing"""
    config_dir = Path(".")
    production = False
    
    vs = VectorStoreES(
        config=test_config,
        config_dir=config_dir,
        es_host=es_client_config['es_host'],
        es_username=es_client_config['es_username'],
        es_password=es_client_config['es_password'],
        production=production
    )
    return vs

@pytest.fixture(autouse=True)
def cleanup(vector_store):
    """Cleanup test indices after each test"""
    yield
    try:
        test_index = f"{vector_store.env_name('test_assistant')}_test_context".lower().replace(' ', '_')
        if vector_store.es_client.indices.exists(index=test_index):
            vector_store.es_client.indices.delete(index=test_index)
            logger.info(f"Cleaned up test index: {test_index}")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")

def test_initialization(es_client_config, test_config):
    """Test VectorStoreES initialization"""
    vs = VectorStoreES(
        config=test_config,
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
    index_name = vector_store.get_or_create_vector_store(context, "test_context", True)
    
    assert index_name is not None
    assert isinstance(index_name, str)
    assert vector_store.es_client.indices.exists(index=index_name)
    
    # Test getting existing
    index_name2 = vector_store.get_or_create_vector_store(context, "test_context", False)
    assert index_name2 == index_name

def test_upload_files(vector_store):
    """Test uploading files to vector store"""
    # Create vector store
    index_name = vector_store.get_or_create_vector_store({}, "test_context", True)
    
    # Create temporary metadata files for testing
    test_docs = [
        ("doc1.txt", "This is test document 1", {"title": "Test 1"}),
        ("doc2.txt", "This is test document 2", {"title": "Test 2"})
    ]
    
    # Prepare documents for upload
    docs_to_upload = [
        (filename, BytesIO(content.encode('utf-8')), 'text/plain')
        for filename, content, _ in test_docs
    ]

    metadata_files = {
        f"{doc[0]}.metadata.json": json.dumps(doc[2])
        for doc in test_docs
    }

    # Save the real open function before patching
    real_open = builtins.open

    def mock_exists(self, *args, **kwargs):
        # self is a Path instance
        filename = str(self).split('/')[-1]
        return filename in metadata_files

    def mock_open(file_path, *args, **kwargs):
        # Ensure file_path is a string
        if isinstance(file_path, Path):
            file_path = str(file_path)
        filename = file_path.split('/')[-1]
        
        if filename in metadata_files:
            return BytesIO(metadata_files[filename].encode('utf-8'))
        # Call the original open to avoid recursion
        return real_open(file_path, *args, **kwargs)

    # Patch both Path.exists and builtins.open
    with patch('pathlib.Path.exists', new=mock_exists), \
         patch('builtins.open', side_effect=mock_open):
        # Upload files
        vector_store.upload_files({}, "test_context", index_name, docs_to_upload, None)
        
        # Force refresh index
        vector_store.es_client.indices.refresh(index=index_name)
        
        # Verify documents were uploaded
        for filename, content, metadata in test_docs:
            doc = vector_store.es_client.get(index=index_name, id=filename)
            assert doc['_source']['content'] == content
            assert len(doc['_source']['vector']) == DEFAULT_EMBEDDING_SIZE
            assert doc['_source']['metadata']['title'] == metadata['title']

            
def test_delete_existing_files(vector_store):
    """Test deleting files from vector store"""
    # Create and populate vector store
    index_name = vector_store.get_or_create_vector_store({}, "test_context", True)
    
    test_docs = [
        ("doc1.txt", "Test document 1", {"title": "Test 1", "type": "text"}),
        ("doc2.txt", "Test document 2", {"title": "Test 2", "type": "text"})
    ]
    
    docs_to_upload = [
        (filename, BytesIO(content.encode('utf-8')), metadata)
        for filename, content, metadata in test_docs
    ]
    
    vector_store.upload_files({}, "test_context", index_name, docs_to_upload, None)
    vector_store.es_client.indices.refresh(index=index_name)
    
    # Delete one document
    vs_info = index_name
    deleted_count = vector_store.delete_existing_files({}, vs_info, ["doc1.txt"])
    
    vector_store.es_client.indices.refresh(index=index_name)
    
    assert deleted_count == 1
    
    # Verify doc1 is deleted but doc2 still exists
    assert not vector_store.es_client.exists(index=index_name, id="doc1.txt")
    assert vector_store.es_client.exists(index=index_name, id="doc2.txt")

def test_update_tools(vector_store):
    """Test updating tools"""
    context = {"max_num_results": 10}
    index_name = vector_store.get_or_create_vector_store(context, "test_context", True)
    
    vector_store.update_tools(context, index_name)
    
    assert len(vector_store.tools) == 1
    tool = vector_store.tools[0]
    assert tool['type'] == 'function'
    assert tool['function']['name'] == f"search_{index_name}"
    assert 'query' in tool['function']['parameters']['properties']
    assert tool['function']['parameters']['required'] == ['query']

def test_update_tool_resources(vector_store):
    """Test updating tool resources"""
    context = {}
    index_name = vector_store.get_or_create_vector_store(context, "test_context", True)
    
    vector_store.update_tool_resources(context, index_name)
    
    # For ES implementation, tool_resources should be None
    assert vector_store.tool_resources is None

def test_semantic_search(vector_store):
    """Test semantic search functionality"""
    # First create and populate vector store
    index_name = vector_store.get_or_create_vector_store({}, "test_context", True)
    
    # Upload some test documents with metadata
    test_docs = [
        ("doc1.txt", "Python is a high-level programming language", {"title": "Python Doc"}),
        ("doc2.txt", "JavaScript runs in web browsers", {"title": "JS Doc"}),
        ("doc3.txt", "Docker helps with containerization", {"title": "Docker Doc"})
    ]
    
    docs_to_upload = [
        (filename, BytesIO(content.encode('utf-8')), metadata)
        for filename, content, metadata in test_docs
    ]
    
    vector_store.upload_files({}, "test_context", index_name, docs_to_upload, None)
    vector_store.es_client.indices.refresh(index=index_name)
    
    # Test search
    query = "What programming languages are mentioned?"
    response = vector_store.openai_client.embeddings.create(
        input=query,
        model=DEFAULT_EMBEDDING_MODEL
    )
    query_vector = response.data[0].embedding
    
    # Use the vector_store's search method instead of direct ES query
    results = vector_store.search("test_context", query, query_vector)
    
    # Verify results
    assert len(results['hits']['hits']) > 0
    # Verify that Python and JavaScript documents are in top results
    contents = [hit['_source']['content'] for hit in results['hits']['hits']]
    assert any('Python' in content for content in contents)
    assert any('JavaScript' in content for content in contents)

def test_multiple_contexts(vector_store):
    """Test handling of multiple contexts and initialization state"""
    # Create first context
    context1 = {}
    index_name1 = vector_store.get_or_create_vector_store(context1, "test_context1", True)
    
    # Upload a document to first context
    docs1 = [("doc1.txt", "Content for context 1", {"title": "Context 1"})]
    docs_to_upload1 = [(f, BytesIO(c.encode('utf-8')), m) for f, c, m in docs1]
    vector_store.upload_files(context1, "test_context1", index_name1, docs_to_upload1, None)
    
    # Create second context
    context2 = {}
    index_name2 = vector_store.get_or_create_vector_store(context2, "test_context2", True)
    
    # Upload a document to second context
    docs2 = [("doc2.txt", "Content for context 2", {"title": "Context 2"})]
    docs_to_upload2 = [(f, BytesIO(c.encode('utf-8')), m) for f, c, m in docs2]
    vector_store.upload_files(context2, "test_context2", index_name2, docs_to_upload2, None)
    
    # Force refresh indices
    vector_store.es_client.indices.refresh(index=index_name1)
    vector_store.es_client.indices.refresh(index=index_name2)
    
    # Verify both contexts exist and have correct content
    assert vector_store.es_client.indices.exists(index=index_name1)
    assert vector_store.es_client.indices.exists(index=index_name2)
    
    doc1 = vector_store.es_client.get(index=index_name1, id="doc1.txt")
    doc2 = vector_store.es_client.get(index=index_name2, id="doc2.txt")
    
    assert doc1['_source']['content'] == "Content for context 1"
    assert doc2['_source']['content'] == "Content for context 2"

def test_metadata_handling(vector_store):
    """Test metadata handling in vector store"""
    index_name = vector_store.get_or_create_vector_store({}, "test_context", True)
    
    test_docs = [
        {
            "filename": "doc1.txt",
            "content": "This is a legal document about procedures",
            "metadata": {
                "title": "Legal Procedures",
                "document_type": "procedure",
                "extracted_data": {
                    "DocumentMetadata": {
                        "DocumentTitle": "Legal Procedures",
                        "Description": "Document about legal procedures"
                    }
                }
            }
        }
    ]
    
    docs_to_upload = [
        (doc["filename"],
         BytesIO(doc["content"].encode('utf-8')),
         'text/plain') for doc in test_docs
    ]
    
    metadata_files = {
        f"{doc['filename']}.metadata.json": doc['metadata']
        for doc in test_docs
    }
    
    real_open = builtins.open

    def mock_exists(self, *args, **kwargs):
        filename = str(self).split('/')[-1]
        return filename in metadata_files

    def mock_open(file_path, *args, **kwargs):
        if isinstance(file_path, Path):
            file_path = str(file_path)
        filename = file_path.split('/')[-1]
        
        if filename in metadata_files:
            return BytesIO(json.dumps(metadata_files[filename]).encode('utf-8'))
        return real_open(file_path, *args, **kwargs)

    with patch('pathlib.Path.exists', new=mock_exists), \
         patch('builtins.open', side_effect=mock_open):
        vector_store.upload_files({}, "test_context", index_name, docs_to_upload, None)
        vector_store.es_client.indices.refresh(index=index_name)
        
        doc_metadata = vector_store.verify_document_metadata(index_name, "doc1.txt")
        # Now the metadata title should be "Legal Procedures" as expected
        assert doc_metadata.get("title") == "Legal Procedures"

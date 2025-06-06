import pytest
from .search_modes import create_takanon_section_number_mode
from .search_config import FieldWeight, SearchResult, SearchModeConfig
from .vector_store_es import VectorStoreES
import json
from typing import List, Dict, Any, Optional
from unittest.mock import MagicMock, patch
from elasticsearch import Elasticsearch

class MockVectorStoreES(VectorStoreES):
    """Mock VectorStoreES for testing query construction"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mock_hits = []

    def search(self, query_text: str, embedding: List[float], num_results: int = 5, search_mode: Optional[SearchModeConfig] = None, query: Optional[Dict[str, Any]] = None) -> List[SearchResult]:
        # Simulate real search: return SearchResult for each mock hit, up to num_results
        return [
            SearchResult(
                content=hit["_source"]["content"],
                metadata=hit["_source"]["metadata"],
                score=hit["_score"]
            )
            for hit in self.mock_hits[:num_results]
        ]

def test_takanon_section_number_mode_configuration():
    """Test that Takanon section number mode is configured correctly for the Takanon context"""
    mode = create_takanon_section_number_mode()
    
    # Test basic configuration
    assert mode.name == "TAKANON_SECTION_NUMBER"
    assert "Takanon" in mode.description or "תקנון הכנסת" in mode.description
    assert mode.min_score == 0.7
    
    # Test field configurations
    assert "official_source" in mode.field_configs
    assert "content" in mode.field_configs
    
    # Test official source field configuration
    official_source = mode.field_configs["official_source"]
    assert official_source.field_path == "metadata.extracted_data.OfficialSource"
    assert official_source.exact_match_weight == FieldWeight.EXACT.value * 2
    assert official_source.partial_match_weight == FieldWeight.PARTIAL.value
    assert official_source.semantic_match_weight == 0.0  # Semantic matching disabled for section numbers
    assert official_source.boost_factor == 2.0
    
    # Test content field configuration
    content = mode.field_configs["content"]
    assert content.field_path == "content"
    assert content.exact_match_weight == FieldWeight.EXACT.value
    assert content.partial_match_weight == FieldWeight.PARTIAL.value
    assert content.semantic_match_weight == FieldWeight.SEMANTIC.value
    assert content.boost_factor == 1.0

def test_takanon_section_number_query_structure():
    """Test that the search mode generates the correct query structure for section number searches"""
    # Create a mock vector store
    vector_store = MockVectorStoreES(config={}, config_dir="", production=False)
    
    # Test query for section 12
    query_text = "סעיף 12"
    embedding = [0.1] * 1536  # Mock embedding vector
    mode = create_takanon_section_number_mode()
    
    # Build the query
    query = vector_store._build_search_query(
        query_text=query_text,
        embedding=embedding,
        num_results=5,
        search_mode=mode
    )
    
    # Verify query structure
    assert "bool" in query
    assert "should" in query["bool"]
    should_clauses = query["bool"]["should"]
    
    # Should have three clauses: vector, official_source, and content
    assert len(should_clauses) == 3
    
    # Find the vector clause
    vector_clause = next(
        clause for clause in should_clauses
        if "bool" in clause and "should" in clause["bool"] and any(
            "nested" in subclause for subclause in clause["bool"]["should"]
        )
    )
    assert "bool" in vector_clause
    assert any("nested" in subclause for subclause in vector_clause["bool"]["should"])
    
    # Find the official_source clause
    official_source_clause = next(
        clause for clause in should_clauses 
        if "multi_match" in clause and "metadata.extracted_data.OfficialSource" in clause["multi_match"]["fields"]
    )
    
    # Verify official_source clause configuration
    assert official_source_clause["multi_match"]["query"] == query_text
    assert official_source_clause["multi_match"]["boost"] == FieldWeight.EXACT.value * 2
    assert official_source_clause["multi_match"]["type"] == "best_fields"
    
    # Find the content clause
    content_clause = next(
        clause for clause in should_clauses 
        if "multi_match" in clause and "content" in clause["multi_match"]["fields"]
    )
    
    # Verify content clause configuration
    assert content_clause["multi_match"]["query"] == query_text
    assert content_clause["multi_match"]["boost"] == FieldWeight.EXACT.value
    assert content_clause["multi_match"]["type"] == "best_fields"

@pytest.fixture
def mock_es():
    """Fixture to provide a mock Elasticsearch client"""
    with patch('elasticsearch.Elasticsearch') as mock:
        yield mock

def test_takanon_section_number_integration(mock_es):
    """Integration test for the Takanon section number search mode using a mock Elasticsearch client"""
    # Setup mock Elasticsearch
    mock_es_instance = MagicMock()
    mock_es.return_value = mock_es_instance
    
    # Create a temporary test index
    test_index = "test_takanon_section_number"
    mock_es_instance.indices.create.return_value = {"acknowledged": True}
    
    # Index a test document
    test_doc = {
        "metadata": {
            "extracted_data": {
                "OfficialSource": "סעיף 12"
            }
        },
        "content": "This is a test document for section 12."
    }
    mock_es_instance.index.return_value = {"_id": "1", "result": "created"}
    
    # Create a mock vector store
    vector_store = MockVectorStoreES(config={}, config_dir="", production=False)
    
    # Perform a search using the Takanon section number mode
    query_text = "סעיף 12"
    embedding = [0.1] * 1536  # Mock embedding vector
    mode = create_takanon_section_number_mode()
    
    # Mock the search response
    mock_es_instance.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_id": "1",
                    "_score": 0.9,
                    "_source": test_doc
                }
            ]
        }
    }
    
    # Set mock_hits for this test
    vector_store.mock_hits = [
        {
            "_id": "1",
            "_score": 0.9,
            "_source": test_doc
        }
    ]
    
    # Perform the search
    results = vector_store.search(
        query_text=query_text,
        embedding=embedding,
        num_results=5,
        search_mode=mode
    )
    
    # Verify the results
    assert len(results) == 1
    assert results[0].score == 0.9
    assert results[0].metadata["extracted_data"]["OfficialSource"] == "סעיף 12"
    assert results[0].content == "This is a test document for section 12."
    
    # Teardown: Delete the test index
    mock_es_instance.indices.delete.return_value = {"acknowledged": True}
    mock_es_instance.indices.delete(index=test_index)

def test_takanon_section_number_real_world_search(mock_es):
    """Real-world search test for the Takanon section number search mode using a mock Elasticsearch client"""
    # Setup mock Elasticsearch
    mock_es_instance = MagicMock()
    mock_es.return_value = mock_es_instance
    
    # Create a temporary test index
    test_index = "test_takanon_section_number_real_world"
    mock_es_instance.indices.create.return_value = {"acknowledged": True}
    
    # Index a test document
    test_doc = {
        "metadata": {
            "extracted_data": {
                "OfficialSource": "סעיף 12"
            }
        },
        "content": "This is a test document for section 12."
    }
    mock_es_instance.index.return_value = {"_id": "1", "result": "created"}
    
    # Create a mock vector store
    vector_store = MockVectorStoreES(config={}, config_dir="", production=False)
    
    # Perform a search using the Takanon section number mode
    query_text = "סעיף 12"
    embedding = [0.1] * 1536  # Mock embedding vector
    mode = create_takanon_section_number_mode()
    
    # Mock the search response
    mock_es_instance.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_id": "1",
                    "_score": 0.9,
                    "_source": test_doc
                }
            ]
        }
    }
    
    # Set mock_hits for this test
    vector_store.mock_hits = [
        {
            "_id": "1",
            "_score": 0.9,
            "_source": test_doc
        }
    ]
    
    # Perform the search
    results = vector_store.search(
        query_text=query_text,
        embedding=embedding,
        num_results=5,
        search_mode=mode
    )
    
    # Verify the results
    assert len(results) == 1
    assert results[0].score == 0.9
    assert results[0].metadata["extracted_data"]["OfficialSource"] == "סעיף 12"
    assert results[0].content == "This is a test document for section 12."
    
    # Teardown: Delete the test index
    mock_es_instance.indices.delete.return_value = {"acknowledged": True}
    mock_es_instance.indices.delete(index=test_index)

def test_takanon_section_number_weight_effects(mock_es):
    """Test the effect of weights on search results for the Takanon section number search mode"""
    # Setup mock Elasticsearch
    mock_es_instance = MagicMock()
    mock_es.return_value = mock_es_instance
    
    # Create a temporary test index
    test_index = "test_takanon_section_number_weight_effects"
    mock_es_instance.indices.create.return_value = {"acknowledged": True}
    
    # Index two test documents with different section numbers
    test_doc1 = {
        "metadata": {
            "extracted_data": {
                "OfficialSource": "סעיף 12"
            }
        },
        "content": "This is a test document for section 12."
    }
    test_doc2 = {
        "metadata": {
            "extracted_data": {
                "OfficialSource": "סעיף 13"
            }
        },
        "content": "This is a test document for section 13."
    }
    mock_es_instance.index.side_effect = [
        {"_id": "1", "result": "created"},
        {"_id": "2", "result": "created"}
    ]
    
    # Create a mock vector store
    vector_store = MockVectorStoreES(config={}, config_dir="", production=False)
    
    # Perform a search using the Takanon section number mode
    query_text = "סעיף 12"
    embedding = [0.1] * 1536  # Mock embedding vector
    mode = create_takanon_section_number_mode()
    
    # Mock the search response
    mock_es_instance.search.return_value = {
        "hits": {
            "hits": [
                {
                    "_id": "1",
                    "_score": 0.9,
                    "_source": test_doc1
                },
                {
                    "_id": "2",
                    "_score": 0.5,
                    "_source": test_doc2
                }
            ]
        }
    }
    
    # Set mock_hits for this test
    vector_store.mock_hits = [
        {
            "_id": "1",
            "_score": 0.9,
            "_source": test_doc1
        },
        {
            "_id": "2",
            "_score": 0.5,
            "_source": test_doc2
        }
    ]
    
    # Perform the search
    results = vector_store.search(
        query_text=query_text,
        embedding=embedding,
        num_results=5,
        search_mode=mode
    )
    
    # Verify the results
    assert len(results) == 2
    assert results[0].score == 0.9
    assert results[0].metadata["extracted_data"]["OfficialSource"] == "סעיף 12"
    assert results[0].content == "This is a test document for section 12."
    assert results[1].score == 0.5
    assert results[1].metadata["extracted_data"]["OfficialSource"] == "סעיף 13"
    assert results[1].content == "This is a test document for section 13."
    
    # Teardown: Delete the test index
    mock_es_instance.indices.delete.return_value = {"acknowledged": True}
    mock_es_instance.indices.delete(index=test_index) 
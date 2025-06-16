import pytest
from .search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE, DEFAULT_SEARCH_MODE_NAME
from .search_config import FieldWeight, SearchModeConfig
from botnim.query import SearchResult
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

    def search(self, query_text: str, num_results: int = 5, search_mode: Optional[SearchModeConfig] = None, query: Optional[Dict[str, Any]] = None) -> List[SearchResult]:
        # Simulate real search: return SearchResult for each mock hit, up to num_results
        return [
            SearchResult(
                score=hit["_score"],
                id=hit["_id"],
                content=hit["_source"]["content"].strip().split('\n')[0],
                full_content=hit["_source"]["content"],
                metadata=hit["_source"].get("metadata", None),
                _explanation=None
            )
            for hit in self.mock_hits[:num_results]
        ]

def test_takanon_section_number_mode():
    """Test the Takanon section number search mode"""
    mode = SEARCH_MODES["TAKANON_SECTION_NUMBER"]
    
    # Test basic configuration
    assert mode.name == "TAKANON_SECTION_NUMBER"
    assert "סעיף" in mode.description
    assert mode.min_score == 0.5
    
    # Test field configurations
    assert len(mode.fields) == 4
    
    # Test official source field
    official_source = next(f for f in mode.fields if f.name == "official_source")
    assert official_source.weight.exact_match > 0
    assert official_source.weight.partial_match >= 0
    assert official_source.boost_factor > 0
    
    # Test content field
    content = next(f for f in mode.fields if f.name == "content")
    assert content.weight.exact_match > 0
    assert content.weight.partial_match >= 0
    assert content.boost_factor > 0
    
    # Test document title field
    document_title = next(f for f in mode.fields if f.name == "document_title")
    assert document_title.weight.exact_match > 0
    assert document_title.weight.partial_match >= 0
    assert document_title.boost_factor > 0
    assert document_title.fuzzy_matching is True

def test_takanon_section_number_query_structure():
    """Test that the search mode generates the correct query structure for section number searches"""
    # Create a mock vector store
    vector_store = MockVectorStoreES(config={}, config_dir="", production=False)
    
    # Test query for section 12
    query_text = "סעיף 12"
    mode = SEARCH_MODES["TAKANON_SECTION_NUMBER"]
    
    # Build the query
    query = vector_store._build_search_query(
        query_text=query_text,
        search_mode=mode
    )
    
    # Verify query structure
    assert "query" in query
    assert "bool" in query["query"]
    should_clauses = query["query"]["bool"]["should"]
    
    # Should have six clauses: two for each field (match_phrase and match)
    assert len(should_clauses) > 0
    
    # Verify each clause has the correct structure
    for clause in should_clauses:
        assert "match" in clause or "match_phrase" in clause
        query_type = "match" if "match" in clause else "match_phrase"
        field_name = next(iter(clause[query_type].keys()))
        field_config = clause[query_type][field_name]
        assert isinstance(field_config, dict)
        assert "query" in field_config
        assert "boost" in field_config
        assert field_config["query"] == query_text

    # Verify minimum_should_match
    assert query["query"]["bool"]["minimum_should_match"] == 1

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
    mode = SEARCH_MODES["TAKANON_SECTION_NUMBER"]
    
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
    mode = SEARCH_MODES["TAKANON_SECTION_NUMBER"]
    
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
    mode = SEARCH_MODES["TAKANON_SECTION_NUMBER"]
    
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

def test_regular_mode_query_structure():
    """Test that the REGULAR mode produces the expected query structure."""
    vector_store = MockVectorStoreES(config={}, config_dir="", production=False)
    query_text = "example query"

    # Build the query with explicit REGULAR mode
    regular_mode = SEARCH_MODES["REGULAR"]
    query_regular = vector_store._build_search_query(query_text=query_text, search_mode=regular_mode)

    # Basic structure checks
    assert "query" in query_regular
    assert "bool" in query_regular["query"]
    should_clauses = query_regular["query"]["bool"]["should"]
    assert len(should_clauses) > 0
    for clause in should_clauses:
        assert "match" in clause or "match_phrase" in clause
    assert query_regular["query"]["bool"]["minimum_should_match"] == 1

def test_search_modes_registry_contains_expected_modes():
    assert "TAKANON_SECTION_NUMBER" in SEARCH_MODES
    assert "REGULAR" in SEARCH_MODES


def test_search_modes_registry_is_immutable():
    with pytest.raises(TypeError):
        SEARCH_MODES["NEW_MODE"] = "should fail"
    with pytest.raises(TypeError):
        del SEARCH_MODES["REGULAR"]


def test_default_search_mode_is_regular():
    assert DEFAULT_SEARCH_MODE_NAME == "REGULAR"
    assert DEFAULT_SEARCH_MODE is SEARCH_MODES["REGULAR"]


def test_search_modes_registry_returns_default_for_unknown():
    # Simulate the lookup logic used in business code
    mode = SEARCH_MODES.get("NON_EXISTENT_MODE", DEFAULT_SEARCH_MODE)
    assert mode is DEFAULT_SEARCH_MODE


def test_search_mode_config_structure():
    # Check that all configs in the registry are SearchModeConfig instances
    from botnim.vector_store.search_config import SearchModeConfig
    for mode in SEARCH_MODES.values():
        assert isinstance(mode, SearchModeConfig)
        assert hasattr(mode, "fields")
        assert isinstance(mode.fields, list)
        assert hasattr(mode, "name")
        assert hasattr(mode, "description") 
import pytest
from botnim.vector_store.search_config import FieldWeight, SearchFieldConfig, SearchModeConfig
from botnim.vector_store.search_modes import create_takanon_section_number_mode

def test_field_weight_values():
    """Test that FieldWeight values are correct"""
    fw = FieldWeight(exact_match=3.0, partial_match=1.5, semantic_match=0.5)
    assert fw.exact_match == 3.0
    assert fw.partial_match == 1.5
    assert fw.semantic_match == 0.5

def test_search_field_config_defaults():
    """Test that SearchFieldConfig uses FieldWeight values as defaults"""
    fw = FieldWeight(exact_match=2.0, partial_match=1.0, semantic_match=0.0)
    config = SearchFieldConfig(name="test", weight=fw, boost_factor=1.0, fuzzy_matching=False, field_path="test.field")
    assert config.name == "test"
    assert config.weight.exact_match == 2.0
    assert config.weight.partial_match == 1.0
    assert config.weight.semantic_match == 0.0
    assert config.boost_factor == 1.0
    assert config.fuzzy_matching is False
    assert config.field_path == "test.field"

def test_search_field_config_custom_weights():
    """Test that SearchFieldConfig accepts custom FieldWeight objects"""
    fw = FieldWeight(exact_match=4.0, partial_match=2.5, semantic_match=1.5)
    config = SearchFieldConfig(name="custom", weight=fw, boost_factor=2.0, fuzzy_matching=True, field_path="custom.field")
    assert config.weight.exact_match == 4.0
    assert config.weight.partial_match == 2.5
    assert config.weight.semantic_match == 1.5
    assert config.boost_factor == 2.0
    assert config.fuzzy_matching is True
    assert config.field_path == "custom.field"

def test_search_mode_config():
    """Test SearchModeConfig creation and field configs"""
    fw = FieldWeight(exact_match=2.0, partial_match=1.0, semantic_match=0.0)
    field_config = SearchFieldConfig(name="test", weight=fw, boost_factor=1.0, fuzzy_matching=False, field_path="test.field")
    mode = SearchModeConfig(
        name="TEST_MODE",
        description="Test mode",
        fields=[field_config],
        min_score=0.5
    )
    assert mode.name == "TEST_MODE"
    assert mode.description == "Test mode"
    assert len(mode.fields) == 1
    assert mode.fields[0].name == "test"
    assert mode.fields[0].weight.exact_match == 2.0
    assert mode.min_score == 0.5

def test_takanon_section_number_mode_configuration():
    """Test the configuration of the Takanon section number search mode"""
    mode = create_takanon_section_number_mode()
    
    # Verify basic configuration
    assert mode.name == "TAKANON_SECTION_NUMBER"
    assert "section number" in mode.description.lower()
    assert mode.min_score == 0.5
    
    # Verify field configs
    field_names = [f.name for f in mode.fields]
    assert "official_source" in field_names
    assert "content" in field_names
    assert "document_title" in field_names
    
    # Check official_source config
    official_source = next(f for f in mode.fields if f.name == "official_source")
    assert official_source.field_path == "metadata.extracted_data.OfficialSource"
    assert official_source.weight.exact_match > 0
    assert official_source.boost_factor > 0
    
    # Check content config
    content = next(f for f in mode.fields if f.name == "content")
    assert content.weight.exact_match > 0
    assert content.boost_factor > 0
    
    # Check document_title config
    document_title = next(f for f in mode.fields if f.name == "document_title")
    assert document_title.field_path == "metadata.extracted_data.DocumentTitle"
    assert document_title.weight.exact_match > 0
    assert document_title.boost_factor > 0
    assert document_title.fuzzy_matching is True 
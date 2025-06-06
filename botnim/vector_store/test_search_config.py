import pytest
from .search_config import FieldWeight, SearchFieldConfig, SearchModeConfig
from .search_modes import create_takanon_section_number_mode

def test_field_weight_values():
    """Test that FieldWeight enum values are correct"""
    assert FieldWeight.EXACT.value == 3.0
    assert FieldWeight.PARTIAL.value == 2.0
    assert FieldWeight.SEMANTIC.value == 1.0

def test_search_field_config_defaults():
    """Test that SearchFieldConfig uses FieldWeight values as defaults"""
    config = SearchFieldConfig(field_path="test.field")
    assert config.exact_match_weight == FieldWeight.EXACT.value
    assert config.partial_match_weight == FieldWeight.PARTIAL.value
    assert config.semantic_match_weight == FieldWeight.SEMANTIC.value
    assert config.boost_factor == 1.0

def test_search_field_config_custom_weights():
    """Test that SearchFieldConfig accepts custom weights"""
    config = SearchFieldConfig(
        field_path="test.field",
        exact_match_weight=4.0,
        partial_match_weight=2.5,
        semantic_match_weight=1.5,
        boost_factor=2.0
    )
    assert config.exact_match_weight == 4.0
    assert config.partial_match_weight == 2.5
    assert config.semantic_match_weight == 1.5
    assert config.boost_factor == 2.0

def test_search_mode_config():
    """Test SearchModeConfig creation and field configs"""
    field_config = SearchFieldConfig(field_path="test.field")
    mode_config = SearchModeConfig(
        name="test_mode",
        description="Test search mode",
        field_configs={"test": field_config}
    )
    assert mode_config.name == "test_mode"
    assert mode_config.description == "Test search mode"
    assert mode_config.field_configs["test"] == field_config
    assert mode_config.min_score == 0.5  # default value 

def test_takanon_section_number_mode_configuration():
    """Test the configuration of the Takanon section number search mode"""
    mode = create_takanon_section_number_mode()
    
    # Verify basic configuration
    assert mode.name == "TAKANON_SECTION_NUMBER"
    assert "section number" in mode.description.lower()
    assert mode.min_score == 0.7
    
    # Verify field configurations
    assert "official_source" in mode.field_configs
    assert "content" in mode.field_configs
    
    # Verify official_source field configuration
    official_source_config = mode.field_configs["official_source"]
    assert official_source_config.field_path == "metadata.extracted_data.OfficialSource"
    assert official_source_config.exact_match_weight == FieldWeight.EXACT.value * 2  # Double weight
    assert official_source_config.partial_match_weight == FieldWeight.PARTIAL.value
    assert official_source_config.semantic_match_weight == 0.0  # No semantic matching
    assert official_source_config.boost_factor == 2.0
    
    # Verify content field configuration
    content_config = mode.field_configs["content"]
    assert content_config.field_path == "content"
    assert content_config.exact_match_weight == FieldWeight.EXACT.value
    assert content_config.partial_match_weight == FieldWeight.PARTIAL.value
    assert content_config.semantic_match_weight == FieldWeight.SEMANTIC.value
    assert content_config.boost_factor == 1.0 
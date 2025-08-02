#!/usr/bin/env python3
"""
Unit tests for enhanced field extraction with JSON schema validation.
"""

import json
import pytest
from unittest.mock import Mock, patch

from ..field_extraction import (
    extract_fields_from_text,
    build_extraction_schema,
    validate_extracted_data,
    validate_single_item,
    validate_manually
)
from ..pdf_extraction_config import (
    SourceConfig,
    FieldConfig
)
from ..exceptions import (
    FieldExtractionError,
    ValidationError as PDFValidationError
)

class TestFieldExtraction:
    """Test suite for field extraction functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create a mock source config for testing
        self.mock_config = SourceConfig(
            name="test_source",
            description="Test source for unit testing",
            file_pattern="test/*.pdf",
            unique_id_field="title",
            fields=[
                FieldConfig(
                    name="title",
                    description="Document title",
                    example="Sample Title",
                    hint="Extract the main title"
                ),
                FieldConfig(
                    name="content",
                    description="Document content",
                    example="Sample content",
                    hint="Extract the main content"
                )
            ],
            extraction_instructions="Extract title and content from the document."
        )
        
        # Create a mock OpenAI client
        self.mock_client = Mock()
        
    def test_build_extraction_schema(self):
        """Test building JSON schema from source config."""
        schema = build_extraction_schema(self.mock_config)
        
        # Verify schema structure
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "required" in schema
        assert schema["additionalProperties"] is False
        
        # Verify field properties
        assert "title" in schema["properties"]
        assert "content" in schema["properties"]
        assert schema["properties"]["title"]["type"] == "string"
        assert schema["properties"]["content"]["type"] == "string"
        
        # Verify required fields
        assert "title" in schema["required"]
        assert "content" in schema["required"]
        assert len(schema["required"]) == 2
        
        # Verify examples are included
        assert "examples" in schema["properties"]["title"]
        assert schema["properties"]["title"]["examples"] == ["Sample Title"]
    
    def test_validate_single_item_with_jsonschema(self):
        """Test single item validation with jsonschema library."""
        schema = build_extraction_schema(self.mock_config)
        
        # Test valid item
        valid_item = {
            "title": "Test Document",
            "content": "This is test content"
        }
        
        result = validate_single_item(valid_item, schema, self.mock_config)
        assert result == valid_item
        
        # Test invalid item (missing required field)
        invalid_item = {
            "title": "Test Document"
            # Missing "content" field
        }
        
        with pytest.raises(PDFValidationError) as exc_info:
            validate_single_item(invalid_item, schema, self.mock_config)
        
        assert "validation failed" in str(exc_info.value).lower()
        
        # Test invalid item (unexpected field)
        invalid_item_with_extra = {
            "title": "Test Document",
            "content": "Test content",
            "extra_field": "This should not be here"
        }
        
        with pytest.raises(PDFValidationError) as exc_info:
            validate_single_item(invalid_item_with_extra, schema, self.mock_config)
        
        assert "validation failed" in str(exc_info.value).lower()
    
    def test_validate_single_item_jsonschema_required(self):
        """Test that jsonschema is required for validation."""
        schema = build_extraction_schema(self.mock_config)
        
        # Test that jsonschema import is required
        try:
            import jsonschema
        except ImportError:
            pytest.skip("jsonschema library not available")
        
        # Test valid item
        valid_item = {
            "title": "Test Document",
            "content": "This is test content"
        }
        
        result = validate_single_item(valid_item, schema, self.mock_config)
        assert result == valid_item
        
        # Test invalid item (missing required field)
        invalid_item = {
            "title": "Test Document"
            # Missing "content" field
        }
        
        with pytest.raises(PDFValidationError) as exc_info:
            validate_single_item(invalid_item, schema, self.mock_config)
        
        assert "validation failed" in str(exc_info.value).lower()
    
    def test_validate_extracted_data_single_object(self):
        """Test validation of single object response."""
        schema = build_extraction_schema(self.mock_config)
        
        single_object = {
            "title": "Test Document",
            "content": "Test content"
        }
        
        result = validate_extracted_data(single_object, schema, self.mock_config)
        assert len(result) == 1
        assert result[0] == single_object
    
    def test_validate_extracted_data_array(self):
        """Test validation of array response."""
        schema = build_extraction_schema(self.mock_config)
        
        array_data = [
            {
                "title": "Document 1",
                "content": "Content 1"
            },
            {
                "title": "Document 2",
                "content": "Content 2"
            }
        ]
        
        result = validate_extracted_data(array_data, schema, self.mock_config)
        assert len(result) == 2
        assert result == array_data
    
    def test_validate_extracted_data_invalid_type(self):
        """Test validation with invalid data type."""
        schema = build_extraction_schema(self.mock_config)
        
        # Test with non-dict/list data
        invalid_data = "not a dict or list"
        
        with pytest.raises(PDFValidationError) as exc_info:
            validate_extracted_data(invalid_data, schema, self.mock_config)
        
        assert "invalid data type" in str(exc_info.value).lower()
    
    def test_validate_manually(self):
        """Test manual validation fallback."""
        # Test valid item
        valid_item = {
            "title": "Test Document",
            "content": "Test content"
        }
        
        result = validate_manually(valid_item, self.mock_config)
        assert result == valid_item
        
        # Test missing fields (should warn but not fail)
        missing_fields_item = {
            "title": "Test Document"
            # Missing "content"
        }
        
        result = validate_manually(missing_fields_item, self.mock_config)
        assert result == missing_fields_item
        
        # Test unexpected fields (should warn but not fail)
        unexpected_fields_item = {
            "title": "Test Document",
            "content": "Test content",
            "extra": "Extra field"
        }
        
        result = validate_manually(unexpected_fields_item, self.mock_config)
        assert result == unexpected_fields_item
        
        # Test invalid types (should fail)
        invalid_types_item = {
            "title": 123,  # Should be string
            "content": "Test content"
        }
        
        with pytest.raises(PDFValidationError) as exc_info:
            validate_manually(invalid_types_item, self.mock_config)
        
        assert "Invalid field types" in str(exc_info.value)
    
    def test_extract_fields_from_text_success(self):
        """Test successful field extraction with schema validation."""
        # Mock OpenAI response
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = json.dumps({
            "title": "Extracted Title",
            "content": "Extracted content"
        })
        
        self.mock_client.chat.completions.create.return_value = mock_response
        
        result = extract_fields_from_text(
            "Sample document text",
            self.mock_config,
            self.mock_client
        )
        
        assert len(result) == 1
        assert result[0]["title"] == "Extracted Title"
        assert result[0]["content"] == "Extracted content"
        
        # Verify OpenAI was called with correct parameters
        self.mock_client.chat.completions.create.assert_called_once()
        call_args = self.mock_client.chat.completions.create.call_args
        assert call_args[1]["response_format"] == {"type": "json_object"}
        assert call_args[1]["temperature"] == 0.0
    
    def test_extract_fields_from_text_empty_input(self):
        """Test field extraction with empty input."""
        with pytest.raises(FieldExtractionError) as exc_info:
            extract_fields_from_text("", self.mock_config, self.mock_client)
        
        assert "empty" in str(exc_info.value).lower()
    
    def test_extract_fields_from_text_no_fields(self):
        """Test field extraction with no fields defined."""
        config_no_fields = SourceConfig(
            name="test_source",
            description="Test source with no fields",
            file_pattern="test/*.pdf",
            unique_id_field="title",
            fields=[],
            extraction_instructions="No fields to extract."
        )
        
        with pytest.raises(FieldExtractionError) as exc_info:
            extract_fields_from_text("Sample text", config_no_fields, self.mock_client)
        
        assert "No fields defined" in str(exc_info.value)
    
    def test_extract_fields_from_text_json_parse_error(self):
        """Test field extraction with JSON parse error."""
        # Mock OpenAI response with invalid JSON
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "invalid json"
        
        self.mock_client.chat.completions.create.return_value = mock_response
        
        with pytest.raises(FieldExtractionError) as exc_info:
            extract_fields_from_text("Sample text", self.mock_config, self.mock_client)
        
        assert "Failed to parse JSON" in str(exc_info.value)

if __name__ == "__main__":
    pytest.main([__file__]) 
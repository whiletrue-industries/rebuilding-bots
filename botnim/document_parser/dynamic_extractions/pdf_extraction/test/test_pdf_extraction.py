import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_extraction_config import PDFExtractionConfig, FieldConfig, SourceConfig
from botnim.document_parser.dynamic_extractions.pdf_extraction.csv_output import flatten_for_csv, write_csv
from botnim.document_parser.dynamic_extractions.pdf_extraction.field_extraction import extract_fields_from_text

@pytest.fixture
def sample_config():
    """Sample configuration for testing"""
    return {
        "sources": [
            {
                "name": "Test Source",
                "description": "Test source for unit tests",
                "file_pattern": "test/*.pdf",
                "unique_id_field": "source_url",
                "metadata": {
                    "source_url": "{pdf_url}",
                    "download_date": "{download_date}"
                },
                "fields": [
                    {
                        "name": "title",
                        "description": "Document title",
                        "example": "Test Document"
                    },
                    {
                        "name": "content",
                        "description": "Document content",
                        "hint": "Full text content"
                    }
                ],
                "extraction_instructions": "Extract title and content from the document."
            }
        ]
    }

def test_config_loading(sample_config):
    """Test loading configuration from dictionary."""
    config = PDFExtractionConfig(**sample_config)
    assert len(config.sources) == 1
    assert config.sources[0].name == "Test Source"
    assert len(config.sources[0].fields) == 2

def test_field_config():
    """Test FieldConfig model."""
    field = FieldConfig(
        name="test_field",
        description="Test field",
        example="test example",
        hint="test hint"
    )
    assert field.name == "test_field"
    assert field.description == "Test field"

def test_source_config():
    """Test SourceConfig model."""
    source = SourceConfig(
        name="Test Source",
        file_pattern="test/*.pdf",
        unique_id_field="source_url",
        fields=[FieldConfig(name="test_field")]
    )
    assert source.name == "Test Source"
    assert len(source.fields) == 1

def test_flatten_for_csv():
    """Test flattening document data for CSV output."""
    doc = {
        "fields": {
            "title": "Test Title",
            "content": "Test Content"
        },
        "metadata": {
            "source_url": "http://example.com",
            "download_date": "2024-01-01"
        }
    }
    fieldnames = ["title", "content", "source_url", "download_date"]
    
    result = flatten_for_csv(doc, fieldnames)
    
    assert result["title"] == "Test Title"
    assert result["content"] == "Test Content"
    assert result["source_url"] == "http://example.com"
    assert result["download_date"] == "2024-01-01"

def test_flatten_for_csv_missing_fields():
    """Test flattening with missing fields."""
    doc = {
        "fields": {
            "title": "Test Title"
        },
        "metadata": {
            "source_url": "http://example.com"
        }
    }
    fieldnames = ["title", "content", "source_url", "download_date"]
    
    result = flatten_for_csv(doc, fieldnames)
    
    assert result["title"] == "Test Title"
    assert result["content"] == ""  # Missing field
    assert result["source_url"] == "http://example.com"
    assert result["download_date"] == ""  # Missing field

def test_write_csv():
    """Test writing CSV file."""
    data = [
        {"title": "Doc 1", "content": "Content 1"},
        {"title": "Doc 2", "content": "Content 2"}
    ]
    fieldnames = ["title", "content"]
    
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = write_csv(data, fieldnames, "Test Source", temp_dir)
        
        # Check file exists
        assert os.path.exists(csv_path)
        
        # Check file content
        with open(csv_path, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "title,content" in content
            assert "Doc 1,Content 1" in content
            assert "Doc 2,Content 2" in content

@patch('botnim.document_parser.dynamic_extractions.pdf_extraction.field_extraction.logger')
def test_extract_fields_from_text_mock(mock_logger):
    """Test field extraction with mocked OpenAI client."""
    # Create mock source config
    source_config = SourceConfig(
        name="Test Source",
        file_pattern="test/*.pdf",
        unique_id_field="source_url",
        fields=[
            FieldConfig(name="title", description="Document title"),
            FieldConfig(name="content", description="Document content")
        ],
        extraction_instructions="Extract title and content from the document."
    )
    
    # Create mock OpenAI client
    mock_client = Mock()
    mock_response = Mock()
    mock_response.choices = [Mock()]
    mock_response.choices[0].message.content = '{"title": "Test Title", "content": "Test Content"}'
    mock_client.chat.completions.create.return_value = mock_response
    
    # Test extraction
    text = "This is a test document with a title and content."
    result = extract_fields_from_text(text, source_config, mock_client)
    
    # Verify result - result is now a list
    assert len(result) == 1
    assert result[0]["title"] == "Test Title"
    assert result[0]["content"] == "Test Content"
    
    # Verify OpenAI was called
    mock_client.chat.completions.create.assert_called_once()

@patch('botnim.document_parser.dynamic_extractions.pdf_extraction.field_extraction.logger')
def test_extract_fields_from_text_error(mock_logger):
    """Test field extraction with error handling."""
    source_config = SourceConfig(
        name="Test Source",
        file_pattern="test/*.pdf",
        unique_id_field="source_url",
        fields=[FieldConfig(name="title")],
        extraction_instructions="Extract title from the document."
    )
    
    # Create mock client that raises an exception
    mock_client = Mock()
    mock_client.chat.completions.create.side_effect = Exception("API Error")
    
    text = "Test document"
    result = extract_fields_from_text(text, source_config, mock_client)
    
    # Should return error dict
    assert "error" in result
    assert "API Error" in result["error"]

@pytest.fixture
def sample_pipeline_config():
    """Sample configuration for pipeline testing"""
    return {
        "sources": [
            {
                "name": "Test Source",
                "file_pattern": "test/*.pdf",
                "unique_id_field": "source_url",
                "fields": [
                    {"name": "title", "description": "Document title"},
                    {"name": "content", "description": "Document content"}
                ]
            }
        ]
    }

@patch('botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_pipeline.PDFExtractionConfig')
def test_pipeline_initialization(mock_config_class, sample_pipeline_config):
    """Test pipeline initialization."""
    mock_config = Mock()
    mock_config_class.from_yaml.return_value = mock_config
    mock_config.sources = []
    
    mock_openai_client = Mock()
    
    from botnim.document_parser.dynamic_extractions.pdf_extraction.pdf_pipeline import PDFExtractionPipeline
    
    with tempfile.TemporaryDirectory() as temp_dir:
        pipeline = PDFExtractionPipeline("config.yaml", mock_openai_client, temp_dir)
        
        assert pipeline.output_dir == Path(temp_dir)
        mock_config_class.from_yaml.assert_called_once_with("config.yaml") 
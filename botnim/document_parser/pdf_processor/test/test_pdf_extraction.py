import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock

from botnim.document_parser.pdf_processor.pdf_extraction_config import PDFExtractionConfig, FieldConfig, SourceConfig
from botnim.document_parser.pdf_processor.csv_output import flatten_for_csv, write_csv, write_csv_by_source
from botnim.document_parser.pdf_processor.field_extraction import extract_fields_from_text

@pytest.fixture
def sample_config():
    """Sample configuration for testing with Open Budget data sources"""
    return {
        "sources": [
            {
                "name": "Test Source",
                "description": "Test source for unit tests",
                "unique_id_field": "url",
                "metadata": {
                    "source_type": "test",
                    "data_provider": "open_budget",
                    "test_mode": "true"
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
                "extraction_instructions": "Extract title and content from the document.",
                "index_csv_url": "https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/index.csv",
                "datapackage_url": "https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/datapackage.json"
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
    """Test SourceConfig model with Open Budget fields."""
    source = SourceConfig(
        name="Test Source",
        unique_id_field="url",
        fields=[FieldConfig(name="test_field")],
        index_csv_url="https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/index.csv",
        datapackage_url="https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/datapackage.json"
    )
    assert source.name == "Test Source"
    assert len(source.fields) == 1
    assert source.index_csv_url is not None
    assert source.datapackage_url is not None

def test_flatten_for_csv():
    """Test flattening document data for CSV output."""
    doc = {
        "fields": {
            "title": "Test Title",
            "content": "Test Content"
        },
        "metadata": {
            "url": "http://example.com",
            "download_date": "2024-01-01"
        }
    }
    fieldnames = ["title", "content", "url", "download_date"]
    
    result = flatten_for_csv(doc, fieldnames)
    
    assert result["title"] == "Test Title"
    assert result["content"] == "Test Content"
    assert result["url"] == "http://example.com"
    assert result["download_date"] == "2024-01-01"

def test_flatten_for_csv_missing_fields():
    """Test flattening with missing fields."""
    doc = {
        "fields": {
            "title": "Test Title"
        },
        "metadata": {
            "url": "http://example.com"
        }
    }
    fieldnames = ["title", "content", "url", "download_date"]
    
    result = flatten_for_csv(doc, fieldnames)
    
    assert result["title"] == "Test Title"
    assert result["content"] == ""  # Missing field
    assert result["url"] == "http://example.com"
    assert result["download_date"] == ""  # Missing field

def test_write_csv():
    """Test writing CSV file."""
    data = [
        {"title": "Doc 1", "content": "Content 1"},
        {"title": "Doc 2", "content": "Content 2"}
    ]
    
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = os.path.join(temp_dir, "test_output.csv")
        result_path = write_csv(data, csv_path)
        
        # Check file exists
        assert os.path.exists(result_path)
        
        # Check file content
        with open(result_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Check that both fields are present (order may vary due to sorting)
            assert '"content"' in content
            assert '"title"' in content
            assert '"Doc 1"' in content
            assert '"Content 1"' in content
            assert '"Doc 2"' in content
            assert '"Content 2"' in content

def test_write_csv_by_source():
    """Test writing separate CSV files by source."""
    data = [
        {"source_name": "Source A", "title": "Doc 1", "content": "Content 1", "extra_field": "extra1"},
        {"source_name": "Source A", "title": "Doc 2", "content": "Content 2", "extra_field": "extra2"},
        {"source_name": "Source B", "title": "Doc 3", "content": "Content 3", "different_field": "diff1"},
    ]
    
    source_configs = [
        {
            "name": "Source A",
            "fields": [
                {"name": "title"},
                {"name": "content"}
            ]
        },
        {
            "name": "Source B", 
            "fields": [
                {"name": "title"},
                {"name": "different_field"}
            ]
        }
    ]
    
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_files = write_csv_by_source(data, temp_dir, source_configs)
        
        # Check that files were created
        assert len(csv_files) == 2
        assert "Source A" in csv_files
        assert "Source B" in csv_files
        
        # Check Source A file
        source_a_path = csv_files["Source A"]
        assert os.path.exists(source_a_path)
        with open(source_a_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Check that required fields are present (order may vary)
            assert '"source_name"' in content
            assert '"title"' in content
            assert '"content"' in content
            assert '"Doc 1"' in content
            assert '"Content 1"' in content
            assert '"Doc 2"' in content
            assert '"Content 2"' in content
            # Should not include extra_field or different_field
            assert "extra_field" not in content
            assert "different_field" not in content
        
        # Check Source B file
        source_b_path = csv_files["Source B"]
        assert os.path.exists(source_b_path)
        with open(source_b_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Check that required fields are present (order may vary)
            assert '"source_name"' in content
            assert '"title"' in content
            assert '"different_field"' in content
            assert '"Doc 3"' in content
            assert '"diff1"' in content
            # Should not include content or extra_field
            assert "Content 3" not in content
            assert "extra_field" not in content

@patch('botnim.document_parser.pdf_processor.field_extraction.logger')
def test_extract_fields_from_text_mock(mock_logger):
    """Test field extraction with mocked OpenAI client."""
    # Create mock source config
    source_config = SourceConfig(
        name="Test Source",
        unique_id_field="url",
        fields=[
            FieldConfig(name="title", description="Document title"),
            FieldConfig(name="content", description="Document content")
        ],
        extraction_instructions="Extract title and content from the document.",
        index_csv_url="https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/index.csv",
        datapackage_url="https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/datapackage.json"
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

@patch('botnim.document_parser.pdf_processor.field_extraction.logger')
def test_extract_fields_from_text_error(mock_logger):
    """Test field extraction with error handling."""
    from botnim.document_parser.pdf_processor.exceptions import FieldExtractionError
    
    source_config = SourceConfig(
        name="Test Source",
        unique_id_field="url",
        fields=[FieldConfig(name="title")],
        extraction_instructions="Extract title from the document.",
        index_csv_url="https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/index.csv",
        datapackage_url="https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/datapackage.json"
    )
    
    # Create mock client that raises an exception
    mock_client = Mock()
    mock_client.chat.completions.create.side_effect = Exception("API Error")
    
    text = "Test document"
    
    # Should raise FieldExtractionError
    with pytest.raises(FieldExtractionError) as exc_info:
        extract_fields_from_text(text, source_config, mock_client)
    
    # Check error message
    assert "API Error" in str(exc_info.value)

@pytest.fixture
def sample_pipeline_config():
    """Sample configuration for pipeline testing with Open Budget data sources"""
    return {
        "sources": [
            {
                "name": "Test Source",
                "unique_id_field": "url",
                "fields": [
                    {"name": "title", "description": "Document title"},
                    {"name": "content", "description": "Document content"}
                ],
                "index_csv_url": "https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/index.csv",
                "datapackage_url": "https://next.obudget.org/datapackages/knesset/ethics_committee_decisions/datapackage.json"
            }
        ]
    }

@patch('botnim.document_parser.pdf_processor.pdf_pipeline.PDFExtractionConfig')
def test_pipeline_initialization(mock_config_class, sample_pipeline_config):
    """Test pipeline initialization."""
    mock_config = Mock()
    mock_config_class.from_yaml.return_value = mock_config
    mock_config.sources = []
    
    mock_openai_client = Mock()
    
    from botnim.document_parser.pdf_processor.pdf_pipeline import PDFExtractionPipeline
    
    pipeline = PDFExtractionPipeline("config.yaml", mock_openai_client)
    
    assert pipeline.config == mock_config
    assert pipeline.openai_client == mock_openai_client
    mock_config_class.from_yaml.assert_called_once_with("config.yaml")

def test_url_revision_tracking():
    """Test URL and revision tracking in CSV output."""
    data = [
        {
            "source_name": "test_source",
            "url": "https://example.com/doc1.pdf",
            "revision": "2025.01.01-01",
            "title": "Test Document",
            "content": "Test content"
        }
    ]
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        temp_csv_path = f.name
    
    try:
        write_csv(data, temp_csv_path)
        
        # Verify URL and revision columns are present
        with open(temp_csv_path, 'r', encoding='utf-8') as f:
            content = f.read()
            assert 'url' in content, "URL column missing from CSV"
            assert 'revision' in content, "Revision column missing from CSV"
            assert 'https://example.com/doc1.pdf' in content, "URL data missing from CSV"
            assert '2025.01.01-01' in content, "Revision data missing from CSV"
    
    finally:
        if os.path.exists(temp_csv_path):
            os.unlink(temp_csv_path) 
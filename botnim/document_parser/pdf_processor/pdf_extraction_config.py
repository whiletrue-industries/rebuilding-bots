"""
Configuration models for PDF extraction pipeline.

This module defines Pydantic models for configuring PDF extraction sources,
field definitions, and extraction instructions.
"""

from typing import List, Dict, Optional
from pydantic import BaseModel, Field
import yaml

class FieldConfig(BaseModel):
    """
    Configuration for a single field to be extracted from PDF documents.
    
    Attributes:
        name: The name of the field (used as column header in CSV/Sheets)
        description: Human-readable description of what this field contains
        example: Example value to help the LLM understand the expected format
        hint: Additional hints or instructions for the LLM extraction
    """
    name: str = Field(..., description="Field name used as column header")
    description: Optional[str] = Field(None, description="Human-readable field description")
    example: Optional[str] = Field(None, description="Example value for LLM guidance")
    hint: Optional[str] = Field(None, description="Additional extraction hints")

class SourceConfig(BaseModel):
    """
    Configuration for a PDF source with extraction rules.
    
    A source represents a collection of PDF files that share the same
    structure and extraction rules (e.g., ethics committee decisions).
    
    Attributes:
        name: Unique name for this source
        description: Human-readable description of the source
        unique_id_field: Field name that serves as unique identifier
        metadata: Static metadata to add to all extracted records
        fields: List of fields to extract from documents
        extraction_instructions: Instructions for the LLM extraction process
        index_csv_url: URL to Open Budget index.csv file
        datapackage_url: URL to Open Budget datapackage.json file
    """
    name: str = Field(..., description="Unique source name")
    description: Optional[str] = Field(None, description="Source description")
    unique_id_field: str = Field(..., description="Field name for unique identification")
    metadata: Dict[str, str] = Field(default_factory=dict, description="Static metadata for all records")
    fields: List[FieldConfig] = Field(..., description="Fields to extract from documents")
    extraction_instructions: Optional[str] = Field(None, description="LLM extraction instructions")
    index_csv_url: str = Field(..., description="URL to Open Budget index.csv file")
    datapackage_url: str = Field(..., description="URL to Open Budget datapackage.json file")

class PDFExtractionConfig(BaseModel):
    """
    Main configuration class for the PDF extraction pipeline.
    
    Contains all source configurations and provides methods for loading
    from YAML files.
    
    Attributes:
        sources: List of source configurations
    """
    sources: List[SourceConfig] = Field(..., description="List of PDF source configurations")

    @classmethod
    def from_yaml(cls, path: str) -> "PDFExtractionConfig":
        """
        Load configuration from a YAML file.
        
        Args:
            path: Path to the YAML configuration file
            
        Returns:
            PDFExtractionConfig instance with loaded configuration
            
        Raises:
            FileNotFoundError: If the YAML file doesn't exist
            yaml.YAMLError: If the YAML file is malformed
            ValidationError: If the configuration doesn't match the schema
        """
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return cls(**data) 
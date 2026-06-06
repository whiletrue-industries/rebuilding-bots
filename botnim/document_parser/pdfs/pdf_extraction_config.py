"""
Configuration models for PDF extraction pipeline.
This module defines Pydantic models for configuring PDF extraction sources,
field definitions, and extraction instructions.
"""

from pathlib import Path
from typing import List, Dict, Optional
from pydantic import BaseModel, Field, model_validator
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
    type: Optional[str] = Field('string', description="Data type of the field (e.g., string, date, number)")
    description: Optional[str] = Field(None, description="Human-readable field description")
    example: Optional[str] = Field(None, description="Example value for LLM guidance")
    hint: Optional[str] = Field(None, description="Additional extraction hints")

class SourceConfig(BaseModel):
    """
    Configuration for a PDF source with extraction rules.

    A source represents a collection of PDF files that share the same
    structure and extraction rules (e.g., ethics committee decisions).

    Exactly one of ``external_source_url`` or ``local_index_csv_path``
    must be set. The former points at a BudgetKey datapackage directory
    (legacy Stage-1+2 in one shot); the latter points at a pre-built
    ``index.csv`` written by a separate Stage-1 fetcher (two-stage source
    pattern).

    Attributes:
        fields: List of fields to extract from documents
        extraction_instructions: Instructions for the LLM extraction process
        external_source_url: URL to Open Budget source directory (BK datapackage)
        local_index_csv_path: Local path (relative to config_dir) to a
            pre-built index.csv. Mutually exclusive with external_source_url.
        output_csv_path: Path to the output CSV file
    """
    fields: List[FieldConfig] = Field(..., description="Fields to extract from documents")
    extraction_instructions: Optional[str] = Field(None, description="LLM extraction instructions")
    external_source_url: Optional[str] = Field(
        None, description="URL to Open Budget source directory (BK datapackage)"
    )
    local_index_csv_path: Optional[str] = Field(
        None,
        description=(
            "Local path (relative to config_dir) to a pre-built index.csv. "
            "Mutually exclusive with external_source_url."
        ),
    )
    output_csv_path: Optional[Path] = Field(None, description="Path to the output CSV file (unused; artifact goes through the store)")

    @model_validator(mode='after')
    def _exactly_one_source(self):
        a = self.external_source_url is not None
        b = self.local_index_csv_path is not None
        if a == b:  # both True or both False
            raise ValueError(
                "exactly one of external_source_url or local_index_csv_path must be set"
            )
        return self

# class PDFExtractionConfig(BaseModel):
#     """
#     Main configuration class for the PDF extraction pipeline.
    
#     Contains all source configurations and provides methods for loading
#     from YAML files.
    
#     Attributes:
#         sources: List of source configurations
#     """
#     sources: List[SourceConfig] = Field(..., description="List of PDF source configurations")

#     @classmethod
#     def from_yaml(cls, path: str) -> "PDFExtractionConfig":
#         """
#         Load configuration from a YAML file.
        
#         Args:
#             path: Path to the YAML configuration file
            
#         Returns:
#             PDFExtractionConfig instance with loaded configuration
            
#         Raises:
#             FileNotFoundError: If the YAML file doesn't exist
#             yaml.YAMLError: If the YAML file is malformed
#             ValidationError: If the configuration doesn't match the schema
#         """
#         with open(path, 'r', encoding='utf-8') as f:
#             data = yaml.safe_load(f)
#         return cls(**data) 
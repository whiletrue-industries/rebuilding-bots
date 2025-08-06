"""
Configuration adapters for converting between different config formats.

This module provides adapters to convert between sync configuration formats
and various processor configuration formats, ensuring compatibility.
"""

from typing import List, Dict, Any, Optional
from .config import PDFProcessingField, PDFProcessingConfig
from ..document_parser.pdf_processor.pdf_extraction_config import FieldConfig, SourceConfig


class PDFConfigAdapter:
    """
    Adapter for converting between sync PDF config and PDF processor config.
    """
    
    @staticmethod
    def sync_to_processor_field(sync_field: PDFProcessingField) -> FieldConfig:
        """
        Convert a sync PDF processing field to a PDF processor field config.
        
        Args:
            sync_field: Sync configuration field
            
        Returns:
            PDF processor field configuration
        """
        # Create description with type information
        description = sync_field.description or f"Extract {sync_field.name}"
        if sync_field.type:
            description += f" (type: {sync_field.type})"
        
        # Create example based on field type
        example = PDFConfigAdapter._get_example_for_type(sync_field.type, sync_field.name)
        
        # Create hint based on required status
        hint = None
        if sync_field.required:
            hint = "This field is required and must be extracted"
        
        return FieldConfig(
            name=sync_field.name,
            description=description,
            example=example,
            hint=hint
        )
    
    @staticmethod
    def sync_to_processor_config(sync_config: PDFProcessingConfig, source_name: str) -> SourceConfig:
        """
        Convert a sync PDF processing config to a PDF processor source config.
        
        Args:
            sync_config: Sync PDF processing configuration
            source_name: Name of the source
            
        Returns:
            PDF processor source configuration
        """
        # Convert fields
        fields = [
            PDFConfigAdapter.sync_to_processor_field(field)
            for field in sync_config.fields
        ]
        
        # Create extraction instructions
        extraction_instructions = (
            f"Extract the specified fields from the document. "
            f"Use model: {sync_config.model}, "
            f"max tokens: {sync_config.max_tokens}, "
            f"temperature: {sync_config.temperature}"
        )
        
        return SourceConfig(
            name=source_name,
            description=f"PDF source: {source_name}",
            file_pattern="*.pdf",
            unique_id_field="document_id",  # Default unique ID field
            metadata={},
            fields=fields,
            extraction_instructions=extraction_instructions
        )
    
    @staticmethod
    def _get_example_for_type(field_type: str, field_name: str) -> Optional[str]:
        """
        Generate example values based on field type.
        
        Args:
            field_type: Type of the field
            field_name: Name of the field
            
        Returns:
            Example value string
        """
        examples = {
            "string": "Sample text value",
            "date": "2024-01-15",
            "text": "Longer text content that may span multiple sentences",
            "array": "item1, item2, item3",
            "number": "123",
            "boolean": "true"
        }
        
        # Try to provide contextual examples
        if "date" in field_name.lower():
            return "2024-01-15"
        elif "name" in field_name.lower():
            return "John Doe"
        elif "title" in field_name.lower():
            return "Document Title"
        elif "summary" in field_name.lower():
            return "Brief summary of the document content"
        elif "content" in field_name.lower() or "text" in field_name.lower():
            return "Full text content of the document"
        
        return examples.get(field_type, "Example value")


class ConfigValidationError(Exception):
    """Exception raised when configuration validation fails."""
    pass


class ConfigValidator:
    """
    Validator for ensuring configuration compatibility.
    """
    
    @staticmethod
    def validate_sync_pdf_config(config: PDFProcessingConfig) -> List[str]:
        """
        Validate a sync PDF processing configuration.
        
        Args:
            config: Configuration to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check required fields
        if not config.fields:
            errors.append("PDF processing config must have at least one field defined")
        
        # Validate individual fields
        for i, field in enumerate(config.fields):
            if not field.name:
                errors.append(f"Field {i}: name is required")
            if not field.description:
                errors.append(f"Field {i}: description is required")
            if field.type not in ["string", "date", "text", "array", "number", "boolean"]:
                errors.append(f"Field {i}: invalid type '{field.type}'")
        
        # Validate processing parameters
        if config.max_tokens <= 0:
            errors.append("max_tokens must be positive")
        if not (0 <= config.temperature <= 2):
            errors.append("temperature must be between 0 and 2")
        
        return errors
    
    @staticmethod
    def validate_processor_config(config: SourceConfig) -> List[str]:
        """
        Validate a PDF processor source configuration.
        
        Args:
            config: Configuration to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check required fields
        if not config.name:
            errors.append("Source name is required")
        if not config.file_pattern:
            errors.append("File pattern is required")
        if not config.unique_id_field:
            errors.append("Unique ID field is required")
        if not config.fields:
            errors.append("At least one field must be defined")
        
        # Validate individual fields
        for i, field in enumerate(config.fields):
            if not field.name:
                errors.append(f"Field {i}: name is required")
            if not field.description:
                errors.append(f"Field {i}: description is required")
        
        return errors 
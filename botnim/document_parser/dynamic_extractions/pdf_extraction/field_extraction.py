"""
Field extraction module.

This module is responsible for extracting structured fields from text using LLM with enhanced JSON schema validation.
"""

import logging
import json
from typing import List, Dict, Any, Optional
from pathlib import Path
from pydantic import ValidationError
from .pdf_extraction_config import SourceConfig, PDFExtractionConfig
from .exceptions import FieldExtractionError, ValidationError as PDFValidationError
from botnim.config import get_logger

# Import jsonschema for enhanced validation
import jsonschema
from jsonschema import ValidationError as JSONSchemaValidationError

logger = get_logger(__name__)

def extract_fields_from_text(text: str, config: SourceConfig, client, model: str = "gpt-4.1") -> Dict:
    """
    Extract structured fields from text using LLM with enhanced JSON schema validation.
    
    Args:
        text: Text content to extract fields from
        config: Source configuration with field definitions
        client: OpenAI client
        model: Model to use for extraction
        
    Returns:
        Dictionary with extracted fields or error information
        
    Raises:
        FieldExtractionError: When extraction fails
        PDFValidationError: When validation fails
    """
    if not text or not text.strip():
        raise FieldExtractionError("Input text is empty or contains only whitespace")
    
    if not config.fields:
        raise FieldExtractionError("No fields defined in configuration")
    
    # Build comprehensive JSON schema for validation
    schema = build_extraction_schema(config)
    
    # Build a detailed prompt with field definitions
    field_definitions = []
    for field in config.fields:
        field_def = f"- {field.name}: {field.description}"
        if field.example:
            field_def += f" (example: {field.example})"
        if field.hint:
            field_def += f" (hint: {field.hint})"
        field_definitions.append(field_def)
    
    field_list = "\n".join(field_definitions)
    
    prompt = f"""You are extracting structured data from a Hebrew document. 

Required fields to extract:
{field_list}

{config.extraction_instructions or "Extract the specified fields from the document text. Return a JSON object with the exact field names as specified above."}

IMPORTANT: Return ONLY a JSON object with the exact field names specified above. Do not add any additional fields or change the field names."""
    
    logger.info("Building prompt for field extraction with schema validation.")
    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"""Document text:\n{text}\n\nReturn the result as a JSON object."""}
    ]
    
    try:
        logger.info(f"Sending extraction prompt to OpenAI (model={model}) with JSON response format")
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        content = response.choices[0].message.content
        logger.info("Received JSON response from OpenAI.")
        
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            raise FieldExtractionError(f"Failed to parse JSON from LLM response: {e}\nResponse was: {content}")
        except Exception as e:
            raise FieldExtractionError(f"Unexpected error parsing LLM response: {e}\nResponse was: {content}")
        
        # Validate extracted data using JSON schema
        validated_data = validate_extracted_data(data, schema, config)
        
        return validated_data
            
    except Exception as e:
        if isinstance(e, (FieldExtractionError, PDFValidationError)):
            raise
        raise FieldExtractionError(f"Field extraction failed: {str(e)}")

def build_extraction_schema(config: SourceConfig) -> Dict[str, Any]:
    """
    Build a comprehensive JSON schema for field extraction validation.
    
    Args:
        config: Source configuration with field definitions
        
    Returns:
        JSON schema dictionary for validation
    """
    schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False  # Prevent unexpected fields
    }
    
    # Add field definitions to schema
    for field in config.fields:
        field_schema = {
            "type": "string",
            "description": field.description
        }
        
        # Add field-specific constraints if available
        if field.example:
            field_schema["examples"] = [field.example]
        
        schema["properties"][field.name] = field_schema
        schema["required"].append(field.name)
    
    return schema

def validate_extracted_data(data: Any, schema: Dict[str, Any], config: SourceConfig) -> List[Dict[str, Any]]:
    """
    Validate extracted data using JSON schema validation.
    
    Args:
        data: Data to validate (can be dict or list)
        schema: JSON schema for validation
        config: Source configuration for field information
        
    Returns:
        Validated data as list of dictionaries
        
    Raises:
        PDFValidationError: When validation fails
    """
    if not isinstance(data, (dict, list)):
        raise PDFValidationError(f"LLM returned invalid data type: {type(data)}. Expected dict or list.")
    
    # Handle both single object and array of objects
    if isinstance(data, list):
        logger.info(f"LLM returned array of {len(data)} entities")
        validated_items = []
        
        for i, item in enumerate(data):
            if not isinstance(item, dict):
                raise PDFValidationError(f"Item {i} is not a dictionary: {type(item)}")
            
            validated_item = validate_single_item(item, schema, config, item_index=i)
            validated_items.append(validated_item)
        
        return validated_items
    else:
        # Single object
        validated_item = validate_single_item(data, schema, config)
        return [validated_item]  # Return as array for consistency

def validate_single_item(item: Dict[str, Any], schema: Dict[str, Any], config: SourceConfig, item_index: Optional[int] = None) -> Dict[str, Any]:
    """
    Validate a single item using JSON schema validation.
    
    Args:
        item: Dictionary item to validate
        schema: JSON schema for validation
        config: Source configuration for field information
        item_index: Index of the item (for error reporting)
        
    Returns:
        Validated item dictionary
        
    Raises:
        PDFValidationError: When validation fails
    """
    item_prefix = f"Item {item_index}: " if item_index is not None else ""
    
    try:
        # Use jsonschema for comprehensive validation
        jsonschema.validate(instance=item, schema=schema)
        logger.info(f"{item_prefix}JSON schema validation passed")
        return item
    except JSONSchemaValidationError as e:
        # Provide detailed validation error information
        error_details = []
        for error in e.context:
            error_details.append(f"  - {error.path}: {error.message}")
        
        error_msg = f"{item_prefix}JSON schema validation failed:\n" + "\n".join(error_details)
        logger.error(error_msg)
        raise PDFValidationError(error_msg)

def validate_manually(item: Dict[str, Any], config: SourceConfig, item_prefix: str = "") -> Dict[str, Any]:
    """
    Manual validation fallback when jsonschema is not available.
    
    Args:
        item: Dictionary item to validate
        config: Source configuration for field information
        item_prefix: Prefix for error messages
        
    Returns:
        Validated item dictionary
        
    Raises:
        PDFValidationError: When validation fails
    """
    # Check for missing required fields
    missing_fields = []
    for field in config.fields:
        if field.name not in item:
            missing_fields.append(field.name)
    
    if missing_fields:
        logger.warning(f"{item_prefix}Missing fields: {missing_fields}")
    
    # Check for unexpected fields
    expected_field_names = {field.name for field in config.fields}
    unexpected_fields = [field_name for field_name in item.keys() if field_name not in expected_field_names]
    
    if unexpected_fields:
        logger.warning(f"{item_prefix}Unexpected fields: {unexpected_fields}")
    
    # Check field types
    invalid_types = []
    for field_name, field_value in item.items():
        if not isinstance(field_value, str):
            invalid_types.append(f"{field_name} (expected string, got {type(field_value).__name__})")
    
    if invalid_types:
        error_msg = f"{item_prefix}Invalid field types: {', '.join(invalid_types)}"
        logger.error(error_msg)
        raise PDFValidationError(error_msg)
    
    return item

def build_metadata(input_file: str, source_url: str, extraction_date: str, extra_metadata: dict = None) -> dict:
    metadata = {
        "input_file": input_file,
        "source_url": source_url,
        "extraction_date": extraction_date,
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    return metadata

 
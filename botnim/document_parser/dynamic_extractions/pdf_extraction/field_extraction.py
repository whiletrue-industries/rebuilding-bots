import logging
import json
from typing import List, Dict, Any, Optional
from pathlib import Path
from pydantic import ValidationError
from .pdf_extraction_config import SourceConfig, PDFExtractionConfig
from .exceptions import FieldExtractionError, ValidationError as PDFValidationError
from botnim.config import get_logger

logger = get_logger(__name__)

def extract_fields_from_text(text: str, config: SourceConfig, client, model: str = "gpt-4.1") -> Dict:
    """
    Extract structured fields from text using LLM.
    
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
    
    logger.info("Building prompt for field extraction.")
    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"""Document text:\n{text}\n\nReturn the result as a JSON object."""}
    ]
    
    try:
        logger.info(f"Sending extraction prompt to OpenAI (model={model})")
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
        
        # Validate extracted data
        if not isinstance(data, (dict, list)):
            raise PDFValidationError(f"LLM returned invalid data type: {type(data)}. Expected dict or list.")
        
        # Handle both single object and array of objects
        if isinstance(data, list):
            logger.info(f"LLM returned array of {len(data)} entities")
            # Validate each object in the array
            for i, item in enumerate(data):
                if not isinstance(item, dict):
                    raise PDFValidationError(f"Item {i} is not a dictionary: {type(item)}")
                
                missing = [f.name for f in config.fields if f.name not in item]
                if missing:
                    logger.warning(f"Missing fields in entity {i}: {missing}")
            return data
        else:
            # Single object
            missing = [f.name for f in config.fields if f.name not in data]
            if missing:
                logger.warning(f"Missing fields in extraction: {missing}")
            return [data]  # Return as array for consistency
            
    except Exception as e:
        if isinstance(e, (FieldExtractionError, PDFValidationError)):
            raise
        raise FieldExtractionError(f"Field extraction failed: {str(e)}")

def build_metadata(input_file: str, source_url: str, extraction_date: str, extra_metadata: dict = None) -> dict:
    metadata = {
        "input_file": input_file,
        "source_url": source_url,
        "extraction_date": extraction_date,
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    return metadata

 
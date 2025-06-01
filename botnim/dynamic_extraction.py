import json
import re
from openai import OpenAI
from pathlib import Path
from .config import get_logger

logger = get_logger(__name__)

def clean_json_string(content: str) -> str:
    """
    Clean and fix JSON string content, particularly handling Hebrew quotes and special characters.
    
    Args:
        content (str): The JSON string to clean
        
    Returns:
        str: Cleaned JSON string
    """
    # Strip hidden characters and normalize whitespace
    content = content.strip()
    # Escape quotes between Hebrew letters
    content = re.sub(r'(?<=[\u05D0-\u05EA])"(?=[\u05D0-\u05EA])', r'\\"', content)
    # Escape quotes where a Hebrew letter precedes and space or comma follows
    content = re.sub(r'(?<=[\u05D0-\u05EA])"(?=[\s,])', r'\\"', content)
    # Escape quotes where space or comma precedes and Hebrew letter follows
    content = re.sub(r'(?<=[\s,])"(?=[\u05D0-\u05EA])', r'\\"', content)
    return content

def extract_structured_content(text: str, template: str = None, document_type: str = None) -> dict:
    """
    Extracts structured content from text using OpenAI API.
    
    Args:
        text (str): The text to extract information from
        template (str, optional): JSON template for extraction. If None, uses default template.
        document_type (str, optional): Type of document being processed. Defaults to None.
    
    Returns:
        dict: Extracted structured content
    """
    if template is None:
        template = """{
            "DocumentTitle": "",
            "PublicationDate": "",
            "OfficialSource": "",
            "ReferenceLinks": [],
            "ClauseRepresentation": "",
            "OfficialRoles": [
              {
                "Role": "",
                "ClauseLocation": "",
                "Quote": ""
              }
            ],
            "OfficialOrganizations": [
              {
                "Organization": "",
                "ClauseLocation": "",
                "Quote": ""
              }
            ],
            "Placenames": [
              {
                "Name": "",
                "ClauseLocation": "",
                "Quote": ""
              }
            ],
            "Description": "",
            "LegalReferences": [
            {
              "ReferenceTitle": "",
              "ReferenceText": "",
              "ReferenceQuote": ""
            }
          ],
          "Amendments": [],
          "AdditionalKeywords": [],
          "Topics": []
        }"""

    try:
        # Initialize the client
        client = OpenAI()
        logger.info(f"Extracting structured content for document type: {document_type}")

        system_message = f"""You are a highly accurate legal text extraction engine. Your task is to extract all relevant metadata from the provided legal text according to the JSON template below. Follow these rules exactly:

        1. Use only the information given in the text.
        2. Output must be valid JSON that exactly follows the provided schema—do not add any extra keys or commentary.
        3. Ensure all special characters, especially quotes within text, are properly escaped.
        4. At the document level (DocumentMetadata), extract:
            - "DocumentTitle" from the heading.
            - "OfficialSource" from any indicated section (e.g. "סעיף 137") and include any associated URL in "ReferenceLinks".
            - "ClauseRepresentation" should indicate whether the metadata pertains to a main clause, sub-clause, or specific section.
            - Extract any official roles/positions mentioned in the document and list them in "OfficialRoles".
            - Extract any official organizations mentioned in the document and list them in "OfficialOrganizations".
            - Extract any real-world locations or placenames mentioned in the document and list them in "Placenames".
            - "Description" should be a one-line summary describing the entire document's clauses content.
        5. At the document level, also extract:
            - "LegalReferences": For each legal reference
            - "Amendments": If any amendment information is present
            - "AdditionalKeywords": Extract key legal terms, topics, and identifiers
            - "Topics": Aggregate all one-line descriptions from sub-clauses
        6. For any field where no data is provided, return an empty string or an empty array as appropriate.
        7. Do not infer or generate data that is not explicitly provided.
        8. Ensure all key names follow standard, consistent naming.
        9. Output only the JSON.

        Extraction Template:
        {template}

        Text:
        {text}

        Output (JSON only):"""

        # Make the API call without streaming
        logger.info("Calling OpenAI API for content extraction")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_message}],
            temperature=0.0,
            max_tokens=2000,
            stream=False,
            response_format={"type": "json_object"}
        )

        # Get the response content and parse as JSON
        # First approach: Try to parse the raw response directly
        raw_content = response.choices[0].message.content
        try:
            extracted_data = json.loads(raw_content)
            logger.info(f"Successfully extracted structured content: {json.dumps(extracted_data, ensure_ascii=False)}...\n")
            return extracted_data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response as JSON: {e} -->")
            logger.error(f"Response content: {raw_content}")
            
            # Second approach: Fix Hebrew quotes directly in the raw content
            fixed_content = raw_content
            
            # Replace Hebrew quotes in the most common patterns
            # 1. Fix quotes between Hebrew letters (like יו"ר)
            fixed_content = re.sub(r'(?<=[\u05D0-\u05EA])"(?=[\u05D0-\u05EA])', r'\\"', fixed_content)
            
            # 2. Fix quotes in the description field with complex Hebrew text
            if '"Description":' in fixed_content:
                parts = fixed_content.split('"Description":', 1)
                if len(parts) > 1:
                    before = parts[0]
                    desc_and_after = parts[1]
                    # Find the end of the description value (next field or end of object)
                    match = re.search(r',\s*"[A-Za-z]+":|\n\s*\}', desc_and_after)
                    if match:
                        desc_end_pos = match.start()
                        desc_value = desc_and_after[:desc_end_pos]
                        after = desc_and_after[desc_end_pos:]
                        # Fix quotes in the description
                        desc_value = re.sub(r'(?<=[\u05D0-\u05EA\s])"(?=[\u05D0-\u05EA\s])', r'\\"', desc_value)
                        fixed_content = before + '"Description":' + desc_value + after
            
            # 3. Fix quotes in role names, organizations, keywords, etc.
            for field in ['"DocumentTitle":', '"Role":', '"Organization":', '"Quote":', '"AdditionalKeywords":']:
                if field in fixed_content:
                    parts = fixed_content.split(field, 1)
                    if len(parts) > 1:
                        before = parts[0]
                        value_and_after = parts[1].strip()
                        # Check if it's an array or simple value
                        if value_and_after.startswith('['):
                            # It's an array, more complex handling needed
                            # We'll use a simple approach first
                            value_and_after = re.sub(r'(?<=[\u05D0-\u05EA\s])"(?=[\u05D0-\u05EA\s])', r'\\"', value_and_after)
                        else:
                            # Simple value, find the end of the value
                            match = re.search(r',\s*"[A-Za-z]+":|\n\s*\}', value_and_after)
                            if match:
                                value_end_pos = match.start()
                                value = value_and_after[:value_end_pos]
                                after = value_and_after[value_end_pos:]
                                # More aggressive handling for DocumentTitle field
                                if field == '"DocumentTitle":':
                                    # For DocumentTitle, catch all Hebrew quotes 
                                    value = re.sub(r'(?<=[\u05D0-\u05EA])"(?=[\u05D0-\u05EA])', r'\\"', value)
                                    value = re.sub(r'(?<=[\u05D0-\u05EA])"(?=[\s])', r'\\"', value)
                                    value = re.sub(r'(?<=[\s])"(?=[\u05D0-\u05EA])', r'\\"', value)
                                else:
                                    value = re.sub(r'(?<=[\u05D0-\u05EA\s])"(?=[\u05D0-\u05EA\s])', r'\\"', value)
                                value_and_after = value + after
                        fixed_content = before + field + value_and_after
            
            # Last resort: Try to parse the fixed content
            try:
                extracted_data = json.loads(fixed_content)
                logger.info(f"Successfully parsed JSON after fixing Hebrew quotes: {json.dumps(extracted_data, ensure_ascii=False)}")
                return extracted_data
            except json.JSONDecodeError:
                # Final approach: Try to manually reconstruct the JSON
                try:
                    # Create a minimal valid JSON structure
                    minimal_json = {
                        "DocumentTitle": "Parsing Error",
                        "Description": "Failed to parse API response",
                        "error": str(e),
                        "raw_content": raw_content
                    }
                    
                    # Try to extract some key information
                    title_match = re.search(r'"DocumentTitle":\s*"([^"]+)"', raw_content)
                    if title_match:
                        minimal_json["DocumentTitle"] = title_match.group(1).replace('"', '\\"')
                        
                    desc_match = re.search(r'"Description":\s*"([^"]+)"', raw_content)
                    if desc_match:
                        minimal_json["Description"] = desc_match.group(1).replace('"', '\\"')
                        
                    logger.info(f"Created minimal JSON with extracted information")
                    return minimal_json
                except Exception as ex:
                    logger.error(f"Failed to create minimal JSON: {ex}")
                    return {
                        "DocumentTitle": "Parsing Error",
                        "Description": "Failed to parse API response",
                        "error": str(e),
                        "raw_content": raw_content
                    }
    except Exception as e:
        logger.error(f"Error in extract_structured_content: {str(e)}")
        return {"error": str(e)}



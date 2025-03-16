import json
import re
from openai import OpenAI
from pathlib import Path
from .config import get_logger

logger = get_logger(__name__)

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
        4. When including Hebrew text with quotation marks, ensure they are properly escaped with backslashes.
        5. At the document level (DocumentMetadata), extract:
            - "DocumentTitle" from the heading.
            - "OfficialSource" from any indicated section (e.g. "סעיף 137") and include any associated URL in "ReferenceLinks".
            - "ClauseRepresentation" should indicate whether the metadata pertains to a main clause, sub-clause, or specific section.
            - Extract any official roles/positions mentioned in the document and list them in "OfficialRoles".
            - Extract any official organizations mentioned in the document and list them in "OfficialOrganizations".
            - Extract any real-world locations or placenames mentioned in the document and list them in "Placenames".
            - "Description" should be a one-line summary describing the entire document's clauses content.
        6. At the document level, also extract:
            - "LegalReferences": For each legal reference
            - "Amendments": If any amendment information is present
            - "AdditionalKeywords": Extract key legal terms, topics, and identifiers
            - "Topics": Aggregate all one-line descriptions from sub-clauses
        7. For any field where no data is provided, return an empty string or an empty array as appropriate.
        8. Do not infer or generate data that is not explicitly provided.
        9. Ensure all key names follow standard, consistent naming.
        10. Output only the JSON.

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
            stream=False  
        )

        # Get the response content and parse as JSON
        try:
            extracted_data = json.loads(response.choices[0].message.content)
            logger.info(f"Successfully extracted structured content: {json.dumps(extracted_data, ensure_ascii=False)}...\n")
            return extracted_data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response as JSON: {e} -->")
            logger.error(f"Response content: {response.choices[0].message.content}")
            
            # try to fix common JSON parsing issues
            try:
                # Try to parse with more lenient JSON parsing
                content = response.choices[0].message.content
                # Replace problematic quotes in Hebrew text
                content = re.sub(r'(["]\w+)["]([\w\s]+["]\w+)', r'\1\"\2', content)
                extracted_data = json.loads(content)
                logger.info(f"Successfully parsed JSON after fixing: {json.dumps(extracted_data, ensure_ascii=False)}")
                return extracted_data
            except Exception as recovery_error:
                logger.error(f"Recovery attempt failed: {str(recovery_error)}")
                # Return a minimal valid structure instead of error
                return {
                    "DocumentMetadata": {
                        "DocumentTitle": "Parsing Error",
                        "Description": "Failed to parse API response"
                    },
                    "error": str(e),
                    "raw_content": response.choices[0].message.content
                }
    except Exception as e:
        logger.error(f"Error in extract_structured_content: {str(e)}")
        return {"error": str(e)}

def determine_document_type(file_path: Path) -> str:
    """
    Determine the document type by extracting it from the file name.
    The convention is that the document type appears before the underscore.
    
    Args:
        file_path (Path): Path to the source file
        
    Returns:
        str: Document type, defaults to empty string if pattern not found
    """
    try:
        filename = file_path.stem  # Get filename without extension
        if '_' in filename:
            doc_type = filename.split('_')[0]
            return doc_type.strip()
    except Exception as e:
        logger.error(f"Warning: Could not determine document type from filename {file_path}: {e}")
    
    return ""  # Default type if pattern not found 
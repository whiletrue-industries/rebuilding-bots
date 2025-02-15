from openai import OpenAI
from dotenv import load_dotenv
import os
import json

load_dotenv()

def extract_structured_content(text: str, template: str = None) -> dict:
    """
    Extracts structured content from text using OpenAI API.
    
    Args:
        text (str): The text to extract information from
        template (str, optional): JSON template for extraction. If None, uses default template.
    
    Returns:
        dict: Extracted structured content
    """
    if template is None:
        template = """{
          "DocumentMetadata": {
            "DocumentTitle": "",
            "DocumentType": "תקנון הכנסת",
            "PublicationDate": "",
            "OfficialSource": "",
            "ReferenceLinks": [],
            "Language": "עברית",
            "Version": "",
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
            "Description": ""
          },
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

    # Initialize the client
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    system_message = f"""You are a highly accurate legal text extraction engine. Your task is to extract all relevant metadata from the provided legal text according to the JSON template below. Follow these rules exactly:

    1. Use only the information given in the text.
    2. Output must be valid JSON that exactly follows the provided schema—do not add any extra keys or commentary.
    3. At the document level (DocumentMetadata), extract:
        - "DocumentTitle" from the heading.
        - "OfficialSource" from any indicated section (e.g. "סעיף 137") and include any associated URL in "ReferenceLinks".
        - "ClauseRepresentation" should indicate whether the metadata pertains to a main clause, sub-clause, or specific section.
        - Extract any official roles/positions mentioned in the document and list them in "OfficialRoles".
        - Extract any official organizations mentioned in the document and list them in "OfficialOrganizations".
        - Extract any real-world locations or placenames mentioned in the document and list them in "Placenames".
        - "Description" should be a one-line summary describing the entire document's clause content.
    4. At the document level, also extract:
        - "LegalReferences": For each legal reference
        - "Amendments": If any amendment information is present
        - "AdditionalKeywords": Extract key legal terms, topics, and identifiers
        - "Topics": Aggregate all one-line descriptions from sub-clauses
    5. For any field where no data is provided, return an empty string or an empty array as appropriate.
    6. Do not infer or generate data that is not explicitly provided.
    7. Ensure all key names follow standard, consistent naming.
    8. Output only the JSON.

    Extraction Template:
    {template}

    Text:
    {text}

    Output (JSON only):"""

    # Make the API call without streaming
    response = client.chat.completions.create(
        model="gpt-4o-mini",  # or your preferred model
        messages=[{"role": "system", "content": system_message}],
        temperature=0.0,
        max_tokens=2000,
        stream=False  
    )

    # Get the response content and parse as JSON
    try:
        extracted_data = json.loads(response.choices[0].message.content)
        return extracted_data
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse API response as JSON: {e}")

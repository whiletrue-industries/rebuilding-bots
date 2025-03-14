import io
import dataflows as DF
import mimetypes
import json
from pathlib import Path
import os
import glob
from datetime import datetime, timezone
from openai import OpenAI
from .config import get_logger
import dataflows as DF
import re


logger = get_logger(__name__)

# Copy the necessary functions from dynamic_extraction.py
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
          "DocumentMetadata": {
            "DocumentTitle": "",
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

def collect_sources_files(config_dir, context_name, source):
    files = list(config_dir.glob(source))
    file_streams = [(f.name, f.open('rb'), 'text/markdown') for f in files]
    return file_streams

def collect_sources_split(config_dir, context_name, source, extract_metadata=False):
    """
    Collect sources from a split file with enhanced metadata extraction.
    """
    try:
        filename = Path(config_dir) / source
        logger.info(f"Processing split file: {filename}")
        
        content = filename.read_text()
        sections = content.split('\n---\n')
        logger.info(f"Split file into {len(sections)} sections")
        
        sources = []
        for idx, section in enumerate(sections):
            if section.strip():
                file_name = f'{context_name}_{idx}.md'
                section_bytes = section.strip().encode('utf-8')
                mime_type = 'text/markdown'
                
                metadata = None
                if extract_metadata:
                    # Basic metadata
                    metadata = {
                        'source_type': 'split',
                        'original_file': str(filename),
                        'section_index': idx,
                        'section_size': len(section_bytes),
                        'total_sections': len(sections),
                        'mime_type': mime_type,
                        'extracted_at': datetime.now(timezone.utc).isoformat(),
                        'status': 'processed'
                    }
                    
                    # Enhanced metadata using LLM extraction
                    try:
                        document_type = determine_document_type(filename)
                        logger.info(f"Extracting structured content for split section {idx} with document type: {document_type}")
                        extracted_data = extract_structured_content(section, document_type=document_type)
                        
                        # Add extracted data to metadata
                        metadata['document_type'] = document_type
                        metadata['extracted_data'] = extracted_data
                        
                        # Use document title from extraction if available
                        if extracted_data.get('DocumentMetadata', {}).get('DocumentTitle'):
                            metadata['title'] = extracted_data['DocumentMetadata']['DocumentTitle']
                        else:
                            metadata['title'] = f"{filename.stem} (Section {idx+1})"
                            
                        logger.info(f"Added enhanced metadata for section {idx}")
                    except Exception as e:
                        logger.error(f"Error extracting structured content from section {idx}: {str(e)}")
                        metadata['status'] = 'extraction_error'
                        metadata['error'] = str(e)
                
                sources.append((file_name, io.BytesIO(section_bytes), mime_type, metadata))
                logger.info(f"Added split section {idx} as {file_name}")
        
        return sources
    except Exception as e:
        logger.error(f"Error processing split file {source}: {str(e)}")
        return []

def collect_sources_google_spreadsheet(context_name, source, extract_metadata=False):
    """
    Collect sources from a Google Spreadsheet with enhanced metadata extraction.
    """
    try:        
        url = source
        logger.info(f"Processing Google Spreadsheet with dataflows: {url}")
        
        resources, dp, _ = DF.Flow(
            DF.load(url, name='rows'),
        ).results()
        
        rows = resources[0]
        headers = [f.name for f in dp.resources[0].schema.fields]
        
        logger.info(f"Loaded spreadsheet with {len(rows)} rows and headers: {headers}")
        
        sources = []
        for idx, row in enumerate(rows):
            content = ''
            if len(headers) > 1:
                for i, header in enumerate(headers):
                    if row.get(header):
                        if i > 0:
                            content += f'{header}:\n{row[header]}\n\n'
                        else:
                            content += f'{row[header]}\n\n'
            
            if content:
                file_name = f'{context_name}_{idx}.md'
                content_bytes = content.strip().encode('utf-8')
                mime_type = 'text/markdown'
                
                metadata = None
                if extract_metadata:
                    # Basic metadata
                    metadata = {
                        'source_type': 'google_spreadsheet',
                        'url': url,
                        'row_index': idx,
                        'headers': headers,
                        'row_data': {h: row.get(h) for h in headers if row.get(h)},
                        'extracted_at': datetime.now(timezone.utc).isoformat(),
                        'status': 'processed'
                    }
                    
                    # Enhanced metadata using LLM extraction
                    try:
                        document_type = "spreadsheet_row"
                        logger.info(f"Extracting structured content for spreadsheet row {idx}")
                        extracted_data = extract_structured_content(content, document_type=document_type)
                        
                        # Add extracted data to metadata
                        metadata['document_type'] = document_type
                        metadata['extracted_data'] = extracted_data
                        
                        # Use document title from extraction if available
                        if extracted_data.get('DocumentMetadata', {}).get('DocumentTitle'):
                            metadata['title'] = extracted_data['DocumentMetadata']['DocumentTitle']
                        else:
                            # Use first column value as title if available
                            first_header = headers[0] if headers else None
                            metadata['title'] = row.get(first_header, f"Row {idx}")
                            
                        logger.info(f"Added enhanced metadata for spreadsheet row {idx}")
                    except Exception as e:
                        logger.error(f"Error extracting structured content from spreadsheet row {idx}: {str(e)}")
                        metadata['status'] = 'extraction_error'
                        metadata['error'] = str(e)
                
                sources.append((file_name, io.BytesIO(content_bytes), mime_type, metadata))
                logger.info(f"Added spreadsheet row {idx} as {file_name}")
        
        return sources
    except ImportError:
        logger.error("dataflows library not available, cannot process Google Spreadsheet")
        return []
    except Exception as e:
        logger.error(f"Error processing spreadsheet {url}: {str(e)}")
        return []

def collect_context_sources(context_config, config_dir, extract_metadata=False):
    """
    Collect context sources from the config directory.
    """
    logger.info(f"Collecting context sources from {config_dir} with metadata extraction: {extract_metadata}")
    logger.info(f"Context config: {context_config}")
    
    # Add detailed logging about the directory and files
    try:
        config_dir_path = Path(config_dir)
        logger.info(f"Config directory exists: {config_dir_path.exists()}")
        logger.info(f"Config directory is a directory: {config_dir_path.is_dir()}")
        
        if config_dir_path.exists() and config_dir_path.is_dir():
            # List files in the directory
            files = list(config_dir_path.glob("*"))
            logger.info(f"Files in config directory: {[str(f) for f in files]}")
    except Exception as e:
        logger.error(f"Error inspecting directory: {str(e)}")
    
    sources = []
    
    # Handle the case where the context has 'type' and 'source' directly
    if 'type' in context_config and 'source' in context_config:
        source_type = context_config['type']
        source_path = context_config['source']
        context_name = context_config['slug']
        
        logger.info(f"Processing source of type: {source_type}, path: {source_path}")
        
        try:
            if source_type == 'files':
                # Check if extraction is a directory
                extraction_dir_name = context_config.get('extraction_dir', 'extraction')
                extraction_dir = os.path.join(config_dir, extraction_dir_name)
                
                if os.path.isdir(extraction_dir):
                    logger.info(f"Found extraction directory: {extraction_dir}")
                    
                    # Use the original pattern but with the correct path
                    path_pattern = os.path.join(extraction_dir, '*.md')
                    logger.info(f"Looking for files matching pattern: {path_pattern}")
                    
                    matching_files = glob.glob(path_pattern)
                    logger.info(f"Found {len(matching_files)} matching files")
                    
                    for file_path in matching_files:
                        try:
                            with open(file_path, 'rb') as f:
                                content = f.read()
                            mime_type = 'text/markdown'
                            
                            metadata = None
                            if extract_metadata:
                                # Basic metadata
                                metadata = {
                                    'source_type': 'file',
                                    'filename': os.path.basename(file_path),
                                    'file_path': file_path,
                                    'file_size': len(content),
                                    'mime_type': mime_type,
                                    'extracted_at': datetime.now(timezone.utc).isoformat(),
                                    'status': 'processed'
                                }
                                
                                # Enhanced metadata using LLM extraction
                                try:
                                    content_text = content.decode('utf-8')
                                    document_type = determine_document_type(Path(file_path))
                                    logger.info(f"Extracting structured content for file {file_path} with document type: {document_type}")
                                    extracted_data = extract_structured_content(content_text, document_type=document_type)
                                    
                                    # Add extracted data to metadata
                                    metadata['document_type'] = document_type
                                    metadata['extracted_data'] = extracted_data
                                    
                                    # Use document title from extraction if available
                                    if extracted_data.get('DocumentMetadata', {}).get('DocumentTitle'):
                                        metadata['title'] = extracted_data['DocumentMetadata']['DocumentTitle']
                                    else:
                                        metadata['title'] = os.path.basename(file_path)
                                        
                                    logger.info(f"Added enhanced metadata for {file_path}")
                                except Exception as e:
                                    logger.error(f"Error extracting structured content from {file_path}: {str(e)}")
                                    metadata['status'] = 'extraction_error'
                                    metadata['error'] = str(e)
                            
                            sources.append((file_path, io.BytesIO(content), mime_type, metadata))
                            logger.info(f"Added file: {file_path}")
                        except Exception as e:
                            logger.error(f"Error processing file {file_path}: {str(e)}")
                else:
                    logger.error(f"Extraction directory not found: {extraction_dir}")
            
            elif source_type == 'google-spreadsheet':
                # Use enhanced spreadsheet collection
                sources.extend(collect_sources_google_spreadsheet(context_name, source_path, extract_metadata))
            
            elif source_type == 'split':
                # Use enhanced split collection
                sources.extend(collect_sources_split(config_dir, context_name, source_path, extract_metadata))
            
            else:
                logger.warning(f"Unsupported source type: {source_type}")
        
        except Exception as e:
            logger.error(f"Error processing source of type {source_type}: {str(e)}")
    
    # Handle the case where the context has a 'sources' list
    elif 'sources' in context_config:
        for source in context_config.get('sources', []):
            source_type = source.get('type')
            logger.info(f"Processing source of type: {source_type}, config: {source}")
            
            try:
                if source_type == 'file':
                    new_sources = collect_sources_file(source, config_dir, extract_metadata)
                    logger.info(f"Collected {len(new_sources)} file sources")
                    sources.extend(new_sources)
                elif source_type == 'google_spreadsheet':
                    new_sources = collect_sources_google_spreadsheet(source, extract_metadata)
                    logger.info(f"Collected {len(new_sources)} spreadsheet sources")
                    sources.extend(new_sources)
                else:
                    logger.warning(f"Unknown source type: {source_type}")
            except Exception as e:
                logger.error(f"Error collecting sources of type {source_type}: {str(e)}")
    
    else:
        logger.warning(f"Context config does not have 'type'+'source' or 'sources' field: {context_config}")
    
    logger.info(f"Collected {len(sources)} total sources")
    return sources

def collect_sources_file(source, config_dir, extract_metadata=False):
    """
    Collect sources from a file with enhanced metadata extraction.
    """
    path = Path(config_dir) / source['path']
    logger.info(f"Collecting file source from path: {path}")
    
    if not path.exists():
        logger.error(f"File not found: {path}")
        return []
    
    try:
        with open(path, 'rb') as f:
            content = f.read()
        mime_type, _ = mimetypes.guess_type(path)
        logger.info(f"Read file {path} with size {len(content)} and mime type {mime_type}")
        
        metadata = None
        if extract_metadata:
            # Basic metadata
            metadata = {
                'source_type': 'file',
                'filename': str(path.name),
                'file_path': str(path),
                'file_size': len(content),
                'mime_type': mime_type,
                'extracted_at': datetime.now(timezone.utc).isoformat(),
                'status': 'processed'
            }
            
            # Enhanced metadata using LLM extraction
            if mime_type in ['text/markdown', 'text/plain', 'application/json']:
                try:
                    content_text = content.decode('utf-8')
                    document_type = determine_document_type(path)
                    logger.info(f"Extracting structured content for file {path} with document type: {document_type}")
                    extracted_data = extract_structured_content(content_text, document_type=document_type)
                    
                    # Add extracted data to metadata
                    metadata['document_type'] = document_type
                    metadata['extracted_data'] = extracted_data
                    
                    # Use document title from extraction if available
                    if extracted_data.get('DocumentMetadata', {}).get('DocumentTitle'):
                        metadata['title'] = extracted_data['DocumentMetadata']['DocumentTitle']
                    else:
                        metadata['title'] = path.stem
                        
                    logger.info(f"Added enhanced metadata for {path}")
                except Exception as e:
                    logger.error(f"Error extracting structured content from {path}: {str(e)}")
                    metadata['status'] = 'extraction_error'
                    metadata['error'] = str(e)
            else:
                metadata['title'] = path.stem
                metadata['status'] = 'unsupported_format'
                
            logger.info(f"Created metadata for file {path}")
        
        return [(str(path), io.BytesIO(content), mime_type, metadata)]
    except Exception as e:
        logger.error(f"Error reading file {path}: {str(e)}")
        return []

def collect_all_sources(context_list, config_dir):
    all_sources = []
    for context in context_list:
        all_sources.append(dict(
            **context,
            file_streams=collect_context_sources(context, config_dir)
        ))
    return all_sources
#!/usr/bin/env python3
import argparse
import logging
import os
from pathlib import Path
from typing import Optional, Union, List, Dict
import dotenv
import time

from openai import OpenAI

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TextToMarkdownConverter:
    def __init__(self, model: str = "gpt-4o"):
        """Initialize the converter with the specified LLM model."""
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.model = model
        self.temperature = 0.3

    def convert_text_to_markdown(self, text: str) -> str:
        """Convert text to markdown using LLM."""
        try:
            logger.info("Starting markdown conversion...")
            logger.info(f"Using model: {self.model}")
            
            # Retry logic with exponential backoff
            max_retries = 3
            base_delay = 1  # seconds
            
            for attempt in range(max_retries):
                try:
                    response = self.client.chat.completions.create(
                        model=self.model,
                        messages=[
                            {"role": "system", "content": "You are a helpful assistant that converts text to well-structured markdown. Follow these rules strictly:\n1. Preserve ALL original content - do not omit, summarize, or change any text\n2. Use appropriate markdown headings (h1, h2, etc.) to maintain structure\n3. Format lists and tables properly while keeping their exact content\n4. Maintain any special formatting, emphasis, or styling from the original text\n5. Ensure proper spacing and line breaks\n6. If the text contains code blocks, preserve them exactly as they appear\n7. Do not add any new content or commentary - only convert the existing text to markdown format\n8. Do NOT wrap the output in markdown code blocks (```markdown or ```)"},
                            {"role": "user", "content": f"Convert the following text to markdown, preserving all content exactly as it appears:\n\n{text}"}
                        ],
                        temperature=self.temperature
                    )
                    
                    # Validate response
                    if not response or not response.choices:
                        raise ValueError("Empty response from OpenAI API")
                    
                    # Get the content from the first choice
                    content = response.choices[0].message.content
                    if not content:
                        raise ValueError("Empty content in response")
                    
                    # Remove markdown code block markers if present
                    content = content.strip()
                    if content.startswith("```markdown"):
                        content = content[11:].strip()
                    elif content.startswith("```"):
                        content = content[3:].strip()
                    if content.endswith("```"):
                        content = content[:-3].strip()
                    
                    logger.info("Markdown conversion completed successfully")
                    return content
                    
                except Exception as e:
                    error_msg = str(e)
                    if attempt < max_retries - 1:  # Don't retry on the last attempt
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Attempt {attempt + 1} failed: {error_msg}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        raise  # Re-raise the last exception
                        
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error converting text to markdown: {error_msg}")
            
            if "authentication" in error_msg.lower():
                logger.error("Authentication error. Please check your OPENAI_API_KEY environment variable.")
            elif "rate limit" in error_msg.lower():
                logger.error("Rate limit exceeded. Please try again later or check your API quota.")
            elif "model not found" in error_msg.lower():
                logger.error(f"Model {self.model} not found. Please check if the model name is correct.")
            else:
                logger.error("Unknown error occurred. Please check your internet connection and API key.")
            
            raise

    def split_text(self, text: str, max_chunk_size: int = 4000) -> List[str]:
        """Split text into chunks of approximately equal size, trying to break at natural boundaries."""
        chunks = []
        current_chunk = []
        current_size = 0
        
        # Split text into paragraphs
        paragraphs = text.split('\n\n')
        
        for para in paragraphs:
            para_size = len(para)
            
            # If adding this paragraph would exceed the chunk size, start a new chunk
            if current_size + para_size > max_chunk_size and current_chunk:
                chunks.append('\n\n'.join(current_chunk))
                current_chunk = []
                current_size = 0
            
            current_chunk.append(para)
            current_size += para_size
        
        # Add the last chunk if it's not empty
        if current_chunk:
            chunks.append('\n\n'.join(current_chunk))
        
        return chunks

    def process_large_text(self, text: str) -> str:
        """Process large text by splitting into chunks and combining results."""
        chunks = self.split_text(text)
        logger.info(f"Split text into {len(chunks)} chunks for processing")
        
        combined_markdown = []
        for i, chunk in enumerate(chunks, 1):
            logger.info(f"Processing chunk {i}/{len(chunks)}")
            chunk_markdown = self.convert_text_to_markdown(chunk)
            combined_markdown.append(chunk_markdown)
        
        # Combine all chunks with proper spacing
        return "\n\n".join(combined_markdown)

    def extract_document_name(self, text: str) -> str:
        """Use LLM to extract the document name from the text.
        
        Args:
            text: The input text
            
        Returns:
            The extracted document name
        """
        try:
            logger.info("Extracting document name with LLM...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": """You are a helpful assistant that extracts document names. Follow these rules:
1. Analyze the text to find the official document name
2. Return only the name, without any additional text
3. Preserve the original language and formatting
4. If the document has a formal title, use that
5. If there are multiple possible names, choose the most formal/official one"""},
                    {"role": "user", "content": f"Extract the official document name from this text:\n\n{text}"}
                ],
                temperature=0.1
            )
            
            if not response or not response.choices:
                raise ValueError("Empty response from OpenAI API")
            
            name = response.choices[0].message.content.strip()
            if not name:
                raise ValueError("Empty name in response")
            
            logger.info(f"Extracted document name: {name}")
            return name
            
        except Exception as e:
            logger.error(f"Error extracting document name: {str(e)}")
            raise

    def split_into_sections(self, markdown_content: str) -> List[Dict[str, str]]:
        """Split markdown content into logical sections using LLM.
        
        Args:
            markdown_content: The markdown content to split
            
        Returns:
            List of dictionaries containing section content and metadata
        """
        try:
            logger.info("Starting markdown section splitting...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": """You are a helpful assistant that splits markdown content into logical sections based on Hebrew document structure. Follow these rules:
1. Identify section breaks based on headings like 'פרק א׳:' and numbered items like '1.', '1א.', '2.', '4א.', '13.', '13א.'.
2. Treat EACH numbered or lettered item (e.g., '1.', '1א.', '(א)', '(1)') as the start of a potential section or subsection.
3. Preserve ALL original content, formatting, and Hebrew characters exactly.
4. Return sections in a structured JSON array format:
   ```json
   [
     {
       "title": "section title (e.g., 'פרק א׳: פרשנות' or '1. הגדרות')",
       "section_number": "exact section number/identifier (e.g., 'א', '1', '1א', '4א', '13א(א)')",
       "content": "FULL text content belonging ONLY to this specific section/subsection"
     },
     {
       "title": "next section title",
       "section_number": "next section number",
       "content": "next section content"
     }
   ]
   ```
5. Ensure the 'content' field contains the complete text for that specific section number/identifier ONLY, and does not bleed into the next numbered section.
6. Capture ALL distinct sections/subsections present in the original document. There should be approximately 28 sections based on the numbering.
7. Maintain the original hierarchy implicitly through the section numbers.
8. Do NOT combine multiple numbered sections into one output section."""},
                    {"role": "user", "content": f"Split this markdown content into logical sections based on its structure (פרקים, numbered items like 1., 1א., 2.). Ensure ALL content is preserved within its correct section, and capture ALL distinct numbered sections/subsections. Return the results in JSON format as described:\n\n{markdown_content}"}
                ],
                temperature=0.1 # Low temperature for deterministic output
            )
            
            if not response or not response.choices:
                raise ValueError("Empty response from OpenAI API")
            
            # Process the response
            response_text = response.choices[0].message.content
            logger.debug(f"LLM response: {response_text}")
            
            # First try to parse as JSON
            sections = self._parse_json_sections(response_text)
            
            # If JSON parsing failed, try the field marker format
            if not sections:
                sections = self._parse_field_marker_sections(response_text)
            
            if not sections:
                logger.warning("No sections were extracted from the LLM response")
                # Fallback: create a single section with all content
                sections.append({
                    'title': 'Full Document',
                    'content': markdown_content,
                    'section_number': '1'
                })
            
            logger.info(f"Extracted {len(sections)} sections from LLM response")
            return sections
            
        except Exception as e:
            logger.error(f"Error splitting markdown content: {str(e)}")
            # Fallback: return the entire content as one section
            return [{
                'title': 'Full Document',
                'content': markdown_content,
                'section_number': '1'
            }]
    
    def _parse_json_sections(self, response_text: str) -> List[Dict[str, str]]:
        """Parse sections from JSON format response."""
        import json
        import re
        
        # Try to extract JSON content using regex to handle cases where the LLM
        # might include explanatory text before or after the JSON
        json_match = re.search(r'```(?:json)?\s*(\[[\s\S]*?\])```', response_text)
        if json_match:
            json_str = json_match.group(1)
        else:
            # If no code block, try to find array directly
            json_match = re.search(r'\[\s*{[\s\S]*}\s*\]', response_text)
            if json_match:
                json_str = json_match.group(0)
            else:
                # No JSON found
                return []
        
        try:
            sections_data = json.loads(json_str)
            
            # Validate and process each section
            valid_sections = []
            for i, section in enumerate(sections_data):
                if not isinstance(section, dict):
                    logger.warning(f"Skipping non-dictionary section at index {i}")
                    continue
                
                # Ensure required fields exist
                if 'content' not in section or not section.get('content', '').strip():
                    logger.warning(f"Skipping section with missing/empty content at index {i}")
                    continue
                
                # Ensure section_number exists
                if 'section_number' not in section or not section.get('section_number', '').strip():
                    # Try to get section number from title
                    if 'title' in section and section['title']:
                        # Extract potential section number from title
                        num_match = re.search(r'^(\d+\w?)\.', section['title'])
                        if num_match:
                            section['section_number'] = num_match.group(1)
                        else:
                            section['section_number'] = str(i + 1)
                    else:
                        section['section_number'] = str(i + 1)
                
                # Ensure title exists
                if 'title' not in section or not section.get('title', '').strip():
                    section['title'] = f"Section {section['section_number']}"
                
                valid_sections.append(section)
            
            logger.info(f"Successfully parsed {len(valid_sections)} sections from JSON")
            return valid_sections
        
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON: {e}")
            return []
        except Exception as e:
            logger.warning(f"Error processing JSON sections: {e}")
            return []
    
    def _parse_field_marker_sections(self, response_text: str) -> List[Dict[str, str]]:
        """Parse sections from field marker format response."""
        # Split into sections using the separator
        section_blocks = response_text.split('---')
        sections = []
        
        for block in section_blocks:
            block = block.strip()
            if not block:
                continue
                
            current_section = {}
            lines = block.split('\n')
            content_lines = []
            is_content_section = False # Flag to know when we start reading content
            
            for line in lines:
                stripped_line = line.strip()
                if not stripped_line:
                    if is_content_section: # Preserve empty lines within content
                        content_lines.append("")
                    continue
                    
                if not is_content_section and stripped_line.startswith('title:'):
                    current_section['title'] = stripped_line[6:].strip()
                elif not is_content_section and stripped_line.startswith('content:'):
                    # Content starts here. Add the first line of content.
                    remaining = stripped_line[8:].strip()
                    if remaining:
                        content_lines.append(remaining)
                    is_content_section = True
                elif not is_content_section and stripped_line.startswith('source:'):
                    current_section['source'] = stripped_line[7:].strip()
                elif not is_content_section and stripped_line.startswith('section_number:'):
                    current_section['section_number'] = stripped_line[14:].strip()
                elif is_content_section:
                    # If we are in the content section, append the raw line
                    content_lines.append(line) # Preserve original indentation/spacing
            
            # Join all content lines, preserving original line breaks
            if content_lines:
                current_section['content'] = '\n'.join(content_lines)
            
            # Validate section has required fields
            # Ensure section_number and content are present and not empty
            if ('content' in current_section and current_section['content'].strip() and 
                'section_number' in current_section and current_section['section_number'].strip()):
                # Add default values for missing optional fields
                if 'title' not in current_section:
                    current_section['title'] = f"Section {current_section['section_number']}"
                sections.append(current_section)
            elif block: # Log if a block was processed but didn't form a valid section
                logger.warning(f"Skipping invalid section block: {block[:100]}...")
        
        logger.info(f"Parsed {len(sections)} sections from field marker format")
        return sections

    def save_split_sections(self, sections: List[Dict[str, str]], output_dir: Union[str, Path]) -> None:
        """Save split sections to individual files.
        
        Args:
            sections: List of section dictionaries
            output_dir: Directory to save the files
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get the document name from the process_file method
        document_name = getattr(self, 'document_name', '')
        source_url = getattr(self, 'source_url', '')
        
        # Generate a safe version of the document name for filenames
        safe_doc_name = document_name
        # Replace problematic characters with underscores
        for char in [':', '/', '\\', '*', '?', '"', '<', '>', '|', ' ']:
            safe_doc_name = safe_doc_name.replace(char, '_')
        # Remove extra underscores
        safe_doc_name = '_'.join(filter(None, safe_doc_name.split('_')))
        
        for i, section in enumerate(sections):
            # Generate a safe filename from section number or index
            if 'section_number' in section and section['section_number'].strip():
                # Clean the section number for filenames: replace problematic chars, keep nums/letters
                raw_num = section['section_number'].strip()
                # Replace common problematic chars with underscore
                safe_num = raw_num.replace(':', '_').replace('/', '_').replace(' ', '_').replace('(', '_').replace(')', '_').replace('[', '_').replace(']', '_')
                # Keep only letters (including Hebrew), numbers, and underscores
                safe_num = ''.join(c for c in safe_num if c.isalnum() or c == '_')
                # Avoid leading/trailing underscores and multiple consecutive underscores
                safe_num = '_'.join(filter(None, safe_num.split('_')))
                
                if safe_num: # Ensure we have a non-empty filename component
                    filename = f"{safe_doc_name}_{safe_num}.md"
                else:
                    logger.warning(f"Could not generate safe filename from section number '{raw_num}', using index.")
                    filename = f"{safe_doc_name}_{i+1:02d}.md"
            else:
                filename = f"{safe_doc_name}_{i+1:02d}.md"
            
            # Format document content in the desired structure
            content_lines = []
            
            # 1. Add the full document name with section title
            doc_title = f"*{document_name}*"
            content_lines.append(doc_title)
            content_lines.append("")  # Empty line after title
            
            # 2. Add the source reference with proper URL
            section_ref = f"מקור: סעיף {section.get('section_number', '')}"
            
            # Create section anchor for the URL based on the section number
            section_number = section.get('section_number', '').strip()
            if section_number and source_url:
                # Handle different section number formats - regular numbers, and Hebrew letters
                if section_number.isdigit() or any(c.isalpha() for c in section_number):
                    # Format might be "1", "1א", etc.
                    section_anchor = f"#סעיף_{section_number}"
                    full_url = f"{source_url}{section_anchor}"
                else:
                    # Use the base URL if we can't determine a valid anchor
                    full_url = source_url
            else:
                full_url = source_url
            
            content_lines.append(f"[{section_ref}]({full_url})")
            
            # 3. Add section title if different from document name
            if 'title' in section and section['title'] and section['title'] != document_name:
                content_lines.append(f"{section['title']}")
                content_lines.append("")  # Empty line after section title
            
            # 4. Add the content, properly formatted
            if 'content' in section and section['content'].strip():
                # Process the content to improve formatting
                content_text = section['content'].strip()
                
                # Format the content to ensure it appears as bullet points if needed
                # This preserves existing bullet points and adds bullets to regular paragraphs
                formatted_content = []
                for para in content_text.split('\n'):
                    para = para.strip()
                    if not para:
                        formatted_content.append("")  # Keep empty lines
                    elif para.startswith(('- ', '* ', '• ')):
                        formatted_content.append(para)  # Keep existing bullet points
                    elif para.startswith(('(', '1.', '2.', '3.')):
                        # For numbered items or parenthesized items, add as bullets
                        formatted_content.append(f"- {para}")
                    else:
                        # For regular paragraphs
                        formatted_content.append(para)
                
                # Add the formatted content
                content_lines.append('\n'.join(formatted_content))
            
            # Write file
            file_path = output_dir / filename
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(content_lines))
                logger.info(f"Saved section to: {file_path}")
            except OSError as e:
                logger.error(f"Error writing file {file_path}: {e}")

    def process_file(self, input_path: Union[str, Path], output_path: Optional[Union[str, Path]] = None, 
                    source_url: str = None, split: bool = False) -> None:
        """Process input file and generate markdown output.
    
        Args:
            input_path: Path to input file
            output_path: Optional path for output file
            source_url: The source URL to be embedded in the output files
            split: Whether to split the content into sections
        """
        if not source_url:
            raise ValueError("source_url is required")
        
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        # Read input file
        logger.info(f"Reading input file: {input_path}")
        with open(input_path, 'r', encoding='utf-8') as f:
            text = f.read()
        
        file_size = len(text)
        logger.info(f"Input file size: {file_size} characters")

        # Extract document name
        document_name = self.extract_document_name(text)
        logger.info(f"Using document name: {document_name}")
        
        # Store document name and source URL as instance variables
        self.document_name = document_name
        self.source_url = source_url

        # Process the text (automatically handles large files)
        if file_size > 100000:
            logger.info("File is large, processing in chunks")
            markdown = self.process_large_text(text)
        else:
            logger.info("Converting entire file to markdown")
            markdown = self.convert_text_to_markdown(text)

        if split:
            # Split into sections and save individually
            sections = self.split_into_sections(markdown)
            output_dir = output_path if output_path else input_path.parent / f"{input_path.stem}_sections"
            self.save_split_sections(sections, output_dir)
        else:
            # Write single output file
            if output_path is None:
                output_path = input_path.with_suffix('.md')
            else:
                output_path = Path(output_path)
            
            # Format the content for single file output
            content = []
            content.append(f"**{document_name}**\n")
            content.append(f"[מקור: ]({source_url})\n")
            content.append(markdown)
            
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(content))
            logger.info(f"Markdown file created: {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Convert text files to markdown using LLM')
    parser.add_argument('--input', required=True, help='Path to input text file')
    parser.add_argument('--output', help='Path to output markdown file (optional)')
    parser.add_argument('--source-url', required=True, 
                       help='Source URL to be embedded in the output files')
    parser.add_argument('--model', default='gpt-4o', help='LLM model to use')
    parser.add_argument('--temperature', type=float, default=0.3, help='Temperature for LLM generation')
    parser.add_argument('--split', action='store_true', help='Split content into sections')
    
    args = parser.parse_args()
    
    converter = TextToMarkdownConverter(model=args.model)
    converter.temperature = args.temperature
    converter.process_file(args.input, args.output, args.source_url, args.split)

if __name__ == '__main__':
    main() 
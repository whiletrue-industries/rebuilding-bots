#!/usr/bin/env python3
import argparse
import logging
import os
import json
import re
import time
from pathlib import Path
from typing import Optional, Union, List, Dict, Any, Match
import dotenv

from openai import OpenAI
from openai.types.chat import ChatCompletion

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Types
PathLike = Union[str, Path]
SectionData = Dict[str, str]

class TextToMarkdownConverter:
    def __init__(self, model: str = "gpt-4o") -> None:
        """Initialize the converter with the specified LLM model.
        
        Args:
            model: The name of the LLM model to use
        """
        self.client: OpenAI = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.model: str = model
        self.temperature: float = 0.1
        self.document_name: str = ""
        self.source_url: str = ""

    def convert_text_to_markdown(self, text: str) -> str:
        """Convert text to markdown using LLM.
        
        Args:
            text: The text to convert to markdown
            
        Returns:
            The converted markdown text
            
        Raises:
            ValueError: If the response is empty or invalid
            Exception: For other API errors
        """
        try:
            logger.info("Starting markdown conversion...")
            logger.info(f"Using model: {self.model}")
            
            # Retry logic with exponential backoff
            max_retries: int = 3
            base_delay: int = 1  # seconds
            
            for attempt in range(max_retries):
                try:
                    response: ChatCompletion = self.client.chat.completions.create(
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
                    content: str = response.choices[0].message.content
                    if not content:
                        raise ValueError("Empty content in response")
                    
                    # Save the raw response to the debug log if enabled
                    if hasattr(self, 'debug') and self.debug and hasattr(self, 'debug_log_path') and self.debug_log_path:
                        try:
                            with open(self.debug_log_path, 'a', encoding='utf-8') as debug_log_file:
                                debug_log_file.write(f"\n--- MARKDOWN CONVERSION RESPONSE at {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
                                debug_log_file.write(content)
                                debug_log_file.write("\n\n--- END OF RESPONSE ---\n\n")
                            logger.info(f"Saved raw LLM response to {self.debug_log_path}")
                        except Exception as e:
                            logger.error(f"Failed to write to debug log: {e}")
                    
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
                    error_msg: str = str(e)
                    if attempt < max_retries - 1:  # Don't retry on the last attempt
                        delay: int = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Attempt {attempt + 1} failed: {error_msg}. Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        raise  # Re-raise the last exception
                        
        except Exception as e:
            error_msg: str = str(e)
            logger.error(f"Error converting text to markdown: {error_msg}")

            raise

    def split_text(self, text: str, max_chunk_size: int = 4000) -> List[str]:
        """Split text into chunks of approximately equal size, trying to break at natural boundaries.
        
        Args:
            text: The text to split
            max_chunk_size: Maximum size of each chunk in characters
            
        Returns:
            List of text chunks
        """
        chunks: List[str] = []
        current_chunk: List[str] = []
        current_size: int = 0
        
        # Split text into paragraphs
        paragraphs: List[str] = text.split('\n\n')
        
        for para in paragraphs:
            para_size: int = len(para)
            
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
        """Process large text by splitting into chunks and combining results.
        
        Args:
            text: The large text to process
            
        Returns:
            The processed markdown text
        """
        chunks: List[str] = self.split_text(text)
        logger.info(f"Split text into {len(chunks)} chunks for processing")
        
        combined_markdown: List[str] = []
        for i, chunk in enumerate(chunks, 1):
            logger.info(f"Processing chunk {i}/{len(chunks)}")
            chunk_markdown: str = self.convert_text_to_markdown(chunk)
            combined_markdown.append(chunk_markdown)
        
        # Combine all chunks with proper spacing
        return "\n\n".join(combined_markdown)

    def extract_document_name(self, text: str) -> str:
        """Use LLM to extract the document name from the text.
        
        Args:
            text: The input text
            
        Returns:
            The extracted document name
            
        Raises:
            ValueError: If the response is empty or invalid
            Exception: For other API errors
        """
        try:
            logger.info("Extracting document name with LLM...")
            
            response: ChatCompletion = self.client.chat.completions.create(
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
            
            name: str = response.choices[0].message.content.strip()
            if not name:
                raise ValueError("Empty name in response")
            
            logger.info(f"Extracted document name: {name}")
            return name
            
        except Exception as e:
            logger.error(f"Error extracting document name: {str(e)}")
            raise

    def split_into_sections(self, markdown_content: str) -> List[SectionData]:
        """Split markdown content into logical sections using LLM.
        
        Args:
            markdown_content: The markdown content to split
            
        Returns:
            List of dictionaries containing section content and metadata
        """
        try:
            logger.info("Starting markdown section splitting...")
            
            response: ChatCompletion = self.client.chat.completions.create(
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
6. Capture ALL distinct sections/subsections present in the original document.
7. Maintain the original hierarchy implicitly through the section numbers.
8. Do NOT combine multiple numbered sections into one output section."""},
                    {"role": "user", "content": f"Split this markdown content into logical sections based on its structure (פרקים, numbered items like 1., 1א., 2.). Ensure ALL content is preserved within its correct section, and capture ALL distinct numbered sections/subsections. Return the results in JSON format as described:\n\n{markdown_content}"}
                ],
                temperature=0.1 # Low temperature for deterministic output
            )
            
            if not response or not response.choices:
                raise ValueError("Empty response from OpenAI API")
            
            # Process the response
            response_text: str = response.choices[0].message.content
            
            # Save the raw response to the debug log if enabled
            if hasattr(self, 'debug') and self.debug and hasattr(self, 'debug_log_path') and self.debug_log_path:
                try:
                    with open(self.debug_log_path, 'a', encoding='utf-8') as debug_log_file:
                        debug_log_file.write(f"\n--- SECTION SPLITTING RESPONSE at {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
                        debug_log_file.write(response_text)
                        debug_log_file.write("\n\n--- END OF RESPONSE ---\n\n")
                    logger.info(f"Saved raw LLM response to {self.debug_log_path}")
                except Exception as e:
                    logger.error(f"Failed to write to debug log: {e}")
            
            # First try to parse as JSON
            sections: List[SectionData] = self._parse_json_sections(response_text)
            
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
    
    def _parse_json_sections(self, response_text: str) -> List[SectionData]:
        """Parse sections from JSON format response.
        
        Args:
            response_text: The text response from the LLM
            
        Returns:
            List of section dictionaries
        """
        # Try to extract JSON content using regex to handle cases where the LLM
        # might include explanatory text before or after the JSON
        json_match: Optional[Match[str]] = re.search(r'```(?:json)?\s*(\[[\s\S]*?\])```', response_text)
        if json_match:
            json_str: str = json_match.group(1)
        else:
            # If no code block, try to find array directly
            json_match = re.search(r'\[\s*{[\s\S]*}\s*\]', response_text)
            if json_match:
                json_str = json_match.group(0)
            else:
                # No JSON found
                return []
        
        try:
            sections_data: List[Dict[str, Any]] = json.loads(json_str)
            
            # Validate and process each section
            valid_sections: List[SectionData] = []
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
                        num_match: Optional[Match[str]] = re.search(r'^(\d+\w?)\.', section['title'])
                        if num_match:
                            section['section_number'] = num_match.group(1)
                        else:
                            section['section_number'] = str(i + 1)
                    else:
                        section['section_number'] = str(i + 1)
                
                # Ensure title exists
                if 'title' not in section or not section.get('title', '').strip():
                    section['title'] = f"Section {section['section_number']}"
                
                # Cast to make type checker happy
                valid_section: SectionData = {
                    'title': str(section.get('title', '')),
                    'section_number': str(section.get('section_number', '')),
                    'content': str(section.get('content', ''))
                }
                
                # Add source if present
                if 'source' in section:
                    valid_section['source'] = str(section['source'])
                    
                valid_sections.append(valid_section)
            
            logger.info(f"Successfully parsed {len(valid_sections)} sections from JSON")
            return valid_sections
        
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON: {e}")
            return []
        except Exception as e:
            logger.warning(f"Error processing JSON sections: {e}")
            return []
    
    def _parse_field_marker_sections(self, response_text: str) -> List[SectionData]:
        """Parse sections from field marker format response.
        
        Args:
            response_text: The text response from the LLM
            
        Returns:
            List of section dictionaries
        """
        # Split into sections using the separator
        section_blocks: List[str] = response_text.split('---')
        sections: List[SectionData] = []
        
        for block in section_blocks:
            block = block.strip()
            if not block:
                continue
                
            current_section: Dict[str, str] = {}
            lines: List[str] = block.split('\n')
            content_lines: List[str] = []
            is_content_section: bool = False # Flag to know when we start reading content
            
            for line in lines:
                stripped_line: str = line.strip()
                if not stripped_line:
                    if is_content_section: # Preserve empty lines within content
                        content_lines.append("")
                    continue
                    
                if not is_content_section and stripped_line.startswith('title:'):
                    current_section['title'] = stripped_line[6:].strip()
                elif not is_content_section and stripped_line.startswith('content:'):
                    # Content starts here. Add the first line of content.
                    remaining: str = stripped_line[8:].strip()
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

    def _sanitize_for_filename(self, text: str) -> str:
        """Convert text to a filesystem-safe filename component.
        
        Args:
            text: The text to sanitize
            
        Returns:
            A filesystem-safe version of the text
        """
        if not text or not text.strip():
            return ""
            
        # Replace problematic characters with underscores
        safe_text: str = text.strip()
        for char in [':', '/', '\\', '*', '?', '"', '<', '>', '|', ' ']:
            safe_text = safe_text.replace(char, '_')
            
        # Keep only letters (including Hebrew), numbers, and underscores
        safe_text = ''.join(c for c in safe_text if c.isalnum() or c == '_')
            
        # Avoid leading/trailing underscores and multiple consecutive underscores
        safe_text = '_'.join(filter(None, safe_text.split('_')))
            
        return safe_text

    def _format_content(self, content_text: str) -> str:
        """Format content text with proper bullet points and structure.
        
        Args:
            content_text: The raw content text to format
            
        Returns:
            Formatted content text
        """
        formatted_content: List[str] = []
        
        # Format the content to ensure it appears as bullet points if needed
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
        
        return '\n'.join(formatted_content)

    def save_split_sections(self, sections: List[SectionData], output_dir: PathLike) -> None:
        """Save split sections to individual files.
        
        Args:
            sections: List of section dictionaries
            output_dir: Directory to save the files
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get the document name from the process_file method
        document_name: str = getattr(self, 'document_name', '')
        source_url: str = getattr(self, 'source_url', '')
        
        # Generate a safe version of the document name for filenames
        safe_doc_name: str = self._sanitize_for_filename(document_name)
        
        for i, section in enumerate(sections):
            # Generate a safe filename from section number or index
            filename: str = self._generate_filename(section, i, safe_doc_name)
            
            # Format document content in the desired structure
            content_lines: List[str] = self._prepare_section_content(section, document_name, source_url)
            
            # Write file
            file_path: Path = output_dir / filename
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(content_lines))
                logger.info(f"Saved section to: {file_path}")
            except OSError as e:
                logger.error(f"Error writing file {file_path}: {e}")

    def _generate_filename(self, section: SectionData, index: int, safe_doc_name: str) -> str:
        """Generate a safe filename for a section.
        
        Args:
            section: The section data
            index: The section index (for fallback)
            safe_doc_name: The sanitized document name
            
        Returns:
            A safe filename for the section
        """
        if 'section_number' in section and section['section_number'].strip():
            # Clean the section number for filenames
            raw_num: str = section['section_number'].strip()
            safe_num: str = self._sanitize_for_filename(raw_num)
            
            if safe_num: # Ensure we have a non-empty filename component
                filename: str = f"{safe_doc_name}_{safe_num}.md" if safe_doc_name else f"{safe_num}.md"
            else:
                logger.warning(f"Could not generate safe filename from section number '{raw_num}', using index.")
                filename = f"{safe_doc_name}_{index+1:02d}.md" if safe_doc_name else f"section_{index+1:02d}.md"
        else:
            filename = f"{safe_doc_name}_{index+1:02d}.md" if safe_doc_name else f"section_{index+1:02d}.md"
            
        return filename

    def _prepare_section_content(self, section: SectionData, document_name: str, source_url: str) -> List[str]:
        """Prepare the content for a section file.
        
        Args:
            section: The section data
            document_name: The document name
            source_url: The source URL
            
        Returns:
            List of lines for the section content
        """
        content_lines: List[str] = []
        
        # 1. Add the full document name with section title
        doc_title: str = f"**{document_name}**"
        content_lines.append(doc_title)
        content_lines.append("")  # Empty line after title
        
        # 2. Add the source reference with proper URL
        section_ref: str = f"מקור: סעיף {section.get('section_number', '')}"
        full_url: str = self._generate_section_url(section.get('section_number', ''), source_url)
        content_lines.append(f"[{section_ref}]({full_url})")
        
        # 3. Add section title if different from document name
        if 'title' in section and section['title'] and section['title'] != document_name:
            content_lines.append(f"{section['title']}")
            content_lines.append("")  # Empty line after section title
        
        # 4. Add the content, properly formatted
        if 'content' in section and section['content'].strip():
            # Format the content
            formatted_content: str = self._format_content(section['content'].strip())
            content_lines.append(formatted_content)
        
        return content_lines

    def _generate_section_url(self, section_number: str, source_url: str) -> str:
        """Generate a URL for a section.
        
        Args:
            section_number: The section number
            source_url: The base source URL
            
        Returns:
            The complete URL for the section
        """
        section_number = section_number.strip()
        if section_number and source_url:
            # Handle different section number formats - regular numbers, and Hebrew letters
            if section_number.isdigit() or any(c.isalpha() for c in section_number):
                # Format might be "1", "1א", etc.
                section_anchor: str = f"#סעיף_{section_number}"
                full_url: str = f"{source_url}{section_anchor}"
            else:
                # Use the base URL if we can't determine a valid anchor
                full_url = source_url
        else:
            full_url = source_url
            
        return full_url

    def process_file(self, input_path: PathLike, output_path: Optional[PathLike] = None, 
                    source_url: Optional[str] = None, split: bool = False, debug: bool = False) -> None:
        """Process input file and generate markdown output.
    
        Args:
            input_path: Path to input file
            output_path: Optional path for output file
            source_url: The source URL to be embedded in the output files
            split: Whether to split the content into sections
            debug: Whether to save raw LLM responses to a log file
            
        Raises:
            ValueError: If source_url is not provided
            FileNotFoundError: If the input file is not found
        """
        if not source_url:
            raise ValueError("source_url is required")
        
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        # Create debug log file if debug mode is enabled
        self.debug = debug
        self.debug_log_path = None
        if debug:
            self.debug_log_path = input_path.with_suffix('.log')
            logger.info(f"Debug mode enabled. Raw LLM responses will be saved to: {self.debug_log_path}")
            # Initialize the log file
            with open(self.debug_log_path, 'w', encoding='utf-8') as f:
                f.write(f"Debug log for {input_path.name} created at {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Read input file
        logger.info(f"Reading input file: {input_path}")
        with open(input_path, 'r', encoding='utf-8') as f:
            text: str = f.read()
        
        file_size: int = len(text)
        logger.info(f"Input file size: {file_size} characters")

        # Extract document name
        document_name: str = self.extract_document_name(text)
        logger.info(f"Using document name: {document_name}")
        
        # Store document name and source URL as instance variables
        self.document_name = document_name
        self.source_url = source_url

        # Process the text (automatically handles large files)
        markdown: str
        if file_size > 100000:
            logger.info("File is large, processing in chunks")
            markdown = self.process_large_text(text)
        else:
            logger.info("Converting entire file to markdown")
            markdown = self.convert_text_to_markdown(text)

        if split:
            # Split into sections and save individually
            sections: List[SectionData] = self.split_into_sections(markdown)
            output_dir: Path
            if output_path:
                output_dir = Path(output_path)
            else:
                output_dir = input_path.parent / f"{input_path.stem}_sections"
            self.save_split_sections(sections, output_dir)
        else:
            # Write single output file
            if output_path is None:
                output_path = input_path.with_suffix('.md')
            else:
                output_path = Path(output_path)
            
            # Format the content for single file output
            content: List[str] = []
            content.append(f"**{document_name}**\n")
            content.append(f"[מקור: ]({source_url})\n")
            content.append(markdown)
            
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(content))
            logger.info(f"Markdown file created: {output_path}")

def main() -> None:
    """Entry point for the script."""
    parser = argparse.ArgumentParser(description='Convert text files to markdown using LLM')
    parser.add_argument('--input', required=True, help='Path to input text file')
    parser.add_argument('--output', help='Path to output markdown file (optional)')
    parser.add_argument('--source-url', required=True, 
                       help='Source URL to be embedded in the output files')
    parser.add_argument('--model', default='gpt-4o', help='LLM model to use')
    parser.add_argument('--temperature', type=float, default=0.1, help='Temperature for LLM generation')
    parser.add_argument('--split', action='store_true', help='Split content into sections')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode to save raw LLM responses')
    
    args = parser.parse_args()
    
    converter = TextToMarkdownConverter(model=args.model)
    converter.temperature = args.temperature
    converter.process_file(args.input, args.output, args.source_url, args.split, args.debug)

if __name__ == '__main__':
    main() 
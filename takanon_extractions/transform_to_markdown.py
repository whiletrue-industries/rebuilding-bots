#!/usr/bin/env python3
import argparse
import logging
import os
from pathlib import Path
from typing import Optional, Union, List
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

    def process_file(self, input_path: Union[str, Path], output_path: Optional[Union[str, Path]] = None, 
                    source_url: str = None) -> None:
        """Process input file and generate markdown output.
    
        Args:
            input_path: Path to input file
            output_path: Optional path for output file
            source_url: The source URL to be embedded in the output files
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

        # Process the text (automatically handles large files)
        if file_size > 100000:
            logger.info("File is large, processing in chunks")
            markdown = self.process_large_text(text)
        else:
            logger.info("Converting entire file to markdown")
            markdown = self.convert_text_to_markdown(text)

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
    
    args = parser.parse_args()
    
    converter = TextToMarkdownConverter(model=args.model)
    converter.temperature = args.temperature
    converter.process_file(args.input, args.output, args.source_url)

if __name__ == '__main__':
    main() 
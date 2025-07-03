from openai import OpenAI
import json
import os
from pathlib import Path
import dotenv
import argparse
from pydantic import BaseModel
from typing import List
from botnim.config import get_logger, DEFAULT_ENVIRONMENT

# Load environment variables
ROOT = Path(__file__).parent.parent.parent
dotenv.load_dotenv(ROOT / '.env')

# Logger setup
logger = get_logger(__name__)

# Define Pydantic models for structure extraction
class StructureItem(BaseModel):
    depth: int
    section_name: str

class StructureResponse(BaseModel):
    items: List[StructureItem]

def get_openai_client(environment=DEFAULT_ENVIRONMENT):
    if environment == "production":
        api_key = os.environ.get('OPENAI_API_KEY_PRODUCTION')
        key_name = 'OPENAI_API_KEY_PRODUCTION'
    else:
        api_key = os.environ.get('OPENAI_API_KEY_STAGING')
        key_name = 'OPENAI_API_KEY_STAGING'
    
    if not api_key:
        logger.error(f"{key_name} environment variable not set. LLM calls will fail.")
        raise ValueError(f"Missing required environment variable: {key_name}")
    
    logger.info(f"Using OpenAI API key from {key_name} (length: {len(api_key)} chars)")
    return OpenAI(api_key=api_key, timeout=120.0)

def extract_structure_from_markdown(markdown_text: str, client: OpenAI, model: str = "gpt-4o-2024-08-06", max_tokens: int = 16384) -> List[StructureItem]:
    """
    Extract structural elements from markdown using OpenAI API.
    """
    logger.info("Sending markdown to LLM for structure extraction")
    logger.info(f"Input text length: {len(markdown_text)} characters")
    logger.info(f"Using model: {model}")
    logger.info(f"Max tokens: {max_tokens}")
    
    # Show preview of content being sent
    preview = (markdown_text[:200] + '...') if len(markdown_text) > 200 else markdown_text


    # Warn about large content
    if len(markdown_text) > 100000:
        logger.warning(f"Large input detected ({len(markdown_text)} chars). This may take longer or fail.")
    
    try:
        response = client.beta.chat.completions.parse(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are analyzing pre-filtered structural lines from a legal/regulatory document.\n"
                        "Your task: Extract the complete hierarchical structure, with special attention to identifying individual CLAUSES.\n\n"
                        "CRITICAL: Distinguish between TABLE OF CONTENTS and ACTUAL CONTENT:\n"
                        "- 'תוכן עניינים' (Table of Contents) is just a navigation section\n"
                        "- Main document parts like 'חלק א׳', 'חלק ב׳' are ACTUAL content sections\n"
                        "- Do NOT nest main content parts under 'תוכן עניינים'\n"
                        "- Main parts should be at the same hierarchy level as 'תוכן עניינים'\n\n"
                        "For each structural element, determine:\n"
                        "- 'depth': hierarchy level based on your analysis of the content structure\n"
                        "- 'section_name': clean descriptive name extracted from the content\n\n"
                        "Document Structure Guidelines:\n"
                        "- Main document divisions (חלק א׳, חלק ב׳, etc.) should be at depth 1\n"
                        "- Chapters within parts (פרק ראשון, פרק שני, etc.) should be at depth 2\n"
                        "- Individual CLAUSES/ARTICLES (סעיף 1, סעיף 2, etc.) should be at depth 3\n"
                        "- Sub-clauses and provisions within clauses should be at depth 4+\n\n"
                        "Content Pattern Analysis:\n"
                        "- Look for numbering systems (Arabic numerals, Hebrew numerals, letters)\n"
                        "- Identify structural markers (סעיף, תקנה, חלק, פרק, etc.)\n"
                        "- Pay special attention to individual clauses - these are the most important granular elements\n"
                        "- Distinguish between navigation links and actual content sections\n\n"
                        "Return ALL provided lines - don't filter any out. Every structural element matters."
                    )
                },
                {
                    "role": "user",
                    "content": f"""```markdown\n{markdown_text}\n```"""
                }
            ],
            response_format=StructureResponse,
            max_tokens=max_tokens
        )
        logger.info("LLM API call completed successfully")
        
    except Exception as e:
        logger.error(f"OpenAI API call failed: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        raise

    parsed_response = response.choices[0].message.parsed
    if parsed_response is None:
        logger.error("Failed to parse structure response")
        return []

    logger.info(f"LLM returned {len(parsed_response.items)} structured elements")
    return parsed_response.items


def build_nested_structure(flat_items: List[StructureItem]) -> List[dict]:
    """
    Convert flat list of structure items with depth indicators into nested tree structure.
    """
    if not flat_items:
        return []
    
    # Convert to dictionaries with children arrays
    nested_items = []
    stack = []  # Stack to keep track of parent nodes at each depth level
    
    for item in flat_items:
        # Create the current node
        current_node = {
            "depth": item.depth,
            "section_name": item.section_name,
            "children": []
        }
        
        # Remove items from stack that are at same or deeper level
        while stack and stack[-1]["depth"] >= item.depth:
            stack.pop()
        
        # If stack is empty, this is a root level item
        if not stack:
            nested_items.append(current_node)
        else:
            # Add as child to the last item in stack
            stack[-1]["children"].append(current_node)
        
        # Push current node to stack
        stack.append(current_node)
    
    return nested_items


def flatten_for_json_serialization(nested_items: List[dict]) -> List[dict]:
    """
    Flatten nested structure for JSON serialization while preserving hierarchy.
    Each item includes its children inline.
    """
    def process_node(node):
        result = {
            "depth": node["depth"],
            "section_name": node["section_name"]
        }
        if node["children"]:
            result["children"] = [process_node(child) for child in node["children"]]
        return result
    
    return [process_node(item) for item in nested_items]


def main():
    """
    CLI interface for extracting structure from markdown files.
    """
    parser = argparse.ArgumentParser(
        description="Extract hierarchical structure from markdown using OpenAI API"
    )
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to input markdown file"
    )
    parser.add_argument(
        "output_file", 
        type=str,
        help="Path to output JSON file"
    )
    parser.add_argument(
        "--environment",
        choices=["staging", "production"],
        default=DEFAULT_ENVIRONMENT,
        help=f"Environment to use (default: {DEFAULT_ENVIRONMENT})"
    )
    parser.add_argument(
        "--model",
        default="gpt-4.1",
        help="OpenAI model to use"
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=16384,
        help="Maximum tokens for OpenAI response (default: 16384)"
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty print JSON output"
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    input_path = Path(args.input_file)
    if not input_path.exists():
        logger.error(f"Input file does not exist: {args.input_file}")
        return 1
    
    # Read input markdown file
    try:
        logger.info(f"Reading input file: {args.input_file}")
        with open(input_path, 'r', encoding='utf-8') as f:
            markdown_text = f.read()
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        return 1
    
    # Get OpenAI client
    try:
        client = get_openai_client(args.environment)
    except Exception as e:
        logger.error(f"Failed to initialize OpenAI client: {e}")
        return 1
    
    # Extract structure
    try:
        logger.info("Extracting structure from markdown")
        structure_items = extract_structure_from_markdown(markdown_text, client, args.model, args.max_tokens)
        
        # Build nested tree structure
        logger.info("Building nested tree structure")
        nested_structure = build_nested_structure(structure_items)
        
        # Convert to JSON-serializable format
        structure_data = flatten_for_json_serialization(nested_structure)
        
        # Prepare output data with metadata
        output_data = {
            "metadata": {
                "input_file": str(input_path),
                "environment": args.environment,
                "model": args.model,
                "max_tokens": args.max_tokens,
                "total_items": len(structure_items),
                "structure_type": "nested_hierarchy"
            },
            "structure": structure_data
        }
        
    except Exception as e:
        logger.error(f"Failed to extract structure: {e}")
        return 1
    
    # Write output JSON file
    try:
        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing output to: {args.output_file}")
        with open(output_path, 'w', encoding='utf-8') as f:
            if args.pretty:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            else:
                json.dump(output_data, f, ensure_ascii=False)
                
        logger.info(f"Successfully processed {len(structure_items)} structure items into nested hierarchy")
        logger.info(f"Output saved to: {args.output_file}")
        
    except Exception as e:
        logger.error(f"Failed to write output file: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

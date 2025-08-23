#!/usr/bin/env python3
"""
Extract hierarchical structure from HTML using OpenAI API
"""

from openai import OpenAI
import os
from pydantic import BaseModel
from typing import List, Optional
from botnim.config import get_logger, DEFAULT_ENVIRONMENT

# Logger setup
logger = get_logger(__name__)

# Define Pydantic models for structure extraction
class StructureItem(BaseModel):
    depth: int
    section_name: str
    html_id: Optional[str] = None
    section_type: Optional[str] = None

class StructureResponse(BaseModel):
    items: List[StructureItem]

def extract_structure_from_html(html_text: str, client: OpenAI, model: str, max_tokens: Optional[int], mark_type: str = None) -> List[StructureItem]:
    """
    Extract structural elements from HTML using OpenAI API.
    """
    logger.info("Sending HTML to LLM for structure extraction")
    logger.info(f"Input text length: {len(html_text)} characters")
    logger.info(f"Using model: {model}")
    logger.info(f"Max tokens: {max_tokens}")
    if mark_type:
        logger.info(f"Mark type for extraction: {mark_type}")
    
    # Warn about large content
    if len(html_text) > 100000:
        logger.warning(f"Large input detected ({len(html_text)} chars). This may take longer or fail.")
    
    # Build system prompt
    system_prompt = (
        "You are analyzing the raw HTML of a legal/regulatory document.\n"
        "Your task: Extract the complete hierarchical structure, with special attention to identifying individual CLAUSES.\n\n"
        "CRITICAL: Distinguish between TABLE OF CONTENTS and ACTUAL CONTENT:\n"
        "- 'תוכן עניינים' (Table of Contents) is just a navigation section\n"
        "- Main document parts like 'חלק א׳', 'חלק ב׳' are ACTUAL content sections\n"
        "- Do NOT nest main content parts under 'תוכן עניינים'\n"
        "- Main parts should be at the same hierarchy level as 'תוכן עניינים'\n\n"
        "For each structural element, determine:\n"
        "- 'depth': hierarchy level based on your analysis of the content structure\n"
        "- 'section_name': clean descriptive name extracted from the content\n"
        "- 'section_type': a consistent Hebrew label describing the type of section. Use only the following values: 'תוכן עניינים', 'חלק', 'פרק', 'סימן', 'סעיף', 'תת-סעיף'. For example, use 'חלק' for main parts (e.g., 'חלק א׳'), 'פרק' for chapters (e.g., 'פרק ראשון'), 'סעיף' for clauses (e.g., 'סעיף 1'), 'סימן' for sub-divisions, 'תוכן עניינים' for table of contents, and 'תת-סעיף' for sub-clauses. Be consistent throughout the document.\n\n"
        "Content Pattern Analysis:\n"
        "- Look for numbering systems (Arabic numerals, Hebrew numerals, letters)\n"
        "- Identify structural markers (סעיף, תקנה, חלק, פרק, סימן, תת-סעיף, תוכן עניינים, etc.)\n"
        "- Pay special attention to individual clauses - these are the most important granular elements\n"
        "- Distinguish between navigation links and actual content sections\n\n"
        "Return ALL provided lines - don't filter any out. Every structural element matters.\n"
        "The input will be raw HTML. Parse the HTML structure and use it to inform your hierarchy.\n"
    )
    if mark_type:
        system_prompt += (
            f"\nIMPORTANT: The user wants to mark all content of type: '{mark_type}'. "
            "For every structural element that matches this type, add a key 'html_id' to the output JSON, "
            "with the value being the HTML id attribute of the relevant block (if present in the HTML). "
            "If the block does not have an id, set 'html_id' to null. "
            "This instruction applies regardless of the hierarchy level or the specific content type the user requests."
        )

    try:
        api_kwargs = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"""```html\n{html_text}\n```"""}
            ],
            "response_format": StructureResponse,
            "temperature": 0.0
        }
        if max_tokens is not None:
            api_kwargs["max_tokens"] = max_tokens
        response = client.beta.chat.completions.parse(**api_kwargs)
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
    Preserves all fields from StructureItem (e.g., html_id).
    """
    if not flat_items:
        return []
    
    nested_items = []
    stack = []  # Stack to keep track of parent nodes at each depth level
    
    for item in flat_items:
        # Convert the StructureItem to a dict, including all fields
        current_node = item.model_dump()
        current_node["children"] = []
        
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

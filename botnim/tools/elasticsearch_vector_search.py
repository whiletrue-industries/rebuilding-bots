from typing import Dict
import yaml
from pathlib import Path
from ..config import SPECS
from botnim.query import QueryClient, run_query
import logging

logger = logging.getLogger(__name__)

BOT_NAME_MAPPING = {
    'takanon': 'בוט תקנון הכנסת',
    'budgetkey': 'בוט התקציב הפתוח'
}

def create_elastic_search_tool(bot_name: str, context_name: str, environment: str) -> Dict:
    """Creates an Elasticsearch vector search tool configuration for a specific context"""
    
    # Load the bot's config to get context details
    config_path = SPECS / bot_name / 'config.yaml'
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Find the specific context configuration to get its slug
    context_config = next(
        (ctx for ctx in config.get('context', []) if ctx['name'] == context_name),
        {'slug': context_name.lower().replace(' ', '_')}  # fallback to sanitized name
    )
    
    # Use slugs for the tool name
    tool_name = f"ElasticVectorSearch_{bot_name}_{context_config['slug']}"
    
    return {
        "type": "function",
        "function": {
            "name": tool_name,
            "description": context_config.get('description', 
                f"Search the {config['name']}'s {context_name} knowledge base using semantic search"),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": context_config.get('search_description', 
                            "The search query text")
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Number of results to return",
                        "default": 7
                    }
                },
                "required": ["query"]
            }
        }
    }

def elastic_vector_search_handler(environment: str, bot_name: str, context_name: str, query: str, num_results: int = 3) -> str:
    """Handles Elasticsearch vector search requests from the assistant"""
    logger.info(f"Running elastic_vector_search_handler with query: {query}, num_results: {num_results}")
    results = run_query(query, environment, bot_name, context_name, num_results, format="dict")
    
    # Log the results
    logger.info(f"Search results: {results}")
    
    # Format results for the assistant
    formatted_results = []
    for result in results:
        formatted_results.append(
            f"[Score: {result.score:.2f}]\n"
            f"ID: {result.id}\n"
            f"Content:\n{result.full_content}\n"
            f"{'-' * 40}"
        )

    return "\n\n".join(formatted_results)
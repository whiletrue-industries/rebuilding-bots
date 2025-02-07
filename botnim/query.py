import os
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv
from openai import OpenAI
from botnim.vector_store.vector_store_es import VectorStoreES
from botnim.config import get_logger
import argparse
import sys
import yaml

logger = get_logger(__name__)
load_dotenv()



class QueryClient:
    """Class to handle vector store queries"""
    def __init__(self, bot_name: str):
        self.bot_name = bot_name
        self.config = self._load_config()
        self.vector_store = self._initialize_vector_store()

    def _load_config(self) -> dict:
        """Load configuration from the specs directory"""
        specs_dir = Path(__file__).parent.parent / 'specs' / self.bot_name / 'config.yaml'
        if not specs_dir.exists():
            logger.warning(f"No config found for {self.bot_name}, using default config")
            return {"name": f"{self.bot_name}_assistant"}
            
        with open(specs_dir) as f:
            return yaml.safe_load(f)
        
    def _initialize_vector_store(self) -> VectorStoreES:
        """Initialize the vector store connection"""
        return VectorStoreES(
            config=self.config,  # Use the config from the QueryClient instance
            es_host=os.getenv('ES_HOST', 'https://localhost:9200'),
            es_username=os.getenv('ES_USERNAME', 'elastic'),
            es_password=os.getenv('ES_PASSWORD'),
            verify_certs=False  # TODO: change to True when in production to use the production index
        )

    def _build_search_query(self, query_text: str, embedding: List[float]) -> Dict[str, Any]:
        """Build the hybrid search query"""

        text_match = {
            "multi_match": {            ## you can define here the fields you want to search in
                "query": query_text,
                "fields": ["content"],
                "boost": 0.2,          ## you can define here the boost for the text match vs. vector match
                "type": 'best_fields',  ## you can define here the type of search: cross_fields, bool, simple, phrase, phrase_prefix
                "operator": 'or',       ## you can define here the operator: or, and 
            }
        }
        
        vector_match = {
            "knn": {
                "field": "vector",  # the field we want to search in
                "query_vector": embedding,  # the embedding we want to search for
                "k": 7,  # the number of results we want to get
                "num_candidates": 20,  # the number of candidates we want to consider
                "boost": 0.5  # the boost for the vector match
            }
        }
        
        return {
            "bool": {
                "should": [text_match, vector_match],
                "minimum_should_match": 1,
            }
        }
    
    

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

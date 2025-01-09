import logging
from pathlib import Path
import requests
import io
from typing import List, Union, BinaryIO, Tuple
from .base import KnowledgeBase

logger = logging.getLogger(__name__)

class ContextManager:
    def __init__(self, config_dir: Path, kb_backend: KnowledgeBase):
        self.config_dir = config_dir
        self.kb_backend = kb_backend

    def process_context(self, context_config: dict, replace: bool = False) -> Tuple[str, str]:
        """Process a context configuration and return (vector_store_id, assistant_id)"""
        kb_name = context_config['name']
        exists, vector_store_id, assistant_id = self.kb_backend.exists(kb_name)

        if exists:
            if replace:
                logger.info(f"Deleting existing knowledge base: {kb_name}")
                self.kb_backend.delete(assistant_id)
                # Create new vector store and get new IDs
                vector_store_id, assistant_id = self.kb_backend.create(kb_name)
            else:
                logger.info(f"Using existing assistant, creating new vector store for: {kb_name}")
                # Create new vector store but keep existing assistant
                vector_store_id, assistant_id = self.kb_backend.create(kb_name)
        else:
            vector_store_id, assistant_id = self.kb_backend.create(kb_name)
        
        return vector_store_id, assistant_id

    def _process_files(self, file_pattern: str) -> List[BinaryIO]:
        """Process regular files matching the pattern"""
        files = list(self.config_dir.glob(file_pattern))
        # Verify files have supported extensions
        supported_extensions = {'.txt', '.md', '.pdf', '.doc', '.docx'}
        valid_files = [f for f in files if f.suffix.lower() in supported_extensions]
        if len(valid_files) < len(files):
            logger.warning(f"Skipping files without supported extensions. Supported: {supported_extensions}")
        return [f.open('rb') for f in valid_files]

    def _process_split_file(self, context_config: dict) -> List[Tuple[str, BinaryIO, str]]:
        """Process a split file"""
        filename = self.config_dir / context_config['split']
            
        if not filename.exists():
            logger.warning(f"Split file not found: {filename}")
            return []

        content = filename.read_text().split('\n---\n')
        documents = []
        
        for i, c in enumerate(content):
            if c.strip():
                file_stream = io.BytesIO(c.strip().encode('utf-8'))
                documents.append((
                    f'ידע_נוסף_{i:03d}.md',  # Using .md extension for markdown content
                    file_stream,
                    'text/markdown'  # Changed content type to markdown
                ))
            else:
                logger.debug(f'Skipping empty section {i} in split file')
                
        return documents

    def collect_documents(self, context_config: dict) -> List[Union[BinaryIO, Tuple[str, BinaryIO, str]]]:
        """Collect documents from a context configuration without creating a knowledge base"""
        documents = []
        
        # Process regular files
        if 'files' in context_config:
            documents.extend(self._process_files(context_config['files']))

        # Process split files (e.g., common knowledge)
        if 'split' in context_config:
            split_docs = self._process_split_file(context_config)
            if split_docs:
                documents.extend(split_docs)
            
        return documents

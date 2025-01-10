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

    def _add_environment_suffix(self, name: str) -> str:
        """Add environment suffix if not in production"""
        if not self.kb_backend.production:
            name_parts = name.rsplit('.', 1)
            return f"{name_parts[0]} - פיתוח.{name_parts[1]}" if len(name_parts) > 1 else f"{name} - פיתוח"
        return name

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
        
        # Return files with environment-specific names
        return [(self._add_environment_suffix(f.name), f.open('rb'), 'text/plain') for f in valid_files]

    def _process_split_file(self, context_config: dict) -> List[Tuple[str, BinaryIO, str]]:
        """Process a directory of split files"""
        dir_path = self.config_dir / context_config['split'].replace('.txt', '')
        
        if not dir_path.exists():
            logger.warning(f"Split directory not found: {dir_path}")
            return []

        documents = []
        for file_path in sorted(dir_path.glob('*.md')):
            if file_path.read_text().strip():  # Skip empty files
                env_filename = self._add_environment_suffix(file_path.name)
                documents.append((
                    env_filename,
                    file_path.open('rb'),
                    'text/markdown'
                ))
            else:
                logger.debug(f'Skipping empty file: {file_path}')
        
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

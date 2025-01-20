from pathlib import Path
import requests
import io
from typing import List, Union, BinaryIO, Tuple
from .base import VectorStore
from ..config import get_logger

logger = get_logger(__name__)

class ContextManager:
    def __init__(self, config_dir: Path, kb_backend: VectorStore):
        self.config_dir = config_dir
        self.kb_backend = kb_backend

    def _add_environment_suffix(self, name: str) -> str:
        """Add environment suffix if not in production"""
        if not self.kb_backend.production:
            name_parts = name.rsplit('.', 1)
            return f"{name_parts[0]} - פיתוח.{name_parts[1]}" if len(name_parts) > 1 else f"{name} - פיתוח"
        return name

    def create_vector_store(self, context_config: dict) -> str:
        """Create a vector store for the given context configuration
        
        Args:
            context_config: Configuration dictionary for the context
            
        Returns:
            str: ID of the created vector store
        """
        kb_name = context_config['name']
        vector_store_id = self.kb_backend.create(self._add_environment_suffix(kb_name))
        logger.info(f"Created vector store '{kb_name}' with ID: {vector_store_id}")
        return vector_store_id

    def _process_files(self, file_pattern: str) -> List[Tuple[str, str, str]]:
        """Process markdown files matching the pattern"""
        files = sorted(self.config_dir.glob(file_pattern))
        valid_files = [f for f in files if f.suffix.lower() == '.md']
        if len(valid_files) < len(files):
            logger.warning(f"Skipping non-markdown files. Only .md files are currently supported.")
        
        return [(
            self._add_environment_suffix(f.name), 
            str(f), 
            'text/markdown'
        ) for f in valid_files]

    def _process_split_file(self, context_config: dict) -> List[Tuple[str, BinaryIO, str]]:
        """Process a directory of split files
        
        This method supports a different way of organizing knowledge base content,
        where information is split into multiple markdown files. Each file in the
        specified directory becomes a separate entry in the knowledge base.
        
        This is useful when:
        - Content is manually curated
        - Information is naturally split into distinct files
        - Content comes from multiple sources
        
        Args:
            context_config: Configuration dictionary containing:
                - split: Path to directory containing split markdown files
                
        Returns:
            List of tuples (filename, file_handle, content_type) for each markdown file
        
        Example config:
            context:
              - name: Manual Knowledge
                split: manual_kb    # Directory containing .md files
        """
        dir_path = self.config_dir / context_config['split']
        
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
        """Collect documents from all configured sources
        
        Supports multiple source types:
        1. Spreadsheet source (type: spreadsheet)
        2. Split files (split: directory_path)
        3. Regular files (files: glob_pattern)
        
        Args:
            context_config: Configuration dictionary specifying the source type and location
            
        Returns:
            List of documents in the format required by the vector store
        """
        documents = []
        
        # Handle spreadsheet source
        if context_config.get('type') == 'spreadsheet' and 'source' in context_config:
            from .download_sources import download_and_convert_spreadsheet
            documents.extend([
                (self._add_environment_suffix(filename), file_obj, content_type)
                for filename, file_obj, content_type 
                in download_and_convert_spreadsheet(
                    context_config['source'],
                    context_config['name']
                )
            ])
        
        # Handle split files
        elif 'split' in context_config:
            documents.extend(self._process_split_file(context_config))
        
        # Handle regular files
        elif 'files' in context_config:
            documents.extend(self._process_files(context_config['files']))
        
        return documents

    def _process_directory(self, dir_path: Path) -> List[Tuple[str, str, str]]:
        """Process all markdown files in a directory"""
        documents = []
        for file_path in sorted(dir_path.glob('*.md')):
            if file_path.read_text().strip():
                env_filename = self._add_environment_suffix(file_path.name)
                documents.append((
                    env_filename,
                    str(file_path),
                    'text/markdown'
                ))
        return documents

    def setup_contexts(self, contexts: list) -> str:
        """Set up all contexts and return the vector store ID
        
        Args:
            contexts: List of context configurations
            
        Returns:
            str: ID of the created vector store
        """
        if not contexts:
            return None
        
        # Create vector store using the first context's name
        vector_store_id = self.create_vector_store(contexts[0])
        
        # Collect and upload documents from all contexts
        all_documents = []
        for context in contexts:
            documents = self.collect_documents(context)
            if documents:
                all_documents.extend(documents)
            else:
                logger.warning(f"No documents found for context: {context.get('name', 'unnamed')}")
        
        if all_documents:
            self.kb_backend.upload_documents(vector_store_id, all_documents)
        else:
            logger.warning("No documents found in any context")
        
        return vector_store_id

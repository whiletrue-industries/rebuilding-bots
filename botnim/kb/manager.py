from pathlib import Path
import requests
import io
from typing import List, Union, BinaryIO, Tuple
from .base import VectorStore
from .download_sources import download_and_convert_spreadsheet
from ..config import get_logger

logger = get_logger(__name__)

class ContextManager:
    def __init__(self, config_dir: Path, vs_backend: VectorStore):
        """Initialize the context manager
        
        Args:
            config_dir: Directory containing bot configuration and files
            vs_backend: Vector store backend for storing and retrieving documents
        """
        self.config_dir = config_dir
        self.vs_backend = vs_backend

    def _add_environment_suffix(self, name: str) -> str:
        """Add environment suffix if not in production"""
        if not self.vs_backend.production:
            name_parts = name.rsplit('.', 1)
            return f"{name_parts[0]} - פיתוח.{name_parts[1]}" if len(name_parts) > 1 else f"{name} - פיתוח"
        return name

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
        """Collect documents from a single context source
        
        Args:
            context_config: Configuration dictionary specifying the source type and location
            
        Returns:
            List of documents in the format required by the vector store
        """
        # Handle spreadsheet source
        if context_config.get('type') == 'spreadsheet' and 'source' in context_config:
            return [
                (self._add_environment_suffix(filename), file_obj, content_type)
                for filename, file_obj, content_type 
                in download_and_convert_spreadsheet(
                    context_config['source'],
                    context_config['name']
                )
            ]
        
        # Handle split files
        elif 'split' in context_config:
            return self._process_split_file(context_config)
        
        # Handle regular files
        elif 'files' in context_config:
            return self._process_files(context_config['files'])
            
        return []

    def setup_contexts(self, contexts: list) -> dict:
        """Collect documents from all contexts and let backend handle organization
        
        Args:
            contexts: List of context configurations
            
        Returns:
            dict: Tools and tool_resources for the assistant
        """
        if not contexts:
            return None
        
        # Collect documents from each context separately
        context_documents = []
        for context in contexts:
            documents = self.collect_documents(context)
            if documents:
                context_documents.append((context['name'], documents))
            else:
                logger.warning(f"No documents found for context: {context.get('name', 'unnamed')}")
        
        if context_documents:
            # Let backend decide how to organize the vector stores
            return self.vs_backend.setup_contexts(contexts[0]['name'], context_documents)
        else:
            logger.warning("No documents found in any context")
            return None

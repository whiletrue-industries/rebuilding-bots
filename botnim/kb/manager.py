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

    def process_context(self, context_config: dict) -> str:
        """Process a context configuration and return vector store ID
        
        Args:
            context_config: Configuration dictionary for the context
            
        Returns:
            str: ID of the created vector store
        """
        kb_name = context_config['name']
        vector_store_id = self.kb_backend.create(kb_name)
        logger.info(f"Created vector store: {vector_store_id}")
        return vector_store_id

    def _get_content_type(self, file_path: Path) -> str:
        """Get the appropriate content type based on file extension"""
        content_types = {
            '.txt': 'text/plain',
            '.md': 'text/markdown',
            '.pdf': 'application/pdf',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        }
        return content_types[file_path.suffix.lower()]

    def _process_files(self, file_pattern: str) -> List[Tuple[str, str, str]]:
        """Process regular files matching the pattern
        
        Supports multiple file types that OpenAI can process:
        - .txt (plain text)
        - .md (markdown)
        - .pdf (PDF documents)
        - .doc/.docx (Word documents)
        
        Files are uploaded directly to OpenAI which handles the processing.
        """
        files = sorted(self.config_dir.glob(file_pattern))
        supported_extensions = {'.txt', '.md', '.pdf', '.doc', '.docx'}
        valid_files = [f for f in files if f.suffix.lower() in supported_extensions]
        if len(valid_files) < len(files):
            logger.warning(f"Skipping files without supported extensions. Supported: {supported_extensions}")
        
        return [(
            self._add_environment_suffix(f.name), 
            str(f), 
            self._get_content_type(f)
        ) for f in valid_files]

    def _process_split_file(self, context_config: dict) -> List[Tuple[str, BinaryIO, str]]:
        """Process a directory of split files"""
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
        """Collect documents from all configured sources"""
        documents = []
        
        # If source is present, treat as spreadsheet
        if 'source' in context_config:
            from .download_sources import download_and_convert_spreadsheet
            target_dir = self.config_dir / context_config.get('split', f"{context_config['name']}_split")
            target_dir.mkdir(exist_ok=True)
            download_and_convert_spreadsheet(
                context_config['source'], 
                target_dir,
                context_config['name']
            )
            documents.extend(self._process_directory(target_dir))
        
        # Process files if specified
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

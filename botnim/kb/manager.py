from pathlib import Path
import requests
import io
from typing import List, Union, BinaryIO, Tuple
from .base import KnowledgeBase
from ..config import get_logger

logger = get_logger(__name__)

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
        files = sorted(self.config_dir.glob(file_pattern))
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
        """Collect documents from all configured sources"""
        documents = []
        source_type = context_config.get('type', 'files')  # Default to files for backward compatibility
        
        if source_type == 'files':
            # Process multiple separate files
            if 'files' in context_config:
                documents.extend(self._process_files(context_config['files']))
        
        elif source_type == 'spreadsheet':
            # Process Google Spreadsheet source
            if 'source' in context_config:
                from .download_sources import download_and_convert_spreadsheet
                target_dir = self.config_dir / f"{context_config['name']}_split"
                target_dir.mkdir(exist_ok=True)
                download_and_convert_spreadsheet(
                    context_config['source'], 
                    target_dir,
                    context_config['name']
                )
                documents.extend(self._process_directory(target_dir))
        
        elif source_type == 'split_file':
            # Process single file that needs splitting
            if 'source' in context_config:
                source_path = self.config_dir / context_config['source']
                target_dir = source_path.parent / f"{source_path.stem}_split"
                target_dir.mkdir(exist_ok=True)
                self._split_file(source_path, target_dir)
                documents.extend(self._process_directory(target_dir))
        
        else:
            logger.warning(f"Unknown source type: {source_type}")
        
        return documents

    def _process_directory(self, dir_path: Path) -> List[Tuple[str, BinaryIO, str]]:
        """Process all markdown files in a directory"""
        documents = []
        for file_path in sorted(dir_path.glob('*.md')):
            if file_path.read_text().strip():
                env_filename = self._add_environment_suffix(file_path.name)
                documents.append((
                    env_filename,
                    file_path.open('rb'),
                    'text/markdown'
                ))
        return documents

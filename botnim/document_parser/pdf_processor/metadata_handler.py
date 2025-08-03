"""
Metadata handling for PDF extraction pipeline.

This module handles metadata files that contain source URLs and other
information for PDF files.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class MetadataHandler:
    """
    Handles metadata for PDF files.
    """
    
    def __init__(self, input_directory: str):
        """
        Initialize metadata handler.
        
        Args:
            input_directory: Directory containing PDF files and metadata
        """
        self.input_directory = Path(input_directory)
    
    def load_metadata_for_pdf(self, pdf_path: Path) -> Dict[str, Any]:
        """
        Load metadata for a specific PDF file.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Dictionary containing metadata
        """
        # Look for metadata file with same name as PDF
        # Use hash for long filenames to avoid filesystem limits
        metadata_path = self._get_metadata_path(pdf_path)
        
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                # Validate that source_url is present and not a file path
                source_url = metadata.get('source_url', '')
                if source_url and not self._is_file_path(source_url):
                    logger.info(f"Loaded metadata with source URL: {source_url}")
                else:
                    logger.warning(f"Invalid or missing source URL in metadata: {source_url}")
                    metadata['source_url'] = self._generate_placeholder_url(pdf_path)
                
                return metadata
                
            except Exception as e:
                logger.warning(f"Failed to load metadata from {metadata_path}: {e}")
        
        # Return default metadata with placeholder URL
        return self._create_default_metadata(pdf_path)
    
    def _is_file_path(self, url: str) -> bool:
        """
        Check if a URL is actually a file path.
        
        Args:
            url: URL or path to check
            
        Returns:
            True if it's a file path, False if it's a proper URL
        """
        # Check for common file path indicators
        if url.startswith('/') or url.startswith('./') or url.startswith('../'):
            return True
        
        # Check for Windows file paths
        if ':\\' in url or url.startswith('C:\\') or url.startswith('D:\\'):
            return True
        
        # Check for file extensions without protocol
        if '.' in url and not url.startswith(('http://', 'https://', 'ftp://')):
            # If it contains a file extension and no protocol, it's likely a file path
            return True
        
        return False
    
    def _get_metadata_path(self, pdf_path: Path) -> Path:
        """
        Get the metadata file path for a PDF, using hash for long filenames.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Path to metadata file
        """
        import hashlib
        
        # Create metadata file in the same directory as the PDF
        pdf_directory = pdf_path.parent
        
        # Check if filename is too long (>50 characters) - be more conservative
        if len(pdf_path.stem) > 50:
            # Use hash of the filename to create a shorter metadata filename
            filename_hash = hashlib.md5(pdf_path.stem.encode('utf-8')).hexdigest()[:16]
            metadata_filename = f"{filename_hash}.pdf.metadata.json"
            logger.info(f"Using hash-based metadata filename for long PDF name: {metadata_filename}")
        else:
            # Use original filename for shorter names
            metadata_filename = f"{pdf_path.stem}.pdf.metadata.json"
        
        return pdf_directory / metadata_filename
    
    def resolve_template_variables(self, template: str, pdf_path: Path, file_metadata: Dict[str, Any]) -> str:
        """
        Resolve template variables in a string.
        
        Args:
            template: Template string with variables like {pdf_url}, {download_date}
            pdf_path: Path to PDF file
            file_metadata: File metadata dictionary
            
        Returns:
            Resolved string with variables replaced
        """
        resolved = template
        
        # Replace {pdf_url} with actual URL from file metadata or placeholder
        if '{pdf_url}' in resolved:
            pdf_url = file_metadata.get('source_url', '')
            if not pdf_url or self._is_file_path(pdf_url):
                pdf_url = self._generate_placeholder_url(pdf_path)
            resolved = resolved.replace('{pdf_url}', pdf_url)
        
        # Replace {download_date} with current date
        if '{download_date}' in resolved:
            current_date = datetime.now().strftime('%Y-%m-%d')
            resolved = resolved.replace('{download_date}', current_date)
        
        # Replace {extraction_date} with current timestamp
        if '{extraction_date}' in resolved:
            extraction_date = datetime.now().isoformat()
            resolved = resolved.replace('{extraction_date}', extraction_date)
        
        # Replace {pdf_name} with PDF filename
        if '{pdf_name}' in resolved:
            pdf_name = pdf_path.name
            resolved = resolved.replace('{pdf_name}', pdf_name)
        
        # Replace {pdf_stem} with PDF filename without extension
        if '{pdf_stem}' in resolved:
            pdf_stem = pdf_path.stem
            resolved = resolved.replace('{pdf_stem}', pdf_stem)
        
        return resolved
    
    def merge_config_metadata(self, file_metadata: Dict[str, Any], config_metadata: Dict[str, Any], 
                            pdf_path: Path) -> Dict[str, Any]:
        """
        Merge file metadata with config metadata, resolving template variables.
        
        Args:
            file_metadata: Metadata from file (.pdf.metadata.json)
            config_metadata: Metadata from config (source.metadata section)
            pdf_path: Path to PDF file
            
        Returns:
            Merged metadata dictionary with config metadata taking precedence
        """
        # Start with file metadata as base
        merged_metadata = file_metadata.copy()
        
        # Apply config metadata, resolving template variables
        for key, value in config_metadata.items():
            if isinstance(value, str):
                # Resolve template variables in string values
                resolved_value = self.resolve_template_variables(value, pdf_path, file_metadata)
                merged_metadata[key] = resolved_value
            else:
                # Non-string values are used as-is
                merged_metadata[key] = value
        
        return merged_metadata
    
    def _generate_placeholder_url(self, pdf_path: Path) -> str:
        """
        Generate a placeholder URL for a PDF file.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Placeholder URL
        """
        # This should be replaced with actual URL extraction logic
        # For now, return a placeholder that indicates it needs to be updated
        return f"PLACEHOLDER_URL_FOR_{pdf_path.name}"
    
    def _create_default_metadata(self, pdf_path: Path) -> Dict[str, Any]:
        """
        Create default metadata for a PDF file.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Default metadata dictionary
        """
        return {
            'source_url': self._generate_placeholder_url(pdf_path),
            'extraction_date': datetime.now().isoformat(),
            'input_file': str(pdf_path),
            'metadata_source': 'default'
        }
    
    def validate_metadata(self, metadata: Dict[str, Any]) -> bool:
        """
        Validate metadata structure and content.
        
        Args:
            metadata: Metadata dictionary to validate
            
        Returns:
            True if metadata is valid, False otherwise
        """
        required_fields = ['source_url', 'extraction_date']
        
        for field in required_fields:
            if field not in metadata:
                logger.warning(f"Missing required metadata field: {field}")
                return False
        
        # Validate source_url is not a file path
        source_url = metadata.get('source_url', '')
        if self._is_file_path(source_url):
            logger.warning(f"Source URL appears to be a file path: {source_url}")
            return False
        
        return True
    
    def update_source_url(self, pdf_path: Path, new_url: str) -> bool:
        """
        Update the source URL for a PDF file.
        
        Args:
            pdf_path: Path to PDF file
            new_url: New source URL
            
        Returns:
            True if update was successful, False otherwise
        """
        metadata_path = self._get_metadata_path(pdf_path)
        
        try:
            # Load existing metadata or create new
            if metadata_path.exists():
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
            else:
                metadata = self._create_default_metadata(pdf_path)
            
            # Update source URL
            metadata['source_url'] = new_url
            metadata['last_updated'] = datetime.now().isoformat()
            
            # Write updated metadata
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Updated source URL for {pdf_path.name}: {new_url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update source URL for {pdf_path.name}: {e}")
            return False 
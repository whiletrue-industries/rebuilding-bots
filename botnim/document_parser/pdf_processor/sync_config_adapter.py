"""
Configuration adapter for converting sync config to PDF extraction config.

This module provides utilities to convert from the sync configuration format
to the PDF extraction configuration format used by the PDF pipeline.
"""

import yaml
from typing import Dict, List, Any
from .pdf_extraction_config import PDFExtractionConfig, SourceConfig, FieldConfig


class SyncConfigAdapter:
    """
    Adapter for converting sync configuration to PDF extraction configuration.
    """
    
    @staticmethod
    def load_pdf_sources_from_sync_config(sync_config_path: str) -> PDFExtractionConfig:
        """
        Load PDF sources from a sync configuration file.
        
        Args:
            sync_config_path: Path to the sync configuration YAML file
            
        Returns:
            PDFExtractionConfig with PDF sources converted from sync config
            
        Raises:
            FileNotFoundError: If the sync config file doesn't exist
            yaml.YAMLError: If the sync config file is malformed
        """
        with open(sync_config_path, 'r', encoding='utf-8') as f:
            sync_data = yaml.safe_load(f)
        
        # Extract PDF sources from sync config
        pdf_sources = []
        for source in sync_data.get('sources', []):
            if source.get('type') == 'pdf' and 'pdf_config' in source:
                pdf_source = SyncConfigAdapter._convert_sync_source_to_pdf_source(source)
                if pdf_source:
                    pdf_sources.append(pdf_source)
        
        return PDFExtractionConfig(sources=pdf_sources)
    
    @staticmethod
    def _convert_sync_source_to_pdf_source(sync_source: Dict[str, Any]) -> SourceConfig:
        """
        Convert a sync source configuration to PDF source configuration.
        
        Args:
            sync_source: Source configuration from sync config
            
        Returns:
            SourceConfig for PDF extraction, or None if conversion not possible
        """
        pdf_config = sync_source.get('pdf_config', {})
        
        # Check if this is an Open Budget source
        if 'index_csv_url' not in pdf_config or 'datapackage_url' not in pdf_config:
            return None  # Skip non-Open Budget sources
        
        # Convert fields from processing config
        fields = []
        processing_config = pdf_config.get('processing', {})
        
        for field_config in processing_config.get('fields', []):
            field = FieldConfig(
                name=field_config.get('name', ''),
                description=field_config.get('description', ''),
                example=field_config.get('example', ''),
                hint=field_config.get('hint', '')
            )
            fields.append(field)
        
        # Create PDF source config
        source_config = SourceConfig(
            name=sync_source.get('name', ''),
            description=sync_source.get('description', ''),
            unique_id_field='url',  # Default for Open Budget sources
            metadata={
                'source_id': sync_source.get('id', ''),
                'source_type': sync_source.get('type', ''),
                'tags': ','.join(sync_source.get('tags', [])),
                'data_provider': 'open_budget'
            },
            fields=fields,
            extraction_instructions=processing_config.get('extraction_instructions', ''),
            index_csv_url=pdf_config['index_csv_url'],
            datapackage_url=pdf_config['datapackage_url']
        )
        
        return source_config 
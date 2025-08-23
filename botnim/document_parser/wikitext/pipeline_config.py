#!/usr/bin/env python3
"""
Pipeline configuration and validation.
"""

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Optional, Dict, Any, List
import json
from urllib.parse import unquote
import requests
import tempfile
from enum import Enum


def sanitize_filename(filename):
    """
    Sanitize filename for filesystem compatibility.
    """
    # Replace problematic characters with underscores
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Replace multiple spaces/underscores with single underscore
    filename = re.sub(r'[_\s]+', '_', filename)
    # Remove leading/trailing underscores
    filename = filename.strip('_')
    return filename


class PipelineStage(Enum):
    """Pipeline execution stages."""
    EXTRACT_STRUCTURE = "extract_structure"
    EXTRACT_CONTENT = "extract_content"
    GENERATE_MARKDOWN = "generate_markdown"

class Environment(Enum):
    """Execution environments."""
    STAGING = "staging"
    PRODUCTION = "production"

@dataclass
class WikitextProcessorConfig:
    """Main pipeline configuration."""
    
    # Input/Output
    input_url: str
    output_base_dir: Path
    
    # Processing parameters
    content_type: str = "סעיף"
    environment: Environment = Environment.STAGING
    
    # OpenAI parameters
    model: str = "gpt-4.1"  # Use mini model with larger context window
    max_tokens: Optional[int] = None  # Optional; if None, use model default
            
    # Derived paths
    structure_file: Optional[Path] = field(init=False)
    content_file: Optional[Path] = field(init=False)
    chunks_dir: Optional[Path] = field(init=False)
    
    def __post_init__(self):
        """Initialize derived paths."""
        with tempfile.NamedTemporaryFile(delete=False, suffix='.html') as tf:
            tf.write(requests.get(self.input_url).content)
            tf.flush()
            self.input_html_file = Path(tf.name)
        self.output_base_dir = Path(self.output_base_dir)
        
        # Create output directory structure
        self.output_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Set derived paths
        base_name = sanitize_filename(unquote(self.input_url).split('?')[0].split('/')[-1].split('.')[0])
        self.structure_file = self.output_base_dir / f"{base_name}_structure.json"
        self.content_file = self.output_base_dir / f"{base_name}_structure_content.json"
        self.metadata_file = self.output_base_dir / f"{base_name}_pipeline_metadata.json"
        self.chunks_dir = self.output_base_dir / "chunks"
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        if not self.input_html_file.exists():
            errors.append(f"Input HTML file not found: {self.input_html_file}")
        
        if not self.content_type.strip():
            errors.append("Content type cannot be empty")
        
        if self.max_tokens is not None and self.max_tokens < 1000:
            errors.append("Max tokens must be at least 1000 if set")
        
        return errors
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "input_html_file": str(self.input_html_file),
            "output_base_dir": str(self.output_base_dir),
            "content_type": self.content_type,
            "environment": self.environment.value,
            "model": self.model,
            "max_tokens": self.max_tokens,
            "structure_file": str(self.structure_file) if self.structure_file else None,
            "content_file": str(self.content_file) if self.content_file else None,
            "chunks_dir": str(self.chunks_dir) if self.chunks_dir else None,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WikitextProcessorConfig':
        """Create from dictionary."""
        # Convert string enums back to enum objects
        if 'environment' in data:
            data['environment'] = Environment(data['environment'])
        
        # Remove derived fields from input data
        derived_fields = {'structure_file', 'content_file', 'chunks_dir'}
        clean_data = {k: v for k, v in data.items() if k not in derived_fields}
        
        return cls(**clean_data)
    
    def save(self, config_file: Path):
        """Save configuration to file."""
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
    
    @classmethod
    def load(cls, config_file: Path) -> 'WikitextProcessorConfig':
        """Load configuration from file."""
        with open(config_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return cls.from_dict(data)


@dataclass
class PipelineMetadata:
    """Metadata for pipeline execution."""
    
    pipeline_version: str = "1.0.0"
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    stages_completed: List[PipelineStage] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Stage-specific metadata
    structure_extraction: Dict[str, Any] = field(default_factory=dict)
    content_extraction: Dict[str, Any] = field(default_factory=dict)
    markdown_generation: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "pipeline_version": self.pipeline_version,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "stages_completed": [stage.value for stage in self.stages_completed],
            "errors": self.errors,
            "warnings": self.warnings,
            "structure_extraction": self.structure_extraction,
            "content_extraction": self.content_extraction,
            "markdown_generation": self.markdown_generation,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PipelineMetadata':
        """Create from dictionary."""
        # Convert string enums back to enum objects
        if 'stages_completed' in data:
            data['stages_completed'] = [PipelineStage(stage) for stage in data['stages_completed']]
        
        return cls(**data)


def validate_json_structure(json_file: Path, required_keys: List[str]) -> List[str]:
    """Validate JSON file structure."""
    errors = []
    
    if not json_file.exists():
        errors.append(f"JSON file not found: {json_file}")
        return errors
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        errors.append(f"Invalid JSON in {json_file}: {e}")
        return errors
    except Exception as e:
        errors.append(f"Error reading {json_file}: {e}")
        return errors
    
    for key in required_keys:
        if key not in data:
            errors.append(f"Missing required key '{key}' in {json_file}")
    
    return errors 
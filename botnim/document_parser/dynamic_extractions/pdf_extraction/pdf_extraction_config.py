from typing import List, Dict, Optional
from pydantic import BaseModel, Field
import yaml

class FieldConfig(BaseModel):
    name: str
    description: Optional[str] = None
    example: Optional[str] = None
    hint: Optional[str] = None

class SourceConfig(BaseModel):
    name: str
    description: Optional[str] = None
    file_pattern: str
    unique_id_field: str
    metadata: Dict[str, str] = Field(default_factory=dict)
    fields: List[FieldConfig]
    extraction_instructions: Optional[str] = None

class PDFExtractionConfig(BaseModel):
    sources: List[SourceConfig]

    @classmethod
    def from_yaml(cls, path: str) -> "PDFExtractionConfig":
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return cls(**data) 
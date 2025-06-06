from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from enum import Enum

class FieldWeight(Enum):
    """Standard weights for different types of matches"""
    EXACT = 3.0
    PARTIAL = 2.0
    SEMANTIC = 1.0

@dataclass
class SearchFieldConfig:
    """Configuration for how a field should be searched"""
    field_path: str  # Path to the field in the document (e.g., "extracted_data.OfficialSource")
    exact_match_weight: float = FieldWeight.EXACT.value
    partial_match_weight: float = FieldWeight.PARTIAL.value
    semantic_match_weight: float = FieldWeight.SEMANTIC.value
    boost_factor: float = 1.0

@dataclass
class SearchModeConfig:
    """Base configuration for a search mode"""
    name: str
    description: str
    field_configs: Dict[str, SearchFieldConfig]
    min_score: float = 0.5

@dataclass
class SearchResult:
    """Result from a search operation"""
    content: str
    metadata: Dict[str, Any]
    score: float
    explanation: Optional[Dict[str, Any]] = None 
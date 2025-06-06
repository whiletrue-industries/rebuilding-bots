from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

@dataclass
class FieldWeight:
    """Configuration for field weights"""
    exact_match: float = 3.0
    partial_match: float = 2.0
    semantic_match: float = 1.0

@dataclass
class SearchFieldConfig:
    """Configuration for how a field should be searched"""
    name: str  # Name of the field (e.g., "official_source")
    weight: FieldWeight = field(default_factory=FieldWeight)
    boost_factor: float = 1.0
    fuzzy_matching: bool = False

@dataclass
class SearchModeConfig:
    """Base configuration for a search mode"""
    name: str
    description: str
    fields: List[SearchFieldConfig]
    min_score: float = 0.5
    minimum_should_match: int = 1
    num_results: int = 3

@dataclass
class SearchResult:
    """Result from a search operation"""
    content: str
    metadata: Dict[str, Any]
    score: float
    explanation: Optional[Dict[str, Any]] = None 
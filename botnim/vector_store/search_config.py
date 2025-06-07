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
    use_phrase_match: bool = False
    field_path: Optional[str] = None

@dataclass
class SearchModeConfig:
    """Base configuration for a search mode"""
    name: str
    description: str
    fields: List[SearchFieldConfig]
    min_score: float = 0.5 
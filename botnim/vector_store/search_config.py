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
    num_results: int = 7  # Default number of results for this mode
    use_vector_search: bool = True
    # Lexical (BM25 / tsvector) branch toggle. ON by default for back-compat
    # with all the ES-era modes. The Aurora pipeline uses Postgres `simple`
    # tsv config (no Hebrew stemmer/analyzer ships with PG), so BM25 here
    # is essentially noise on Hebrew construct-state morphology and dilutes
    # the vector signal under RRF. REGULAR + METADATA_BROWSE flip this to
    # False; SECTION_NUMBER / RELATED_RESOURCE keep it True because they
    # are exact-clause-number lookups where lexical IS the right tool.
    use_lexical_search: bool = True

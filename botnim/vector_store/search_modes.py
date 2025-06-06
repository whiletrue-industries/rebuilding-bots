from typing import Dict
from .search_config import SearchModeConfig, SearchFieldConfig, FieldWeight

def create_takanon_section_number_mode() -> SearchModeConfig:
    """
    Creates a search mode optimized for finding sections in the Takanon `legal text` context.
    
    This mode is specifically designed for the Takanon bot 'legal text' context and heavily weights exact matches
    in the OfficialSource field where section numbers are typically found in the format:
    "סעיף XX" or similar variations.
    
    The mode prioritizes:
    1. Exact matches of section numbers in the OfficialSource field
    2. Partial matches in the OfficialSource field
    3. Content matches as a fallback
    
    This mode is not suitable for other legal contexts that might have different section numbering formats.
    """
    return SearchModeConfig(
        name="TAKANON_SECTION_NUMBER",
        description="Search mode optimized for finding sections by their number in Takanon (תקנון הכנסת) legal text",
        field_configs={
            "official_source": SearchFieldConfig(
                field_path="metadata.extracted_data.OfficialSource",
                exact_match_weight=FieldWeight.EXACT.value * 2,  # Double weight for exact matches
                partial_match_weight=FieldWeight.PARTIAL.value,
                semantic_match_weight=0.0,  # Disable semantic matching for section numbers
                boost_factor=2.0  # Boost this field's importance
            ),
            "content": SearchFieldConfig(
                field_path="content",
                exact_match_weight=FieldWeight.EXACT.value,
                partial_match_weight=FieldWeight.PARTIAL.value,
                semantic_match_weight=FieldWeight.SEMANTIC.value,
                boost_factor=1.0
            )
        },
        min_score=0.7  # Higher minimum score to ensure relevance
    ) 
from typing import Dict
from .search_config import SearchModeConfig, SearchFieldConfig, FieldWeight

def create_takanon_section_number_mode() -> SearchModeConfig:
    """
    Creates a search mode configuration for finding Takanon bot relate documents sections by their number.
    This mode is specialized for the Takanon legal text context and expects queries
    in the format 'סעיף X' where X is the section number.
    
    The search prioritizes exact matches in the OfficialSource field and includes
    fuzzy matching for the resource name to handle slight variations in naming.
    """
    return SearchModeConfig(
        name="TAKANON_SECTION_NUMBER",
        description="Specialized search mode for finding Takanon sections by their number (e.g. 'סעיף 12'). "
                   "Requires both section number and resource name. The resource name can be provided in a "
                   "flexible format (e.g. 'חוק הכנסת' or 'חוק-הכנסת').",
        min_score=0.5,
        fields=[
            SearchFieldConfig(
                name="official_source",
                weight=FieldWeight(
                    exact_match=2.2,  # High boost for exact matches in OfficialSource
                    partial_match=0.8  # Lower boost for partial matches
                ),
                boost_factor=1.5,
                field_path="metadata.extracted_data.OfficialSource"  # Use correct ES field path
            ),
            SearchFieldConfig(
                name="content",
                weight=FieldWeight(
                    exact_match=1.0,
                    partial_match=0.4
                ),
                boost_factor=0.8
            ),
            SearchFieldConfig(
                name="document_title",
                weight=FieldWeight(
                    exact_match=1.8,  # High boost for exact matches in document title
                    partial_match=0.9  # Good boost for partial matches
                ),
                boost_factor=1.2,
                fuzzy_matching=True,  # Enable fuzzy matching for resource names
                field_path="metadata.extracted_data.DocumentTitle"  # Use correct ES field path
            )
        ]
    ) 
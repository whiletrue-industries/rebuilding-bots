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
            # 1. Heavily boost the exact match for the resource name in DocumentTitle
            SearchFieldConfig(
                name="document_title_keyword",
                weight=FieldWeight(
                    exact_match=25.0,  # Highest boost for the exact resource name
                    partial_match=0.0
                ),
                boost_factor=10.0,
                field_path="metadata.extracted_data.DocumentTitle.keyword"
            ),
            # 2. Keep a high boost on OfficialSource, primarily for finding the section number
            SearchFieldConfig(
                name="official_source",
                weight=FieldWeight(
                    exact_match=15.0, # High boost for the section number
                    partial_match=1.0
                ),
                boost_factor=5.0,
                field_path="metadata.extracted_data.OfficialSource"
            ),
            # 3. Reduce boost for fuzzy/partial matches on the title to avoid noise
            SearchFieldConfig(
                name="document_title",
                weight=FieldWeight(
                    exact_match=2.0,
                    partial_match=1.0
                ),
                boost_factor=1.0, # Low boost for fuzzy title match
                fuzzy_matching=True,
                field_path="metadata.extracted_data.DocumentTitle"
            ),
            # 4. Keep content match as a low-priority signal
            SearchFieldConfig(
                name="content",
                weight=FieldWeight(
                    exact_match=1.0,
                    partial_match=0.2
                ),
                boost_factor=0.5
            ),
        ]
    )

def create_regular_search_mode() -> SearchModeConfig:
    """
    Creates the default 'regular' search mode configuration, matching the previous hybrid search logic.
    """
    return SearchModeConfig(
        name="REGULAR",
        description="Standard semantic search across all main fields.",
        fields=[
            SearchFieldConfig(
                name="content",
                weight=FieldWeight(exact_match=0.4, partial_match=0.4, semantic_match=1.0),
                boost_factor=1.0,
                field_path="content"
            ),
            SearchFieldConfig(
                name="title",
                weight=FieldWeight(exact_match=0.4, partial_match=0.4, semantic_match=1.0),
                boost_factor=1.0,
                field_path="metadata.title"
            ),
            SearchFieldConfig(
                name="document_title",
                weight=FieldWeight(exact_match=10.0, partial_match=0.4, semantic_match=1.0),
                boost_factor=1.0,
                field_path="metadata.extracted_data.DocumentTitle"
            ),
            SearchFieldConfig(
                name="document_title_keyword",
                weight=FieldWeight(exact_match=15.0, partial_match=0.0, semantic_match=0.0),
                boost_factor=1.0,
                field_path="metadata.extracted_data.DocumentTitle.keyword"
            ),
            SearchFieldConfig(
                name="official_source",
                weight=FieldWeight(exact_match=0.4, partial_match=0.4, semantic_match=1.0),
                boost_factor=1.0,
                field_path="metadata.extracted_data.OfficialSource"
            ),
            SearchFieldConfig(
                name="official_roles_role",
                weight=FieldWeight(exact_match=0.4, partial_match=0.4, semantic_match=1.0),
                boost_factor=1.0,
                field_path="metadata.extracted_data.OfficialRoles.Role"
            ),
            SearchFieldConfig(
                name="description",
                weight=FieldWeight(exact_match=0.4, partial_match=0.4, semantic_match=1.0),
                boost_factor=1.0,
                field_path="metadata.extracted_data.Description"
            ),
            SearchFieldConfig(
                name="additional_keywords",
                weight=FieldWeight(exact_match=0.4, partial_match=0.4, semantic_match=1.0),
                boost_factor=1.0,
                field_path="metadata.extracted_data.AdditionalKeywords"
            ),
            SearchFieldConfig(
                name="topics",
                weight=FieldWeight(exact_match=0.4, partial_match=0.4, semantic_match=1.0),
                boost_factor=1.0,
                field_path="metadata.extracted_data.Topics"
            ),
        ]
    ) 
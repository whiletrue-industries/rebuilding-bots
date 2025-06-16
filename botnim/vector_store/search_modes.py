from typing import Dict
from types import MappingProxyType
from .search_config import SearchModeConfig, SearchFieldConfig, FieldWeight

# Define the TAKANON_SECTION_NUMBER mode config
TAKANON_SECTION_NUMBER_CONFIG = SearchModeConfig(
    name="TAKANON_SECTION_NUMBER",
    description="Specialized search mode for finding Takanon sections by their number (e.g. 'סעיף 12'). Requires both section number and resource name. The resource name can be provided in a flexible format (e.g. 'חוק הכנסת' or 'חוק-הכנסת').",
    min_score=0.5,
    num_results=3,  # Default for section/resource search
    use_vector_search=False,
    fields=[
        SearchFieldConfig(
            name="document_title_keyword",
            weight=FieldWeight(
                exact_match=25.0,
                partial_match=0.0
            ),
            boost_factor=10.0,
            field_path="metadata.extracted_data.DocumentTitle.keyword"
        ),
        SearchFieldConfig(
            name="official_source",
            weight=FieldWeight(
                exact_match=15.0,
                partial_match=1.0
            ),
            boost_factor=5.0,
            field_path="metadata.extracted_data.OfficialSource"
        ),
        SearchFieldConfig(
            name="document_title",
            weight=FieldWeight(
                exact_match=2.0,
                partial_match=1.0
            ),
            boost_factor=1.0,
            fuzzy_matching=True,
            field_path="metadata.extracted_data.DocumentTitle"
        ),
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

# Define the REGULAR mode config (canonical default)
REGULAR_CONFIG = SearchModeConfig(
    name="REGULAR",
    description="Semantic + full text search across all main fields.",
    min_score=0.5,
    num_results=7,  # Default for regular/semantic search
    use_vector_search=True,
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

# Canonical default search mode for all business logic
DEFAULT_SEARCH_MODE_NAME = "REGULAR"
DEFAULT_SEARCH_MODE = REGULAR_CONFIG

RELATED_RESOURCE_CONFIG = SearchModeConfig(
    name="RELATED_RESOURCE",
    description="Finds documents that are related to or mention a specific resource, by matching ReferenceLinks, LegalReferences, and related fields. Uses exact phrase matching for reference fields.",
    min_score=0.5,
    num_results=5,
    use_vector_search=False,
    fields=[
        SearchFieldConfig(
            name="reference_links",
            weight=FieldWeight(exact_match=15.0, partial_match=0.0, semantic_match=0.0),
            boost_factor=10.0,
            field_path="metadata.extracted_data.ReferenceLinks",
            use_phrase_match=True
        ),
        SearchFieldConfig(
            name="legal_references_title",
            weight=FieldWeight(exact_match=12.0, partial_match=0.0, semantic_match=0.0),
            boost_factor=8.0,
            field_path="metadata.extracted_data.LegalReferences.ReferenceTitle",
            use_phrase_match=True
        ),
        SearchFieldConfig(
            name="legal_references_text",
            weight=FieldWeight(exact_match=10.0, partial_match=0.0, semantic_match=0.0),
            boost_factor=6.0,
            field_path="metadata.extracted_data.LegalReferences.ReferenceText",
            use_phrase_match=True
        ),
        SearchFieldConfig(
            name="description",
            weight=FieldWeight(exact_match=2.0, partial_match=1.0, semantic_match=1.0),
            boost_factor=1.0,
            field_path="metadata.extracted_data.Description"
        ),
    ]
)

# Immutable registry of all search modes
SEARCH_MODES = MappingProxyType({
    "TAKANON_SECTION_NUMBER": TAKANON_SECTION_NUMBER_CONFIG,
    "REGULAR": REGULAR_CONFIG,
    "RELATED_RESOURCE": RELATED_RESOURCE_CONFIG,
    # Add more modes here as needed
})



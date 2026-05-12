from typing import Dict
from types import MappingProxyType
from .search_config import SearchModeConfig, SearchFieldConfig, FieldWeight

# Define the SECTION_NUMBER mode config
SECTION_NUMBER_CONFIG = SearchModeConfig(
    name="SECTION_NUMBER",
    description="Specialized search mode for finding legal text sections by their number (e.g. 'סעיף 12'). Requires both section number and resource name. The resource name can be provided in a flexible format (e.g. 'חוק הכנסת' or 'חוק-הכנסת').",
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
    num_results=15,  # Default for regular/semantic search (bumped from 7 to give RRF fusion enough room to surface boundary-rank docs that vector vs BM25 disagree on)
    use_vector_search=True,
    # BM25 dropped 2026-05-10: PG `simple` tsv has no Hebrew analyzer, the
    # prefix-OR workaround in _build_prefix_or_tsquery doesn't bridge
    # construct-state morphology, and the resulting noisy BM25 list dilutes
    # the (correct) vector ranking under RRF. See the local A/B in
    # /tmp/strategy-sweep.py + the row-0 cosine vs RRF probe on 2026-05-10.
    use_lexical_search=False,
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

# Define the METADATA_BROWSE mode config
METADATA_BROWSE_CONFIG = SearchModeConfig(
    name="METADATA_BROWSE",
    description="Browse committee decisions and documents (like legal advisor letters or legal opinions) with metadata summaries instead of full content. Returns more results (25) with structured metadata for exploration.",
    min_score=0.6,  # Higher threshold for better precision
    num_results=25,  # More results for browsing
    use_vector_search=True,  # Use semantic search for relevance
    use_lexical_search=False,  # Same Hebrew-tsv rationale as REGULAR — see search_config.py.
    fields=[
        SearchFieldConfig(
            name="content",
            weight=FieldWeight(exact_match=0.2, partial_match=0.2, semantic_match=0.8),
            boost_factor=1.0,
            field_path="content"
        ),
        SearchFieldConfig(
            name="document_title",
            weight=FieldWeight(exact_match=5.0, partial_match=0.3, semantic_match=1.0),
            boost_factor=2.0,
            field_path="metadata.extracted_data.DocumentTitle"
        ),
        SearchFieldConfig(
            name="summary",
            weight=FieldWeight(exact_match=0.3, partial_match=0.3, semantic_match=1.0),
            boost_factor=1.5,
            field_path="metadata.extracted_data.Summary"
        ),
        SearchFieldConfig(
            name="topics",
            weight=FieldWeight(exact_match=0.3, partial_match=0.3, semantic_match=1.0),
            boost_factor=1.0,
            field_path="metadata.extracted_data.Topics"
        ),
    ]
)

# RECENCY_BROWSE — pure date-desc browse. No vector or lexical scoring.
# The Aurora backend short-circuits to a date-ordered query when this mode is
# selected (see vector_store_aurora.VectorStoreAurora._recency_search).
# Output shape is identical to METADATA_BROWSE so the formatter at
# botnim.query._format_browse_mode_results handles both.
#
# Why this exists: when the user asks "what are the LATEST X", METADATA_BROWSE
# still returns top-N by hybrid similarity, which can miss the calendar-newest
# document if the query has weak topical signal. RECENCY_BROWSE returns the
# actual newest-by-date documents in the context.
RECENCY_BROWSE_CONFIG = SearchModeConfig(
    name="RECENCY_BROWSE",
    description="Browse the calendar-newest documents in a context. No similarity ranking — pure ORDER BY publication_date DESC. Use when the user asks for 'latest', 'most recent', 'newest' documents and there is no specific topical filter.",
    min_score=0.0,
    num_results=10,
    use_vector_search=False,
    use_lexical_search=False,
    fields=[],
)

# Immutable registry of all search modes
SEARCH_MODES = MappingProxyType({
    "SECTION_NUMBER": SECTION_NUMBER_CONFIG,
    "REGULAR": REGULAR_CONFIG,
    "METADATA_BROWSE": METADATA_BROWSE_CONFIG,
    "RECENCY_BROWSE": RECENCY_BROWSE_CONFIG,
    # Add more modes here as needed
})



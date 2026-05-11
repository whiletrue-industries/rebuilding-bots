"""Tests that metadata_filter flows from run_query() through QueryClient.search()
to VectorStoreAurora.search(). Uses mocks — no real DB required."""
from unittest.mock import MagicMock, patch
import pytest


class TestRunQueryMetadataFilter:
    def test_metadata_filter_passed_to_search(self, monkeypatch):
        """run_query() must forward metadata_filter to QueryClient.search()."""
        monkeypatch.setenv("BOTNIM_QUERY_BACKEND", "aurora")

        captured = {}

        def fake_search(self_inner, query_text, num_results=None, explain=False,
                        search_mode=None, metadata_filter=None):
            captured["metadata_filter"] = metadata_filter
            return []

        from botnim import query as q
        # Bypass the QueryClient constructor (which would otherwise try to
        # connect to Aurora). We only care about the search() forwarding.
        with patch.object(q.QueryClient, "__init__", lambda self, store_id: None), \
             patch.object(q.QueryClient, "search", fake_search):
            q.run_query(
                store_id="unified__government_decisions",
                query_text="החלטה 550",
                metadata_filter={"decision_number": "550"},
            )

        assert captured["metadata_filter"] == {"decision_number": "550"}

    def test_none_metadata_filter_passes_none(self, monkeypatch):
        """When no metadata_filter is supplied the value is None (not an empty dict)."""
        monkeypatch.setenv("BOTNIM_QUERY_BACKEND", "aurora")

        captured = {}

        def fake_search(self_inner, query_text, num_results=None, explain=False,
                        search_mode=None, metadata_filter=None):
            captured["metadata_filter"] = metadata_filter
            return []

        from botnim import query as q
        with patch.object(q.QueryClient, "__init__", lambda self, store_id: None), \
             patch.object(q.QueryClient, "search", fake_search):
            q.run_query(
                store_id="unified__government_decisions",
                query_text="החלטה 550",
            )

        assert captured["metadata_filter"] is None

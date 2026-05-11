"""Unit tests for VectorStoreAurora.government_distribution()."""
import json
from unittest.mock import MagicMock, patch
import pytest
from botnim.vector_store.vector_store_aurora import VectorStoreAurora


def _make_store():
    with patch.object(VectorStoreAurora, "__init__", lambda self, *a, **kw: None):
        store = VectorStoreAurora.__new__(VectorStoreAurora)
        store.config = {"slug": "unified", "context": []}
        store.environment = "staging"
    return store


def _mock_session(fetchone_result, fetchall_result):
    mock_conn = MagicMock()
    # First execute call → fetchone (context lookup)
    # Second execute call → fetchall (distribution query)
    mock_conn.execute.side_effect = [
        MagicMock(fetchone=MagicMock(return_value=fetchone_result)),
        MagicMock(fetchall=MagicMock(return_value=fetchall_result)),
    ]
    mock_sess = MagicMock()
    mock_sess.__enter__ = MagicMock(return_value=mock_conn)
    mock_sess.__exit__ = MagicMock(return_value=False)
    return mock_sess


class TestGovernmentDistribution:
    def test_returns_multiple_governments(self):
        store = _make_store()
        fake_rows = [
            ("36", "ממשלת בנט, 2021-2022", 12, "2022-06-13"),
            ("37", "ממשלת נתניהו, 2022-", 8, "2023-11-19"),
        ]
        with patch("botnim.vector_store.vector_store_aurora.get_session",
                   return_value=_mock_session(("ctx-id",), fake_rows)):
            result = store.government_distribution("government_decisions", "550")

        assert len(result) == 2
        assert result[0]["government_number"] == "36"
        assert result[0]["doc_count"] == 12
        assert result[1]["government_number"] == "37"

    def test_returns_empty_when_single_government(self):
        store = _make_store()
        fake_rows = [("37", "ממשלת נתניהו", 5, "2023-01-01")]
        with patch("botnim.vector_store.vector_store_aurora.get_session",
                   return_value=_mock_session(("ctx-id",), fake_rows)):
            result = store.government_distribution("government_decisions", "999")
        assert result == []

    def test_returns_empty_when_context_not_found(self):
        store = _make_store()
        with patch("botnim.vector_store.vector_store_aurora.get_session",
                   return_value=_mock_session(None, [])):
            result = store.government_distribution("government_decisions", "550")
        assert result == []

    def test_result_shape(self):
        store = _make_store()
        fake_rows = [
            ("28", "ממשלת ברק, 1999-2001", 3, "2000-05-10"),
            ("37", "ממשלת נתניהו, 2022-", 8, "2023-11-19"),
        ]
        with patch("botnim.vector_store.vector_store_aurora.get_session",
                   return_value=_mock_session(("ctx-id",), fake_rows)):
            result = store.government_distribution("government_decisions", "550")

        entry = result[0]
        assert set(entry.keys()) == {"government_number", "government", "doc_count", "latest_publish_date"}
        assert entry["government_number"] == "28"
        assert entry["latest_publish_date"] == "2000-05-10"

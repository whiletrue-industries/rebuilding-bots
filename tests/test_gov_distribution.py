"""Unit tests for VectorStoreAurora.government_distribution()."""
import sys
import json
from unittest.mock import MagicMock, patch
import pytest
import botnim.query as _real_botnim_query
from botnim.query import government_distribution_sidecar
from botnim.vector_store.vector_store_aurora import VectorStoreAurora


@pytest.fixture(autouse=True)
def _restore_real_botnim_query():
    """Ensure sys.modules['botnim.query'] is the real module for each test.

    tests/backend/test_metadata_filter_endpoint.py and
    tests/backend/test_gov_distribution_endpoint.py both replace
    sys.modules['botnim.query'] with a MagicMock at module-import
    time (and never restore it). Because pytest collection order is
    not guaranteed and any later sibling test that imports either of
    those endpoint files first will poison sys.modules globally,
    `patch("botnim.query.QueryClient", ...)` in this file then
    silently patches an attribute on the MagicMock instead of the
    real module — letting the production code call the real
    QueryClient and trip over OPENAI_API_KEY at runtime.

    Restoring the real module before each test in this file makes
    these tests order-independent.
    """
    saved = sys.modules.get("botnim.query")
    sys.modules["botnim.query"] = _real_botnim_query
    try:
        yield
    finally:
        if saved is None:
            sys.modules.pop("botnim.query", None)
        else:
            sys.modules["botnim.query"] = saved


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
    # Expose the inner conn so tests can inspect call args
    mock_sess._mock_conn = mock_conn
    return mock_sess


class TestGovernmentDistribution:
    def test_returns_multiple_governments(self):
        store = _make_store()
        fake_rows = [
            ("36", "ממשלת בנט, 2021-2022", 12, "2022-06-13"),
            ("37", "ממשלת נתניהו, 2022-", 8, "2023-11-19"),
        ]
        mock_sess = _mock_session(("ctx-id",), fake_rows)
        with patch("botnim.vector_store.vector_store_aurora.get_session",
                   return_value=mock_sess):
            result = store.government_distribution("government_decisions", "550")

        assert len(result) == 2
        assert result[0]["government_number"] == "36"
        assert result[0]["doc_count"] == 12
        assert result[1]["government_number"] == "37"

        # Verify mfilter was passed as JSON string, not a raw dict
        mock_conn = mock_sess._mock_conn
        call_args = mock_conn.execute.call_args_list[1]
        params = call_args[0][1]
        assert params["mfilter"] == json.dumps({"decision_number": "550"})

    def test_returns_empty_when_group_by_yields_zero_rows(self):
        """Context exists but the GROUP BY returns 0 rows (no matching decision_number)."""
        store = _make_store()
        with patch("botnim.vector_store.vector_store_aurora.get_session",
                   return_value=_mock_session(("ctx-id",), [])):
            result = store.government_distribution("government_decisions", "nonexistent")
        assert result == []

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


class TestGovernmentDistributionSidecar:
    def test_calls_through_to_aurora(self):
        fake_dist = [
            {"government_number": "36", "government": "ממשלת בנט", "doc_count": 12, "latest_publish_date": "2022-06-13"},
            {"government_number": "37", "government": "ממשלת נתניהו", "doc_count": 8, "latest_publish_date": "2023-11-19"},
        ]
        with patch("botnim.query.QueryClient") as MockClient:
            instance = MockClient.return_value
            instance.vector_store = MagicMock(spec=VectorStoreAurora)
            instance.vector_store.government_distribution.return_value = fake_dist
            instance.context_name = "government_decisions"

            result = government_distribution_sidecar(
                "unified__government_decisions__dev", "550"
            )

        MockClient.assert_called_once_with("unified__government_decisions__dev")
        instance.vector_store.government_distribution.assert_called_once_with(
            "government_decisions", "550"
        )
        assert len(result) == 2
        assert result[0]["government_number"] == "36"

    def test_returns_none_for_non_aurora_backend(self):
        with patch("botnim.query.QueryClient") as MockClient:
            instance = MockClient.return_value
            instance.vector_store = MagicMock()
            instance.vector_store.__class__ = object  # not VectorStoreAurora
            instance.context_name = "government_decisions"

            result = government_distribution_sidecar(
                "unified__government_decisions__dev", "550"
            )

        assert result is None

    def test_returns_none_when_aurora_returns_empty(self):
        with patch("botnim.query.QueryClient") as MockClient:
            instance = MockClient.return_value
            instance.vector_store = MagicMock(spec=VectorStoreAurora)
            instance.vector_store.government_distribution.return_value = []
            instance.context_name = "government_decisions"

            result = government_distribution_sidecar(
                "unified__government_decisions__dev", "999"
            )

        assert result is None

    def test_returns_none_on_db_error(self):
        with patch("botnim.query.QueryClient") as MockClient:
            instance = MockClient.return_value
            instance.vector_store = MagicMock(spec=VectorStoreAurora)
            instance.vector_store.government_distribution.side_effect = Exception("DB connection refused")
            instance.context_name = "government_decisions"

            result = government_distribution_sidecar(
                "unified__government_decisions", "550"
            )

        assert result is None

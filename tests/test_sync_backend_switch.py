"""Confirm sync.py routes --backend aurora to VectorStoreAurora and that
the default backend has been flipped to aurora."""
import pytest
from unittest.mock import MagicMock, patch


def test_aurora_backend_dispatches_to_VectorStoreAurora():
    from botnim import sync as sync_mod
    config = {"slug": "unified", "name": "Unified", "context": ["ctx1"]}

    with patch("botnim.sync.VectorStoreAurora") as mock_aurora:
        mock_aurora.return_value.vector_store_update.return_value = ([], None)
        sync_mod._sync_vector_store(
            config=config, config_dir=".", backend="aurora",
            environment="staging", reindex=False, replace_context="all",
        )
        mock_aurora.assert_called_once()


def test_default_backend_is_aurora():
    from botnim.sync import sync_agents
    import inspect
    sig = inspect.signature(sync_agents)
    assert sig.parameters["backend"].default == "aurora"


def test_es_backend_still_works_for_rollback():
    """The --backend es escape hatch must remain functional during the
    one-release deprecation window."""
    from botnim import sync as sync_mod
    config = {"slug": "unified", "name": "Unified", "context": ["ctx1"]}
    with patch("botnim.sync.VectorStoreES") as mock_es:
        mock_es.return_value.vector_store_update.return_value = ([], None)
        sync_mod._sync_vector_store(
            config=config, config_dir=".", backend="es",
            environment="staging", reindex=False, replace_context="all",
        )
        mock_es.assert_called_once()


def test_unknown_backend_still_raises():
    from botnim import sync as sync_mod
    with pytest.raises(ValueError, match="Unsupported backend"):
        sync_mod._sync_vector_store(
            config={"slug": "x", "context": ["ctx1"]}, config_dir=".", backend="bogus",
            environment="staging", reindex=False, replace_context="all",
        )

"""Unit tests for botnim.refresh_tracker.RefreshTracker."""
from botnim.refresh_tracker import RefreshTracker


def test_initial_snapshot_is_idle():
    t = RefreshTracker()
    snap = t.snapshot()
    assert snap["status"] == "idle"
    assert snap["started_at"] is None
    assert snap["finished_at"] is None
    assert snap["current"] is None
    assert snap["completed"] == []
    assert snap["completed_count"] == 0
    assert snap["ok_count"] == 0
    assert snap["failed_count"] == 0


def test_full_run_lifecycle_records_each_context():
    t = RefreshTracker()
    t.begin_run(total_contexts=3)
    assert t.snapshot()["status"] == "running"

    t.begin_context("unified", "legal_text")
    snap = t.snapshot()
    assert snap["current"] == {
        "bot": "unified",
        "context": "legal_text",
        "started_at": snap["current"]["started_at"],  # timestamp present
    }
    t.end_context("unified", "legal_text", ok=True)

    t.begin_context("unified", "legal_advisor_opinions")
    err = RuntimeError("upstream empty")
    t.end_context("unified", "legal_advisor_opinions", ok=False, error=err)

    t.begin_context("unified", "legal_advisor_letters")
    t.end_context("unified", "legal_advisor_letters", ok=True)

    t.finish_run(ok=True)
    snap = t.snapshot()
    assert snap["status"] == "done"
    assert snap["finished_at"] is not None
    assert snap["current"] is None
    assert snap["completed_count"] == 3
    assert snap["ok_count"] == 2
    assert snap["failed_count"] == 1
    assert [c["context"] for c in snap["completed"]] == [
        "legal_text", "legal_advisor_opinions", "legal_advisor_letters",
    ]
    failed = [c for c in snap["completed"] if c["status"] == "failed"][0]
    assert failed["error_type"] == "RuntimeError"
    assert "upstream empty" in failed["error_message"]


def test_finish_run_with_error_sets_failed_status():
    t = RefreshTracker()
    t.begin_run(total_contexts=0)
    t.finish_run(ok=False, error=ValueError("kaboom"))
    snap = t.snapshot()
    assert snap["status"] == "failed"
    assert snap["last_error"] == "ValueError: kaboom"


def test_begin_run_resets_previous_state():
    """A new run shouldn't carry over completed entries from the previous one."""
    t = RefreshTracker()
    t.begin_run(total_contexts=1)
    t.begin_context("unified", "ctx_a")
    t.end_context("unified", "ctx_a", ok=True)
    t.finish_run(ok=True)
    assert t.snapshot()["completed_count"] == 1

    t.begin_run(total_contexts=2)
    snap = t.snapshot()
    assert snap["status"] == "running"
    assert snap["completed_count"] == 0
    assert snap["total_contexts"] == 2

"""Unit tests for botnim.sanity.alerts.evaluate_alerts.

The rule reference is in docs/superpowers/specs/2026-05-09-...:
  R1 — pass-rate cliff vs rolling 7-day median (RED, needs ≥3 prior runs)
  R2 — OLD wins more rows than NEW (RED)
  R3 — capture failures on NEW (ORANGE only; doesn't downgrade red)
"""
from __future__ import annotations

import pytest

from botnim.sanity.alerts import evaluate_alerts


def test_no_history_no_pass_rate_cliff(make_summary):
    """R1 silent when prior_pass_rates count < 3 (median noisy)."""
    this_run = make_summary(pass_rate=0.50)
    history = []  # no priors at all
    out = evaluate_alerts(this_run, history)
    assert out.severity is None
    assert all(r.rule != "pass_rate_cliff" for r in out.reasons)


def test_two_priors_still_silent(make_summary):
    """Exactly 2 priors is below the threshold (≥3)."""
    this_run = make_summary(pass_rate=0.50)
    history = [make_summary(pass_rate=0.95), make_summary(pass_rate=0.95)]
    out = evaluate_alerts(this_run, history)
    assert all(r.rule != "pass_rate_cliff" for r in out.reasons)


def test_three_priors_with_cliff_fires_red(make_summary):
    """R1: 3 priors at 0.95 → median 0.95; this run 0.80 → 15pp drop ≥ 10 → RED."""
    this_run = make_summary(pass_rate=0.80, ab_new_wins=5, ab_old_wins=3)
    history = [make_summary(pass_rate=0.95) for _ in range(3)]
    out = evaluate_alerts(this_run, history)
    assert out.severity == "red"
    assert any(r.rule == "pass_rate_cliff" for r in out.reasons)


def test_pass_rate_drop_under_threshold_no_alert(make_summary):
    """R1: 9pp drop (just under 10pp) → no alert."""
    this_run = make_summary(pass_rate=0.86)
    history = [make_summary(pass_rate=0.95) for _ in range(3)]
    out = evaluate_alerts(this_run, history)
    assert out.severity is None


def test_old_wins_majority_fires_red(make_summary):
    """R2: ab_old_wins > ab_new_wins → RED."""
    this_run = make_summary(ab_new_wins=4, ab_old_wins=6, ab_ties=1)
    out = evaluate_alerts(this_run, [])
    assert out.severity == "red"
    assert any(r.rule == "old_wins_majority" for r in out.reasons)


def test_ab_tie_no_majority_alert(make_summary):
    """R2: equal wins → no alert."""
    this_run = make_summary(ab_new_wins=4, ab_old_wins=4, ab_ties=3)
    out = evaluate_alerts(this_run, [])
    assert all(r.rule != "old_wins_majority" for r in out.reasons)


def test_capture_infra_only_fires_orange(make_summary):
    """R3 alone: rubric_infra > 0 with no other rule active → ORANGE."""
    this_run = make_summary(
        ab_new_wins=4, ab_old_wins=4, ab_ties=1,
        rubric_pass=7, rubric_fail=0, rubric_xfail=2, rubric_infra=2,
        pass_rate=7 / (7 + 0),
    )
    out = evaluate_alerts(this_run, [])
    assert out.severity == "orange"
    assert any(r.rule == "capture_failed" for r in out.reasons)


def test_capture_infra_does_not_downgrade_red(make_summary):
    """R2 fires red; R3 also fires; final severity stays red."""
    this_run = make_summary(
        ab_new_wins=3, ab_old_wins=5, ab_ties=2, rubric_infra=1,
    )
    out = evaluate_alerts(this_run, [])
    assert out.severity == "red"
    rules = {r.rule for r in out.reasons}
    assert "old_wins_majority" in rules
    assert "capture_failed" in rules


def test_all_xfail_pass_rate_none_silences_r1(make_summary):
    """R1 cannot fire when this_run.pass_rate is None (all-XFAIL run)."""
    this_run = make_summary(
        rubric_pass=0, rubric_fail=0, rubric_xfail=11, rubric_infra=0,
        pass_rate=None,
    )
    history = [make_summary(pass_rate=0.95) for _ in range(3)]
    out = evaluate_alerts(this_run, history)
    assert all(r.rule != "pass_rate_cliff" for r in out.reasons)


def test_r1_and_r2_stack_with_two_reasons(make_summary):
    """R1 + R2 both fire: severity red, two reasons listed."""
    this_run = make_summary(pass_rate=0.60, ab_new_wins=3, ab_old_wins=5)
    history = [make_summary(pass_rate=0.95) for _ in range(3)]
    out = evaluate_alerts(this_run, history)
    assert out.severity == "red"
    rules = {r.rule for r in out.reasons}
    assert rules == {"pass_rate_cliff", "old_wins_majority"}


def test_alert_detail_strings_carry_magnitude(make_summary):
    """Detail strings include numbers a human can read on the banner."""
    this_run = make_summary(pass_rate=0.60, ab_new_wins=3, ab_old_wins=6)
    history = [make_summary(pass_rate=0.90) for _ in range(3)]
    out = evaluate_alerts(this_run, history)
    cliff = next(r for r in out.reasons if r.rule == "pass_rate_cliff")
    assert "60%" in cliff.detail and "30pp" in cliff.detail and "90%" in cliff.detail
    majority = next(r for r in out.reasons if r.rule == "old_wins_majority")
    assert "OLD won 6" in majority.detail and "NEW won 3" in majority.detail

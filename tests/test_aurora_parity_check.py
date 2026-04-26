"""Tests for the parity-check script's pure logic.

The script's IO layer (real sync calls) is integration-tested in CI
against a real Aurora; here we test only the comparison primitives.
"""
import pytest

from botnim.benchmark.aurora_parity_check import (
    jaccard_top_k,
    p95_latency,
    parity_verdict,
)


def test_jaccard_identical_lists():
    assert jaccard_top_k(["a", "b", "c"], ["a", "b", "c"], k=3) == 1.0


def test_jaccard_disjoint_lists():
    assert jaccard_top_k(["a", "b"], ["c", "d"], k=2) == 0.0


def test_jaccard_partial_overlap():
    # {a,b,c} vs {b,c,d} → intersection 2, union 4, jaccard 0.5
    assert jaccard_top_k(["a", "b", "c"], ["b", "c", "d"], k=3) == 0.5


def test_jaccard_truncates_to_k():
    """Lists longer than k are sliced before comparison."""
    long_a = ["a", "b", "c", "d", "e"]
    long_b = ["a", "b", "c", "z", "y"]
    # Top-3: {a,b,c} vs {a,b,c} → 1.0
    assert jaccard_top_k(long_a, long_b, k=3) == 1.0


def test_p95_latency_with_evenly_spaced_samples():
    # 100 samples 1..100 → p95 = 95
    samples = list(range(1, 101))
    assert p95_latency(samples) == 95


def test_p95_latency_empty_returns_none():
    assert p95_latency([]) is None


def test_parity_verdict_passes_when_thresholds_met():
    v = parity_verdict(jaccard=0.85, es_p95=100, aurora_p95=110)
    assert v["pass"] is True
    assert v["jaccard_ok"] is True
    assert v["latency_ok"] is True


def test_parity_verdict_fails_on_low_jaccard():
    v = parity_verdict(jaccard=0.7, es_p95=100, aurora_p95=110)
    assert v["pass"] is False
    assert v["jaccard_ok"] is False


def test_parity_verdict_fails_on_high_aurora_latency():
    v = parity_verdict(jaccard=0.95, es_p95=100, aurora_p95=125)  # 1.25x > 1.2x
    assert v["pass"] is False
    assert v["latency_ok"] is False

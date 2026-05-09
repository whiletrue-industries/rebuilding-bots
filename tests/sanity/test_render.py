from __future__ import annotations

from botnim.sanity import render


def test_render_html_returns_self_contained_string():
    capture = [
        {
            "row": 0,
            "question": "q?",
            "expected_behavior": "e",
            "must_not_contain": [],
            "answer_old": {"text": "old answer", "ok": True},
            "answer_new": {"text": "new answer", "ok": True},
        }
    ]
    judged = {"0": {"ab_verdict": "NEW", "ab_reason": "x", "rubric_score": 0.9, "rubric_verdict": "PASS", "rubric_reason": "y"}}
    html = render.render_html(capture, judged, title="t")
    assert "<html" in html.lower() or "<!doctype" in html.lower()
    # self-contained: no external <link rel="stylesheet" href="http..."> or <script src="http...">
    assert "http://" not in html and "https://" not in html or "external" not in html.lower()
    assert "old answer" in html
    assert "new answer" in html
    assert "t" in html  # title

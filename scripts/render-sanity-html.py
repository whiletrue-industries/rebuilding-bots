#!/usr/bin/env python3
"""scripts/render-sanity-html.py

Render the side-by-side sanity DoD into a single self-contained HTML page.

Inputs:
  --capture <path>   capture-stage JSON (one record per gold-set row, with
                     answer_old + answer_new but no judgements)
  --judged  <path>   judge-stage JSON keyed by row index, e.g.:
                     {
                       "0": {
                         "ab_verdict": "NEW" | "OLD" | "TIE",
                         "ab_reason":  "one sentence",
                         "rubric_score": 0.85,
                         "rubric_verdict": "PASS" | "FAIL" | "XFAIL",
                         "rubric_reason": "one sentence"
                       }, ...
                     }
                     If --judged is missing, the page still renders, with
                     judgement cells marked "(pending)" — useful for
                     iterating between capture and judging.
  --out     <path>   output HTML path
  --title   <str>    optional page title (default: "Sanity DoD")

Single self-contained file: inlines all CSS, no external assets, no JS
beyond a tiny copy-to-clipboard handler. Safe to email or open offline.
"""
from __future__ import annotations

import argparse
import html
import json
import sys
import time
from pathlib import Path


def fmt_duration(ms: float | int | None) -> str:
    if ms is None:
        return "—"
    s = ms / 1000.0
    return f"{s:.1f}s"


def verdict_badge(verdict: str | None) -> str:
    if not verdict:
        return '<span class="badge pending">pending</span>'
    v = verdict.upper()
    klass = {
        "NEW": "win-new",
        "OLD": "win-old",
        "TIE": "tie",
        "PASS": "pass",
        "PASS_T1": "pass",
        "PASS_T2": "xfail",   # green-ish but warning — needed a followup
        "FAIL": "fail",
        "XFAIL": "xfail",
        "INFRA": "infra",
    }.get(v, "neutral")
    label = {
        "NEW": "NEW wins",
        "OLD": "OLD wins",
        "TIE": "tie",
        "PASS_T1": "PASS (1-turn)",
        "PASS_T2": "PASS (after follow-up)",
    }.get(v, v)
    return f'<span class="badge {klass}">{html.escape(label)}</span>'


def score_badge(score) -> str:
    if score is None:
        return '<span class="score pending">—</span>'
    try:
        s = float(score)
    except (TypeError, ValueError):
        return '<span class="score pending">—</span>'
    if s >= 0.8:
        klass = "good"
    elif s >= 0.5:
        klass = "mid"
    else:
        klass = "bad"
    return f'<span class="score {klass}">{s:.2f}</span>'


def _one_turn(turn: dict | None, label: str) -> str:
    if not turn:
        return ""
    text = (turn.get("text") or "").strip()
    if not text and not turn.get("ok"):
        err = turn.get("error") or "no answer"
        return (
            f'<div class="turn-block">'
            f'<div class="turn-label">{html.escape(label)}</div>'
            f'<div class="answer empty">⚠ {html.escape(err)}</div>'
            f"</div>"
        )
    duration = fmt_duration(turn.get("duration_ms"))
    err = turn.get("error")
    err_html = (
        f'<div class="answer-meta error">⚠ {html.escape(err)} (partial answer below)</div>'
        if err
        else ""
    )
    return (
        f'<div class="turn-block">'
        f'<div class="turn-label">{html.escape(label)} <span class="turn-meta">{html.escape(duration)}</span></div>'
        f"{err_html}"
        f'<div class="answer-text">{html.escape(text)}</div>'
        f"</div>"
    )


def render_answer_block(ans: dict | None) -> str:
    if not ans:
        return '<div class="answer empty">no answer captured</div>'

    # New shape: turn1 / turn2. Old shape: text/duration/ok at top level.
    turn1 = ans.get("turn1")
    turn2 = ans.get("turn2")

    if turn1 is None and turn2 is None:
        # Legacy single-turn record — wrap as turn1.
        turn1 = {
            "ok": ans.get("ok"),
            "error": ans.get("error"),
            "text": ans.get("text"),
            "duration_ms": ans.get("duration_ms"),
        }

    blocks = [_one_turn(turn1, "turn 1")]
    if turn2:
        blocks.append(_one_turn(turn2, "turn 2 (follow-up)"))
    return "\n".join(b for b in blocks if b)


CSS = """
* { box-sizing: border-box; }
:root {
  --bg: #0f1115;
  --bg-card: #181b22;
  --bg-card-2: #1f232c;
  --border: #2a2f3a;
  --text: #e7e9ee;
  --text-dim: #9ba3b4;
  --accent: #7ad7ff;
  --pass: #34d399;
  --fail: #f87171;
  --xfail: #fbbf24;
  --infra: #a78bfa;
  --tie: #94a3b8;
  --new: #34d399;
  --old: #94a3b8;
  --pending: #475569;
}
html, body {
  margin: 0;
  padding: 0;
  background: var(--bg);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI",
    Roboto, "Helvetica Neue", Arial, "Noto Sans Hebrew", sans-serif;
  font-size: 14px;
  line-height: 1.55;
}
header.page {
  padding: 36px 48px 18px;
  border-bottom: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  gap: 8px;
}
header.page h1 {
  margin: 0;
  font-size: 28px;
  font-weight: 600;
  letter-spacing: -0.01em;
}
header.page .subtitle {
  color: var(--text-dim);
  font-size: 13px;
  letter-spacing: 0.02em;
}
header.page .meta {
  margin-top: 6px;
  display: grid;
  grid-template-columns: max-content 1fr;
  column-gap: 16px;
  row-gap: 4px;
  font-family: ui-monospace, "SFMono-Regular", "JetBrains Mono", Menlo, monospace;
  font-size: 12px;
  color: var(--text-dim);
}
header.page .meta span.k { color: var(--text-dim); }
header.page .meta span.v { color: var(--text); }

.summary-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  gap: 12px;
  padding: 18px 48px;
  border-bottom: 1px solid var(--border);
}
.summary-tile {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 14px 16px;
}
.summary-tile .label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--text-dim);
}
.summary-tile .value {
  font-size: 22px;
  font-weight: 600;
  margin-top: 4px;
}

main {
  padding: 18px 48px 80px;
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 14px;
  overflow: hidden;
}
.card .row-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 14px 18px;
  background: var(--bg-card-2);
  border-bottom: 1px solid var(--border);
}
.card .row-head .row-label {
  font-family: ui-monospace, "SFMono-Regular", monospace;
  font-size: 12px;
  color: var(--text-dim);
  letter-spacing: 0.04em;
}
.card .row-head .badges {
  display: flex;
  gap: 8px;
  align-items: center;
}
.card .question {
  padding: 14px 18px;
  font-size: 15px;
  font-weight: 500;
  direction: rtl;
  text-align: right;
  border-bottom: 1px solid var(--border);
}
.card .answers {
  display: grid;
  grid-template-columns: 1fr 1fr;
  border-bottom: 1px solid var(--border);
}
.card .answer-col {
  padding: 14px 18px;
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 80px;
}
.card .answer-col:last-child { border-right: none; }
.card .answer-col .col-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--text-dim);
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.card .answer-col .col-label .url {
  font-family: ui-monospace, "SFMono-Regular", monospace;
  text-transform: none;
  letter-spacing: 0;
  font-size: 11px;
}
.answer-text {
  white-space: pre-wrap;
  direction: rtl;
  text-align: right;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 12px;
  font-size: 13.5px;
  line-height: 1.7;
  max-height: 360px;
  overflow-y: auto;
}
.turn-block { display: flex; flex-direction: column; gap: 6px; }
.turn-block + .turn-block { margin-top: 10px; padding-top: 10px; border-top: 1px dashed var(--border); }
.turn-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--accent);
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.turn-meta { color: var(--text-dim); font-family: ui-monospace, monospace; text-transform: none; letter-spacing: 0; }
.answer-meta {
  font-size: 11px;
  color: var(--text-dim);
  font-family: ui-monospace, "SFMono-Regular", monospace;
}
.answer-meta.error { color: var(--fail); }
.answer.empty {
  color: var(--text-dim);
  font-style: italic;
  padding: 8px 12px;
}

.judgements {
  display: grid;
  grid-template-columns: 1fr 1fr;
}
.judgements .j-cell {
  padding: 12px 18px;
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.judgements .j-cell:last-child { border-right: none; }
.judgements .j-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--text-dim);
}
.judgements .j-line {
  display: flex;
  align-items: center;
  gap: 8px;
}
.judgements .j-reason {
  color: var(--text-dim);
  font-size: 13px;
  line-height: 1.5;
}

.expected {
  padding: 12px 18px;
  background: var(--bg-card-2);
  font-size: 12.5px;
  color: var(--text-dim);
  border-top: 1px solid var(--border);
  display: grid;
  grid-template-columns: max-content 1fr;
  column-gap: 12px;
  row-gap: 4px;
}
.expected .ek { color: var(--text); font-weight: 500; }
.expected .ev { color: var(--text-dim); }
.expected .ev.rtl { direction: rtl; text-align: right; }

.badge {
  display: inline-flex;
  align-items: center;
  padding: 3px 10px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.02em;
  text-transform: uppercase;
  border: 1px solid transparent;
}
.badge.pass    { background: rgba(52, 211, 153, 0.15); color: var(--pass);  border-color: rgba(52, 211, 153, 0.4); }
.badge.fail    { background: rgba(248, 113, 113, 0.15); color: var(--fail); border-color: rgba(248, 113, 113, 0.4); }
.badge.xfail   { background: rgba(251, 191, 36, 0.15); color: var(--xfail); border-color: rgba(251, 191, 36, 0.4); }
.badge.infra   { background: rgba(167, 139, 250, 0.15); color: var(--infra); border-color: rgba(167, 139, 250, 0.4); }
.badge.tie     { background: rgba(148, 163, 184, 0.15); color: var(--tie);  border-color: rgba(148, 163, 184, 0.4); }
.badge.win-new { background: rgba(52, 211, 153, 0.15); color: var(--new);   border-color: rgba(52, 211, 153, 0.4); }
.badge.win-old { background: rgba(148, 163, 184, 0.15); color: var(--old);  border-color: rgba(148, 163, 184, 0.4); }
.badge.pending { background: rgba(71, 85, 105, 0.15); color: var(--pending); border-color: rgba(71, 85, 105, 0.4); }

.score {
  font-family: ui-monospace, "SFMono-Regular", monospace;
  font-weight: 600;
  font-size: 13px;
  padding: 2px 8px;
  border-radius: 6px;
  border: 1px solid var(--border);
}
.score.good { color: var(--pass); border-color: rgba(52, 211, 153, 0.4); }
.score.mid  { color: var(--xfail); border-color: rgba(251, 191, 36, 0.4); }
.score.bad  { color: var(--fail); border-color: rgba(248, 113, 113, 0.4); }
.score.pending { color: var(--pending); }

footer {
  padding: 24px 48px 40px;
  color: var(--text-dim);
  font-size: 12px;
  border-top: 1px solid var(--border);
}
footer code { color: var(--accent); }
"""


def render(captures: list[dict], judged: dict[str, dict], title: str = "Sanity DoD", source_path: Path | None = None) -> str:
    # ── summary ──────────────────────────────────────────────────────────
    n = len(captures)
    new_wins = sum(1 for c in captures if (judged.get(str(c.get("row")), {}).get("ab_verdict") or "").upper() == "NEW")
    old_wins = sum(1 for c in captures if (judged.get(str(c.get("row")), {}).get("ab_verdict") or "").upper() == "OLD")
    ties     = sum(1 for c in captures if (judged.get(str(c.get("row")), {}).get("ab_verdict") or "").upper() == "TIE")
    def _v(c, k):
        return (judged.get(str(c.get("row")), {}).get(k) or "").upper()

    rub_pass_t1 = sum(1 for c in captures if _v(c, "rubric_verdict") in ("PASS", "PASS_T1"))
    rub_pass_t2 = sum(1 for c in captures if _v(c, "rubric_verdict") == "PASS_T2")
    rub_fail    = sum(1 for c in captures if _v(c, "rubric_verdict") == "FAIL")
    rub_xfail   = sum(1 for c in captures if _v(c, "rubric_verdict") == "XFAIL")
    rub_infra   = sum(1 for c in captures if _v(c, "rubric_verdict") == "INFRA")

    cards = []
    for cap in captures:
        row = cap.get("row")
        row_str = str(row)
        j = judged.get(row_str, {}) or {}

        ab_verdict = j.get("ab_verdict")
        ab_reason = j.get("ab_reason") or "(judge pending)"
        rub_score = j.get("rubric_score")
        rub_verdict = j.get("rubric_verdict")
        rub_reason = j.get("rubric_reason") or "(judge pending)"

        question_html = html.escape(cap.get("question") or "")
        old_block = render_answer_block(cap.get("answer_old"))
        new_block = render_answer_block(cap.get("answer_new"))
        url_old = (cap.get("answer_old") or {}).get("url") or ""
        url_new = (cap.get("answer_new") or {}).get("url") or ""

        notes = cap.get("observed_notes")
        notes_block = (
            f'<div class="ek">notes</div>'
            f'<div class="ev rtl">{html.escape(notes)}</div>'
        ) if notes else ""

        expected_behavior = cap.get("expected_behavior") or ""
        followup_prompt = cap.get("followup_prompt")
        expected_after_followup = cap.get("expected_after_followup")
        followup_block = ""
        if followup_prompt:
            followup_block += (
                f'<div class="ek">follow-up prompt</div>'
                f'<div class="ev rtl">{html.escape(followup_prompt)}</div>'
            )
        if expected_after_followup:
            followup_block += (
                f'<div class="ek">expected after follow-up</div>'
                f'<div class="ev">{html.escape(expected_after_followup)}</div>'
            )

        cards.append(
            f"""
<section class="card">
  <div class="row-head">
    <span class="row-label">row {html.escape(row_str)}</span>
    <span class="badges">
      A vs B: {verdict_badge(ab_verdict)}
      &nbsp;·&nbsp;
      rubric: {score_badge(rub_score)} {verdict_badge(rub_verdict)}
    </span>
  </div>
  <div class="question">{question_html}</div>
  <div class="answers">
    <div class="answer-col">
      <div class="col-label"><span>OLD (legacy)</span><span class="url">{html.escape(url_old)}</span></div>
      {old_block}
    </div>
    <div class="answer-col">
      <div class="col-label"><span>NEW (current)</span><span class="url">{html.escape(url_new)}</span></div>
      {new_block}
    </div>
  </div>
  <div class="judgements">
    <div class="j-cell">
      <div class="j-label">A vs B verdict</div>
      <div class="j-line">{verdict_badge(ab_verdict)}</div>
      <div class="j-reason">{html.escape(ab_reason)}</div>
    </div>
    <div class="j-cell">
      <div class="j-label">NEW vs gold-set rubric</div>
      <div class="j-line">{score_badge(rub_score)} {verdict_badge(rub_verdict)}</div>
      <div class="j-reason">{html.escape(rub_reason)}</div>
    </div>
  </div>
  <div class="expected">
    <div class="ek">expected (1-turn ideal)</div>
    <div class="ev">{html.escape(expected_behavior)}</div>
    {followup_block}
    {notes_block}
  </div>
</section>
"""
        )

    summary_tiles = "\n".join(
        f'<div class="summary-tile"><div class="label">{lbl}</div><div class="value">{val}</div></div>'
        for lbl, val in [
            ("questions",        n),
            ("NEW wins",         new_wins),
            ("OLD wins",         old_wins),
            ("ties",             ties),
            ("PASS (1-turn)",    rub_pass_t1),
            ("PASS (follow-up)", rub_pass_t2),
            ("FAIL",             rub_fail),
            ("XFAIL",            rub_xfail),
            ("INFRA",            rub_infra),
        ]
    )

    return f"""<!doctype html>
<html lang="en" dir="ltr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>{CSS}</style>
</head>
<body>
  <header class="page">
    <h1>{html.escape(title)}</h1>
    <div class="subtitle">Side-by-side gold-set capture · LLM-as-judge verdicts</div>
    <div class="meta">
      <span class="k">generated</span><span class="v">{html.escape(time.strftime("%Y-%m-%d %H:%M:%S %Z"))}</span>
      <span class="k">capture</span><span class="v">{html.escape(str(source_path)) if source_path else "&lt;in-memory&gt;"}</span>
      <span class="k">questions</span><span class="v">{n}</span>
    </div>
  </header>
  <div class="summary-grid">
    {summary_tiles}
  </div>
  <main>
    {''.join(cards)}
  </main>
  <footer>
    Generated by <code>scripts/render-sanity-html.py</code> from the sanity-dod skill.
  </footer>
</body>
</html>
"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--capture", required=True, type=Path)
    p.add_argument("--judged", type=Path, default=None)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--title", default="Sanity DoD — old vs new")
    args = p.parse_args()

    if not args.capture.exists():
        print(f"capture file not found: {args.capture}", file=sys.stderr)
        return 2
    captures = json.loads(args.capture.read_text())
    captures.sort(key=lambda r: r.get("row", 0))

    judged = {}
    if args.judged and args.judged.exists():
        raw = json.loads(args.judged.read_text())
        # Accept either {row: {...}} or [{row: ..., ...}, ...]
        if isinstance(raw, list):
            judged = {str(r.get("row")): r for r in raw}
        else:
            judged = {str(k): v for k, v in raw.items()}

    args.out.write_text(render(captures, judged, args.title, args.capture))
    print(str(args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())

# Prompt Management — `agent.txt` version-control workflow

The Botnim agent's system prompt lives in git at `specs/unified/agent.txt`
(~263 lines). It combines the legal and budget domains in a single assistant
and is the only bot deployed to staging/production.

---

## The workflow

### 1. Edit

- Open the relevant `specs/<bot>/agent.txt`.
- Each major section is delimited by a Markdown header (`## Name`) with an immediately-preceding HTML comment (`<!-- ... -->`) that explains the section's intent for humans. Keep both.
- Hebrew content inside the prompt is the authoritative copy — don't lose accents, nikud, or whitespace when editing.
- If you add/rename/remove a tool, make sure every `operationId` mentioned in the prompt is still present in the OpenAPI spec the agent loads (`specs/openapi/*.yaml` → uploaded as Agent Actions by `LibreChat/scripts/seed-botnim-agent.js`). A mismatch means the bot will invent a tool call that nothing answers.

### 2. Pull request

- Branch from `main`: `git checkout -b prompt/<short-name>`.
- Commit: `git add specs/<bot>/agent.txt && git commit -m "prompt(<bot>): <what changed, why>"`.
- Open a PR on `whiletrue-industries/rebuilding-bots`.

### 3. Review

Reviewer should verify:

- **OperationId integrity** — every tool name the prompt tells the bot to call exists in `specs/openapi/*.yaml`. Grep is fine for v1:
  ```bash
  grep -oE '[a-zA-Z_]+_[a-zA-Z_]+' specs/unified/agent.txt | sort -u | while read id; do
    grep -q "$id" specs/openapi/*.yaml || echo "MISSING in openapi: $id"
  done
  ```
- **Hebrew grammar** — native-speaker eye on any new Hebrew sentences.
- **Routing logic** — if the change touches "Domain Routing" / "Search Strategy" / "Operating Protocol", confirm the triage rules don't contradict earlier sections.
- **Length budget** — the unified prompt is near the model's context ceiling for very long conversations; don't add a new 30-line section without also trimming something equivalent.

### 4. Merge

- Squash-merge to `main` using a descriptive title: `prompt(unified): tighten ethics-committee routing`.
- The squash gives a clean `git log` entry and keeps rollback trivial.

### 5. Deploy

- Production: GitHub Actions auto-deploys on merge to `main` (see `.github/workflows/deploy-production.yml`).
- Staging: same workflow, triggered on merge. Or use `make deploy-staging TAG=<image-tag>` from the parlibot root if you're promoting an ad-hoc image.

The deploy updates the OpenAI Assistant's `instructions` field — the new prompt is in effect on the **next** chat turn (existing running turns use the prompt they started with).

### 6. Verify

- Open https://botnim.co.il (prod) or https://botnim.staging.build-up.team (staging).
- Ask a question that exercises the changed section (if you tweaked ethics routing, ask an ethics question; if you tweaked budget dataset selection, ask a budget question).
- If something looks wrong, go to § Rollback.

---

## Version history

`git log` is the source of truth. Useful incantations:

```bash
# Every change to the unified prompt, newest first:
git log --follow -p -- specs/unified/agent.txt

# Who touched line 142 (the line that tells the bot when to refuse):
git blame specs/unified/agent.txt -L 142,142

# Everything that landed in the last month:
git log --since='1 month ago' --oneline -- specs/*/agent.txt

# Diff two arbitrary versions side-by-side:
git diff <old-sha>..<new-sha> -- specs/unified/agent.txt
```

---

## Rollback

A bad prompt change is just a revert + redeploy:

```bash
# 1. Find the offending commit
git log --oneline -- specs/unified/agent.txt

# 2. Revert it (creates a new commit undoing the change)
git revert <sha>

# 3. Push to main
git push origin main
```

GitHub Actions picks up the revert commit and redeploys automatically. From the user's perspective, the bot is back on the prior prompt within ~2 minutes.

If the revert itself introduces a merge conflict (someone else edited the prompt between `<sha>` and HEAD), resolve it in a local branch, open a PR, and merge normally.

---

## Section map — `unified/agent.txt`

Keep this in sync when you restructure the prompt. Line numbers are approximate.

| Line | Section | Purpose |
|---:|---|---|
| 1-10 | Preamble | Identity, dual-domain contract, retrieval-only constraint |
| 12-35 | Core Characteristics | Tone, language, definitions of "relevant" / "refuse" / "transport error" |
| 37-70 | Search Strategy | How to pick the right tool per question; retry + vary-query policy |
| 72-95 | Domain Routing | Legal vs. budget triage; dual-track fallback trigger |
| 97-147 | A. Legal Domain Workflow | Retrieve → generate → close for Takanon / ethics / legal text |
| 148-209 | B. Budget Domain Workflow | Common-knowledge → DatasetInfo → query / full-text-search |
| 210-226 | C. Dual-Track Mode | Cross-domain questions (e.g., "budget allocated by the ethics committee") |
| 227-242 | Citation & Link Rules (Legal) | How to cite clauses, committee decisions, and source URLs |
| 243-255 | Forbidden Behaviors | Hard nos (no pretraining answers, no improvisation, no paraphrasing law) |
| 256-263 | Summary of Defaults | Terminal cheat-sheet: default year, default language, default refusal phrase |

---

## CI / future improvements

This workflow currently relies on human review. A follow-up (see Monday `2814167008` / "Contract tests") would add:

- `npm test:contracts` — static validation of each `specs/openapi/*.yaml` + cross-check operationIds against `agent.txt`.
- `.github/workflows/prompt-check.yml` — run the contract tests on every PR that touches `specs/*/agent.txt`.

Out of scope for this task; file it separately when/if we want CI gating.

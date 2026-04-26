# Unified bot — agent prompt source

The agent prompt is no longer maintained as a file in this repo.

After the Aurora migration (2026-04-26, Monday item 2870735681), the
authoritative copy lives in the `agent_prompts` table in the
`botnim_staging` / `botnim_prod` databases. Edit via the LibreChat
admin UI at `/admin/prompts`.

Older history of this prompt is preserved in git up to commit
`bbc31748a6f21a5d0bdfbc124fc06b2f6beedd7c` (run `git log -- specs/unified/agent.txt` to find it).

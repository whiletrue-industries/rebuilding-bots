-- One-shot backfill for documents.source_id (added in migration 0005).
-- Idempotent: only touches rows where source_id IS NULL. Re-runnable.
-- Run against staging then production AFTER alembic upgrade head.
--
-- Single-source contexts: assign the fixed source_id. Multi-source
-- contexts (legal_text, common_takanon_knowledge): derive from the
-- markdown '# <page_name>' header that the wikitext extractor writes
-- as the first line of every chunk. Lexicon / google-sheet docs in
-- common_takanon_knowledge that don't have a wikisource header keep
-- '(unknown)' until the next normal sync overwrites them properly.
--
-- Anything still NULL after the below stays NULL — _write_snapshots
-- groups those under '(unknown)' and a future routine sync will
-- populate them properly via the threaded source_id path.
SELECT 1;
UPDATE documents SET source_id = 'common-knowledge' WHERE source_id IS NULL AND context_id IN (SELECT id FROM contexts WHERE bot='unified' AND name='common_budget_knowledge');
UPDATE documents SET source_id = 'knesset_legal_advisor' WHERE source_id IS NULL AND context_id IN (SELECT id FROM contexts WHERE bot='unified' AND name='legal_advisor_opinions');
UPDATE documents SET source_id = 'knesset_legal_advisor_letters' WHERE source_id IS NULL AND context_id IN (SELECT id FROM contexts WHERE bot='unified' AND name='legal_advisor_letters');
UPDATE documents SET source_id = 'knesset_committee_decisions' WHERE source_id IS NULL AND context_id IN (SELECT id FROM contexts WHERE bot='unified' AND name='committee_decisions');
UPDATE documents SET source_id = 'ethics_committee_decisions' WHERE source_id IS NULL AND context_id IN (SELECT id FROM contexts WHERE bot='unified' AND name='ethics_decisions');
UPDATE documents SET source_id = 'bk_csv' WHERE source_id IS NULL AND context_id IN (SELECT id FROM contexts WHERE bot='unified' AND name='government_decisions');
UPDATE documents SET source_id = (regexp_match(content, '^# (\S+)'))[1] WHERE source_id IS NULL AND context_id IN (SELECT id FROM contexts WHERE bot='unified' AND name IN ('legal_text', 'common_takanon_knowledge')) AND content ~ '^# \S';

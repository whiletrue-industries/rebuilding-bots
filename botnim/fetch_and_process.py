
from pathlib import Path

import yaml

from .document_parser.lexicon.lexicon import scrape_lexicon
from .config import SPECS


def fetch_and_process_source(environment, config_dir, context_name, source, kind):
    fetcher = source.get('fetcher')
    if not fetcher:
        return
    output_base_dir = config_dir / 'extraction'
    fetcher_kind = fetcher.pop('kind')
    if kind not in ['all', fetcher_kind]:
        return
    # Lexicon scraping is intentionally excluded from the routine
    # `kind=all` refresh path — its 5s/entry sleep adds ~50min per fap
    # and the upstream rarely changes. Run it on demand via
    # `botnim fetch-and-process ... lexicon` (or the daily Lambda when
    # we wire a separate cadence). This keeps the routine sync time
    # bounded by the migrated-context LLM extraction work.
    if fetcher_kind == 'lexicon' and kind == 'all':
        print(f"  skipping lexicon under kind=all (run with kind=lexicon to refresh)")
        return
    if fetcher_kind == 'wikitext':
        from .document_parser.wikitext.pipeline_config import Environment, WikitextProcessorConfig
        from .document_parser.wikitext.process_document import WikitextProcessor
        input_url = fetcher['input_url']
        config = WikitextProcessorConfig(
            input_url=input_url,
            output_base_dir=output_base_dir,
            content_type='סעיף',
            environment=Environment(environment),  # Convert string to enum
            model='gpt-4.1-mini',
            max_tokens=None
        )
        runner = WikitextProcessor(config)
        runner.run(generate_markdown=False)
    elif fetcher_kind == 'pdf':
        from .document_parser.pdfs.process_pdfs import process_pdf_source
        from .document_parser.pdfs.pdf_extraction_config import SourceConfig
        output_csv_path = config_dir / source['source']
        config = SourceConfig(**fetcher, output_csv_path=output_csv_path)
        process_pdf_source(config)
    elif fetcher_kind == 'lexicon':
        scrape_lexicon(output_path=config_dir / source['source'])
    elif fetcher_kind == 'bk_csv':
        # BudgetKey single-CSV datapackage (e.g. government_decisions). Different
        # from `pdf` which downloads PDF binaries listed in an index.csv and runs
        # OpenAI extraction per file — `bk_csv` consumes a single CSV resource
        # whose rows are already parsed by BudgetKey upstream. See
        # botnim/document_parser/bk_datapackage/process_bk_csv.py for details.
        from .document_parser.bk_datapackage.process_bk_csv import process_bk_csv_source
        output_csv_path = config_dir / source['source']
        process_bk_csv_source(output_csv_path=output_csv_path, **fetcher)
    elif fetcher_kind == 'knesset_odata':
        # Knesset ParliamentInfo OData service (live). Fetches plenum-session
        # entities + their agenda items joined into one CSV row per
        # (session, item) pair. See
        # botnim/document_parser/knesset_odata/process_odata.py for details.
        from .document_parser.knesset_odata.process_odata import process_knesset_odata_source
        output_csv_path = config_dir / source['source']
        process_knesset_odata_source(output_csv_path=output_csv_path, **fetcher)
    elif fetcher_kind == 'knesset_protocols':
        # Knesset committee + plenum protocol transcripts. Fetches the
        # OData document index, downloads each .doc (actually OOXML)
        # from fs.knesset.gov.il, parses with python-docx into per-
        # speaker-turn rows. See
        # botnim/document_parser/knesset_protocols/process_protocols.py.
        from .document_parser.knesset_protocols.process_protocols import process_knesset_protocols_source
        output_csv_path = config_dir / source['source']
        process_knesset_protocols_source(output_csv_path=output_csv_path, **fetcher)
    elif fetcher_kind == 'knesset_apps_committee':
        from .document_parser.knesset_apps.committee_decisions_json import (
            fetch_committee_decisions_index, CommitteeDecisionsConfig,
        )
        output_csv_path = config_dir / source['source']
        fetch_committee_decisions_index(
            CommitteeDecisionsConfig(output_csv_path=output_csv_path, **fetcher)
        )

    elif fetcher_kind == 'knesset_apps_ethics':
        from .document_parser.knesset_apps.ethics_decisions_html import (
            fetch_ethics_decisions_index, EthicsDecisionsConfig,
        )
        output_csv_path = config_dir / source['source']
        fetch_ethics_decisions_index(
            EthicsDecisionsConfig(output_csv_path=output_csv_path, **fetcher)
        )

    elif fetcher_kind == 'knesset_sharepoint_legal_advisor':
        from .document_parser.knesset_sharepoint.scraper import scrape_legal_advisor_opinions
        output_csv_path = config_dir / source['source']
        scrape_legal_advisor_opinions(output_csv_path=output_csv_path, **fetcher)

    elif fetcher_kind == 'knesset_sharepoint_legal_advisor_letters':
        from .document_parser.knesset_sharepoint.scraper import scrape_legal_advisor_letters
        output_csv_path = config_dir / source['source']
        scrape_legal_advisor_letters(output_csv_path=output_csv_path, **fetcher)

    elif fetcher_kind == 'indexed_pdf':
        from .document_parser.pdfs.process_pdfs import process_pdf_source
        from .document_parser.pdfs.pdf_extraction_config import SourceConfig
        from .storage import get_artifact_store
        from .storage.csv_writer import key_for_extraction
        output_csv_path = config_dir / source['source']
        # local_index_csv_path is documented as relative-to-config_dir (see
        # pdf_extraction_config.py). Resolve it here so process_pdf_source
        # (which only sees the absolute SourceConfig) can find the index Stage
        # 1 wrote — the container cwd is /app, not config_dir.
        fetcher_kw = dict(fetcher)
        raw_idx = fetcher_kw.get('local_index_csv_path')
        if raw_idx:
            idx = Path(raw_idx)
            if not idx.is_absolute():
                idx = config_dir / idx
            fetcher_kw['local_index_csv_path'] = str(idx)
        config = SourceConfig(**fetcher_kw, output_csv_path=output_csv_path)
        # bot = config_dir.name (e.g. "unified"); relpath = source['source']
        # (e.g. "extraction/foo.csv") → key = "cache/unified/extraction/foo.csv"
        store = get_artifact_store()
        artifact_key = key_for_extraction(config_dir.name, source['source'])
        process_pdf_source(config, store=store, key=artifact_key)
    elif fetcher_kind == 'gov_il_decisions':
        # First-party gov.il scrape that writes DIRECTLY to Aurora,
        # bypassing the extraction/<x>.csv → botnim sync pipeline.
        # See botnim/document_parser/gov_il_decisions/process.py for
        # rationale: 26K decisions + LLM-derived categories don't fit
        # the CSV-in-repo pattern, and the bootstrap source data must
        # never live in ECS (operator-only). Run via deploy.sh phase
        # 8a alongside other contexts; sync runs after and writes
        # context_snapshots so /admin/sources reflects this context.
        from .document_parser.gov_il_decisions.process import process_gov_il_decisions_source
        process_gov_il_decisions_source(environment=environment, **fetcher)

def fetch_and_process_context(environment, context, config_dir: Path, kind):
    context_name = context['name']
    if 'sources' in context:
        for source in context['sources']:
            fetch_and_process_source(environment, config_dir, context_name, source, kind)
    else:
        fetch_and_process_source(environment, config_dir, context_name, context, kind)

def fetch_and_process(environment, bot, context, kind):
    specs = []
    config_files = [(d, d / 'config.yaml') for d in SPECS.iterdir() if d.is_dir() and (d / 'config.yaml').exists() and bot in ['all', d.name]]
    for config_dir, conf in config_files:
        with conf.open() as f:
            spec = yaml.safe_load(f)
            contexts = spec['context']
            for c in contexts:
                if context in ['all', c['slug']]:
                    specs.append((config_dir, c))
    print(f"Found {len(specs)} contexts to process")
    # Per-context error isolation. A single upstream going stale (e.g. an
    # empty index.csv on the BudgetKey datapackage for one context) used to
    # abort the whole run via EmptyUpstreamIndex, leaving downstream contexts
    # un-fetched and a subsequent `botnim sync` failing on FileNotFoundError.
    # We now log + skip the failing context and let the others proceed; the
    # per-context CSV stays as-is (the safety guard's whole point), and the
    # operator gets a clear failure summary at the end.
    failures: list[tuple[str, str, Exception]] = []
    for config_dir, spec in specs:
        ctx_name = spec.get('name', spec.get('slug', '?'))
        try:
            fetch_and_process_context(environment, spec, config_dir, kind)
        except Exception as e:
            print(f"WARNING: context {config_dir.name}/{ctx_name} failed: {type(e).__name__}: {e}")
            failures.append((config_dir.name, ctx_name, e))
    if failures:
        print(f"\nfetch-and-process completed with {len(failures)} context failure(s):")
        for bot_name, ctx_name, err in failures:
            print(f"  - {bot_name}/{ctx_name}: {type(err).__name__}: {err}")
        # Do not raise — let downstream sync proceed with whatever CSVs
        # successfully refreshed. Sync itself will skip contexts whose
        # CSVs are missing (already does — see collect_sources.py).


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
    if fetcher_kind == 'wikitext':
        from .document_parser.wikitext.pipeline_config import Environment, WikitextProcessorConfig
        from .document_parser.wikitext.process_document import WikitextProcessor
        input_url = fetcher['input_url']
        config = WikitextProcessorConfig(
            input_url=input_url,
            output_base_dir=output_base_dir,
            content_type='סעיף',
            environment=Environment(environment),  # Convert string to enum
            model='gpt-4.1',
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
    for config_dir, spec in specs:
        fetch_and_process_context(environment, spec, config_dir, kind)

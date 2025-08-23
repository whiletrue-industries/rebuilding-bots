
from pathlib import Path

import yaml

from .document_parser.wikitext.pipeline_config import Environment, WikitextProcessorConfig
from .document_parser.wikitext.process_document import WikitextProcessor
from .config import SPECS


def fetch_and_process_source(environment, config_dir, context_name, source, kind):
    fetcher = source.get('fetcher')
    print(fetcher)
    if not fetcher:
        return
    fetcher_kind = fetcher.get('kind')
    if kind not in ['all', fetcher_kind]:
        return
    if fetcher_kind == 'wikitext':
        input_url = fetcher['input_url']
        output_base_dir = config_dir / 'extraction'
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

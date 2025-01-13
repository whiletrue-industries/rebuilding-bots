import click
from .sync import sync_agents
from .benchmark.runner import run_benchmarks
from .kb.download_sources import download_sources
from .config import SPECS
import shutil
import yaml

@click.group()
def cli():
    """A simple CLI tool."""
    pass

# Sync command, receives two arguments: production/staging and a list of bots to sync ('budgetkey'/'takanon' or 'all')
@cli.command()
@click.argument('environment', type=click.Choice(['staging', 'production']))
@click.argument('bots', default='all')
def sync(environment, bots):
    """Sync all or specific bots with their configurations"""
    sync_agents(environment, bots)

# Run benchmarks command, receives three arguments: production/staging, a list of bots to run benchmarks on ('budgetkey'/'takanon' or 'all') and whether to run benchmarks on the production environment to work locally (true/false)
@cli.command()
@click.argument('environment', type=click.Choice(['production', 'staging']))
@click.argument('bots', type=click.Choice(['budgetkey', 'takanon', 'all']))
@click.option('--local', is_flag=True, default=False, help='Run benchmarks locally')
@click.option('--reuse-answers', is_flag=True, default=False)
@click.option('--select', type=click.STRING, default='failed', help='failed/all/AirTable record ID')
@click.option('--concurrency', type=click.INT, default=None)
def benchmarks(environment, bots, local, reuse_answers, select, concurrency):
    """Run benchmarks on bots."""
    click.echo(f"Running benchmarks on {bots} in {environment} (save results locally: {local}, reuse answers: {reuse_answers}, select: {select})")
    run_benchmarks(environment, bots, local, reuse_answers, select, concurrency)

@cli.command()
@click.argument('bots', type=click.Choice(['budgetkey', 'takanon', 'all']), default='all')
def download(bots):
    """Download external sources for specific or all bots"""
    click.echo(f"Downloading external sources for {bots}...")
    
    # Clean up existing downloaded content based on config
    for config_file in SPECS.glob('*/config.yaml'):
        if bots != 'all' and config_file.parent.name != bots:
            continue
            
        with config_file.open() as f:
            config = yaml.safe_load(f)
            
        if config.get('context'):
            for context in config['context']:
                if 'source' in context:
                    target_dir = config_file.parent / f"{context['name']}_split"
                    if target_dir.exists():
                        shutil.rmtree(target_dir)
    
    download_sources(SPECS, bots)

def main():
    cli()

if __name__ == '__main__':
    main()

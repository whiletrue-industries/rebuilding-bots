"""Sync pipeline: specs -> Elasticsearch indices + published bot config.

Before the Responses API migration this module ALSO maintained remote
OpenAI Assistant objects via ``client.beta.assistants.*``. The Assistants
API retires 2026-08-26, and its server-side replacement ("Prompts") is
dashboard-only (no public REST endpoint, no SDK method; see
``bot_config.py`` module docstring for the full research note).

Post-migration, sync does two things:

1. Build / refresh the Elasticsearch indices referenced by the bot's
   search tools. (Unchanged.)
2. Load the bot's config from ``specs/`` into a
   :class:`~botnim.bot_config.BotConfig` and write the canonical config
   JSON to ``specs/.published/<env>/<bot>.json``. Downstream consumers
   (LibreChat, the FastAPI ``/botnim/config/<bot>`` endpoint) read that
   file at chat time and pass its contents directly to
   ``client.responses.create(model=..., instructions=..., tools=...)``.

No ``client.beta.assistants.*`` calls remain.
"""
from __future__ import annotations

import yaml

from .bot_config import BotConfig, load_bot_config, publish_bot_config
from .config import SPECS, get_logger, get_openai_client, is_production
from .vector_store import VectorStoreES, VectorStoreOpenAI

logger = get_logger(__name__)


def _sync_vector_store(config: dict, config_dir, backend: str, environment: str,
                       replace_context, reindex: bool) -> None:
    """Run the backend-specific vector-store update for a bot's contexts.

    The returned tools/tool_resources from :meth:`vector_store_update` are
    ignored here: with the migration to Responses API, search-tool
    definitions are owned by :mod:`botnim.bot_config` instead of by the
    vector store. The ES indexing side-effects (embedding, upserting,
    deleting) are still what we need.
    """
    if not config.get('context'):
        return
    if backend == 'openai':
        client = get_openai_client(environment)
        vs = VectorStoreOpenAI(config, config_dir, is_production(environment), client)
    elif backend == 'es':
        vs = VectorStoreES(config, config_dir, environment=environment)
    else:
        raise ValueError(f'Unsupported backend: {backend}')
    vs.vector_store_update(
        config['context'],
        replace_context=replace_context,
        reindex=reindex,
    )


def publish_bot(bot_slug: str, environment: str) -> BotConfig:
    """Load, validate, and publish a bot's Responses-API config bundle.

    Replaces the pre-migration ``client.beta.assistants.update()`` call.
    Writes the canonical JSON to ``specs/.published/<env>/<bot>.json`` and
    returns the loaded :class:`BotConfig` for logging / inspection.
    """
    config = load_bot_config(bot_slug, environment)
    path = publish_bot_config(config)
    logger.info(
        'Bot config published: slug=%s env=%s model=%s tools=%d instructions_chars=%d path=%s',
        config.slug, config.environment, config.model,
        len(config.tools), len(config.instructions), path,
    )
    # Keep stdout-friendly marker so CI logs match expectations.
    print(f'Bot config published: {config.slug} ({config.environment}) -> {path}')
    return config


def sync_agents(environment: str, bots: str, backend: str = 'es',
                replace_context=False, reindex: bool = False) -> None:
    """Sync one or more bots: ES indices + published config.

    Parameters
    ----------
    environment:
        Target environment, typically ``"staging"`` or ``"production"``.
    bots:
        A single bot slug (e.g. ``"unified"``) or ``"all"``.
    backend:
        Vector-store backend: ``"es"`` (default) or ``"openai"``.
    replace_context:
        Forwarded to the vector store's update logic.
    reindex:
        Forwarded to the vector store's update logic; forces a full rebuild.
    """
    for config_fn in SPECS.glob('*/config.yaml'):
        config_dir = config_fn.parent
        bot_id = config_dir.name
        if bots not in ('all', bot_id):
            continue
        with config_fn.open() as f:
            raw = yaml.safe_load(f)

        logger.info('Syncing bot: %s (env=%s, backend=%s)', bot_id, environment, backend)
        print(f'Syncing bot: {bot_id} (env={environment}, backend={backend})')

        # 1. Elasticsearch / vector-store side-effects.
        _sync_vector_store(
            raw, config_dir, backend, environment,
            replace_context=replace_context, reindex=reindex,
        )

        # 2. Publish the canonical Responses-API bot config.
        publish_bot(bot_id, environment)

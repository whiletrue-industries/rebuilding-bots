"""Bot configuration loader for the Responses API.

This module replaces the Assistants API "Assistant" object. Instead of creating
or updating a remote OpenAI object, we build a local, code-managed configuration
bundle (model + instructions + tool definitions) that can be passed directly to
``client.responses.create()`` at chat time.

Rationale
---------
The Assistants API retires 2026-08-26. Its server-side configuration replacement
("Prompts") is dashboard-only: there is no public endpoint or SDK method to
create or update a Prompt from code (``/v1/dashboard/prompts`` rejects secret
API keys; ``client.prompts`` does not exist in the ``openai`` SDK as of
2.32.0). To preserve our "edit agent.txt -> PR -> CI sync -> production picks
it up" flow, we keep the source of truth in ``specs/`` and load it at call
time into a :class:`BotConfig`.

Tool format
-----------
Assistants API used a nested function shape::

    {"type": "function", "function": {"name": ..., "parameters": ...}}

Responses API uses a flat shape::

    {"type": "function", "name": ..., "description": ..., "parameters": ...}

:func:`load_bot_config` emits the flat Responses API shape.

Usage
-----
::

    from botnim.bot_config import load_bot_config
    cfg = load_bot_config("unified", "staging")
    response = client.responses.create(
        model=cfg.model,
        instructions=cfg.instructions,
        tools=cfg.tools,
        input=[{"role": "user", "content": question}],
    )
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

import yaml

from .config import SPECS, is_production, get_logger

logger = get_logger(__name__)

#: Default model for all bots. Kept in sync with the historical Assistants-API
#: default (see ``sync.py`` pre-migration).
DEFAULT_MODEL = 'gpt-5.4-mini'

#: Default sampling temperature. Matches the pre-migration value.
DEFAULT_TEMPERATURE = 0.00001


@dataclass
class BotConfig:
    """Canonical, code-managed bot configuration.

    The fields map 1:1 to arguments accepted by ``client.responses.create()``
    (``model``, ``instructions``, ``tools``, ``temperature``), plus local
    metadata (``slug``, ``name``, ``description``) used by the sync step and
    any downstream registry.
    """

    slug: str
    name: str
    description: str
    model: str
    instructions: str
    tools: list[dict[str, Any]] = field(default_factory=list)
    temperature: float = DEFAULT_TEMPERATURE
    environment: str = 'staging'

    def to_dict(self) -> dict[str, Any]:
        """Return a plain-dict representation, stable for JSON serialization."""
        return asdict(self)

    def to_json(self) -> str:
        """Return a pretty-printed JSON string (``ensure_ascii=False`` for Hebrew)."""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)


def openapi_to_tools(openapi_spec: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert an OpenAPI v3 spec into Responses-API flat tool definitions.

    This replaces the pre-migration helper in ``sync.py`` which emitted the
    nested Assistants-API shape. The output can be passed directly to
    ``responses.create(tools=...)``.
    """
    tools: list[dict[str, Any]] = []
    for path in openapi_spec.get('paths', {}).values():
        for method in path.values():
            operation_id = method['operationId']
            operation_desc = method.get('description', '')
            parameters = method.get('parameters', [])
            properties = {
                param['name']: {
                    'type': param['schema']['type'],
                    'description': param.get('description', ''),
                }
                for param in parameters
            }
            required = [p['name'] for p in parameters if p.get('required')]
            tools.append({
                'type': 'function',
                'name': operation_id,
                'description': operation_desc,
                'parameters': {
                    'type': 'object',
                    'properties': properties,
                    'required': required,
                },
            })
    return tools


def _encode_index_name(bot_slug: str, context_slug: str, environment: str) -> str:
    """Stable copy of :meth:`VectorStoreES.encode_index_name`.

    Duplicated here to keep ``bot_config`` importable without the heavy
    ``dataflows`` / ``elasticsearch`` dependency chain. If the canonical
    implementation changes, update this function to match.
    """
    parts = [bot_slug, context_slug]
    if not is_production(environment):
        parts.append('dev')
    return '__'.join(parts)


def _search_tool_for_context(bot_slug: str, context_slug: str, environment: str,
                             context_cfg: dict[str, Any]) -> dict[str, Any]:
    """Build a Responses-API flat ``search_<bot>__<context>[__dev]`` tool.

    Mirrors :meth:`botnim.vector_store.vector_store_es.VectorStoreES.update_tools`
    but emits the flat shape. The returned dict is safe to append to
    :attr:`BotConfig.tools`.
    """
    # Import lazily to keep the top-level module importable when the heavy ES /
    # dataflows stack is not installed (e.g. in LibreChat runtime).
    from .vector_store.search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE

    index_name = _encode_index_name(bot_slug, context_slug, environment)
    description = context_cfg.get('description') or context_cfg.get('name', context_slug)
    examples = context_cfg.get('examples')
    if examples:
        description = f"{description}. Examples: {examples}"

    return {
        'type': 'function',
        'name': f'search_{index_name}',
        'description': description,
        'parameters': {
            'type': 'object',
            'properties': {
                'query': {
                    'type': 'string',
                    'description': 'The query string to use for semantic/free text search',
                },
                'search_mode': {
                    'type': 'string',
                    'description': "Search mode; see 'list-modes' CLI for descriptions.",
                    'enum': [mode.name for mode in SEARCH_MODES.values()],
                    'default': DEFAULT_SEARCH_MODE.name,
                },
                'num_results': {
                    'type': 'integer',
                    'description': 'Number of results to return. Leave empty to use the default for the search mode.',
                    'default': 7,
                },
            },
            'required': ['query'],
        },
    }


def _load_instructions_from_aurora(bot_slug: str) -> str:
    """Assemble the bot's system prompt from the agent_prompts table.

    Returns the joined body text (one section per row, separated by blank
    lines + the same `---` rules the source file used) when one or more
    active rows exist for `agent_type=bot_slug`. Returns an empty string
    when the table is empty, has no active rows, or the DB is unreachable
    — caller falls back to the file-based prompt in those cases (covers
    local-dev pytest runs, first-deploy bootstrap, and any rollback path).

    Local import + bare try/except so that environments without Aurora
    connectivity (e.g. unit tests that mock `botnim.sync` at the module
    level) don't blow up at module-load time.
    """
    try:
        from sqlalchemy import text as _text
        from .db.session import get_session
        with get_session() as sess:
            rows = sess.execute(_text(
                "SELECT body FROM agent_prompts "
                "WHERE agent_type = :a AND active = true "
                "ORDER BY ordinal"
            ), {"a": bot_slug}).fetchall()
    except Exception:
        return ''
    if not rows:
        return ''
    return '\n\n---\n\n'.join(r[0] for r in rows if r[0])


def load_bot_config(bot_slug: str, environment: str,
                    model: str | None = None,
                    temperature: float | None = None) -> BotConfig:
    """Load a :class:`BotConfig` from ``specs/<bot_slug>/``.

    Parameters
    ----------
    bot_slug:
        Directory name under ``specs/``, e.g. ``"unified"``.
    environment:
        ``"production"`` / ``"staging"`` / ``"local"``. Controls the
        ``__dev`` suffix on search-tool names and the ``__dev`` / Hebrew
        suffix on the bot display name.
    model:
        Optional model override. Defaults to :data:`DEFAULT_MODEL`.
    temperature:
        Optional temperature override.

    Raises
    ------
    FileNotFoundError:
        If ``specs/<bot_slug>/config.yaml`` does not exist.
    """
    bot_dir = SPECS / bot_slug
    config_path = bot_dir / 'config.yaml'
    if not config_path.is_file():
        raise FileNotFoundError(f'No config.yaml at {config_path}')

    with config_path.open() as f:
        cfg = yaml.safe_load(f)

    # Prefer the Aurora-stored prompt (post-Aurora-migration design).
    # When agent_prompts has active rows for this bot, assemble them into
    # the system prompt; otherwise fall back to the file at cfg['instructions']
    # (which after the migration is just the banner pointer, but useful
    # in local-dev / first-run environments where Aurora is empty).
    instructions = _load_instructions_from_aurora(bot_slug)
    if not instructions:
        instructions_path = bot_dir / cfg['instructions']
        instructions = instructions_path.read_text()
    # Match pre-migration behavior: in production, strip any __dev markers the
    # prompt author used to annotate dev-only guidance.
    if is_production(environment):
        instructions = instructions.replace('__dev', '')

    name = cfg['name']
    if not is_production(environment):
        name = f'{name} - פיתוח'

    tools: list[dict[str, Any]] = []

    # Context search tools (ES-backed).
    for context_cfg in cfg.get('context', []):
        tools.append(_search_tool_for_context(
            bot_slug=bot_slug,
            context_slug=context_cfg['slug'],
            environment=environment,
            context_cfg=context_cfg,
        ))

    # Built-in / OpenAPI tools.
    for tool in cfg.get('tools', []) or []:
        if tool == 'code-interpreter':
            # Responses API uses "code_interpreter" built-in type; Assistants
            # API used the same spelling, so this string is unchanged.
            tools.append({'type': 'code_interpreter'})
            continue
        openapi_path = (SPECS / 'openapi' / tool).with_suffix('.yaml')
        if not openapi_path.is_file():
            raise FileNotFoundError(f'OpenAPI spec not found: {openapi_path}')
        with openapi_path.open() as f:
            spec = yaml.safe_load(f)
        tools.extend(openapi_to_tools(spec))

    return BotConfig(
        slug=bot_slug,
        name=name,
        description=cfg.get('description', ''),
        model=model or DEFAULT_MODEL,
        instructions=instructions,
        tools=tools,
        temperature=DEFAULT_TEMPERATURE if temperature is None else temperature,
        environment=environment,
    )


def published_config_path(bot_slug: str, environment: str,
                          base_dir: Path | None = None) -> Path:
    """Return the on-disk location for a published bot config.

    The path is under ``specs/.published/`` by default, which is the canonical
    place for sync output to land so downstream consumers (LibreChat, the
    FastAPI ``/botnim/config/<bot>`` endpoint) can read it without calling
    OpenAI.
    """
    base = base_dir if base_dir is not None else (SPECS / '.published')
    return base / environment / f'{bot_slug}.json'


def publish_bot_config(config: BotConfig, base_dir: Path | None = None) -> Path:
    """Write ``config`` to the published-config directory as JSON.

    Returns the path that was written. Creates parent directories as needed.
    """
    path = published_config_path(config.slug, config.environment, base_dir=base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(config.to_json())
    logger.info('Published bot config: %s', path)
    return path

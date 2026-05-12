import asyncio
import dataclasses
import json
import logging
import os
import threading
import requests
import yaml as _yaml
from fastapi import APIRouter, FastAPI, HTTPException, Response, Query, Body, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Dict, Any, Optional
from firebase_admin import firestore
from pydantic import BaseModel

from resolve_firebase_user import FireBaseUser
from refresh_auth import require_refresh_api_key
from sanity_auth import require_sanity_api_key
from botnim.query import run_query, government_distribution_sidecar
from botnim.vector_store.search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE
from botnim.bot_config import load_bot_config
from botnim.config import AVAILABLE_BOTS, VALID_ENVIRONMENTS, DEFAULT_ENVIRONMENT
from botnim.fetch_and_process import fetch_and_process
from botnim.sync import sync_agents
from botnim.word_doc.models import WordDocRequest, WordDocResponse
from botnim.word_doc.render import render_word_doc, sanitize_filename
from botnim.word_doc.storage import upload_word_doc

logger = logging.getLogger(__name__)

app = FastAPI(openapi_url=None, redirect_slashes=False)

from botnim.observability.tracing import init_tracing
from botnim.observability.middleware import install_trace_middleware
init_tracing(app)
install_trace_middleware(app)

# Enable CORS:
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this to the specific origins you want to allow
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
@app.get("/botnim/health")
async def health():
    return "OK"


# ---------------------------------------------------------------------------
# Bot config endpoints (post-Assistants-API migration)
#
# These endpoints expose the code-managed Responses-API BotConfig objects that
# replace the server-side OpenAI Assistant objects. Consumers (LibreChat) use
# them to build client.responses.create(model=..., instructions=..., tools=...)
# calls at chat time.
# ---------------------------------------------------------------------------


@app.get("/bots")
@app.get("/botnim/bots")
async def list_bots() -> List[Dict[str, Any]]:
    """Return slug + display name + description for every available bot."""
    bots = []
    for slug in AVAILABLE_BOTS:
        try:
            # Default environment is fine here; we only use this for the
            # listing, which just needs the slug + human-readable name.
            cfg = load_bot_config(slug, DEFAULT_ENVIRONMENT)
        except FileNotFoundError:
            continue
        bots.append({
            "slug": cfg.slug,
            "name": cfg.name,
            "description": cfg.description,
        })
    return bots


@app.get("/config/{bot}")
@app.get("/botnim/config/{bot}")
async def get_bot_config(
    bot: str,
    environment: Optional[str] = Query(None, description=f"Target environment. One of {VALID_ENVIRONMENTS}. Defaults to server default."),
) -> Dict[str, Any]:
    """Return the Responses-API BotConfig (model, instructions, tools) for ``bot``.

    The returned payload is suitable for direct use as kwargs to
    ``client.responses.create(...)`` (drop the ``slug`` / ``name`` /
    ``description`` metadata fields). It is freshly loaded from
    ``specs/<bot>/`` on every call, so CI-synced spec changes are picked up
    without a server restart.
    """
    env = environment or DEFAULT_ENVIRONMENT
    if env not in VALID_ENVIRONMENTS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid environment '{env}'. Valid: {VALID_ENVIRONMENTS}",
        )
    if bot not in AVAILABLE_BOTS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown bot '{bot}'. Valid: {AVAILABLE_BOTS}",
        )
    try:
        cfg = load_bot_config(bot, env)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return cfg.to_dict()


# Per-request deadline for /retrieve. Sized to land safely below the
# Service-Connect Envoy sidecar's per-request timeout (60s after the
# 2026-05-09 raise) AND the ALB idle timeout (120s) — so when Aurora is
# saturated the API returns a structured 504 the LLM can recover from
# (e.g. retry with search_mode=METADATA_BROWSE), instead of bubbling up
# a synthetic gateway-timeout from the proxy that loses all context.
RETRIEVE_TIMEOUT_SECONDS = float(os.getenv("BOTNIM_RETRIEVE_TIMEOUT_SECONDS", "12"))


def _inject_distribution(results, distribution: list[dict], fmt: str):
    """Wrap retrieve results with a government_distribution sidecar.

    Called only when ``distribution`` has >= 2 entries (the
    ``government_distribution_sidecar`` helper returns ``None`` otherwise,
    and we skip injection on its falsy return). The wrapping is format-aware
    so the LLM sees both the sidecar (which governments are represented in
    the corpus for this decision_number) and the actual retrieve results.

    Args:
        results: whatever ``run_query`` returned (string for text/text-short/
            yaml formats; list[dict] for dict).
        distribution: rows from ``government_distribution_sidecar`` — each
            row carries at minimum ``government_number``, plus best-effort
            ``government`` (cabinet name) and ``doc_count``.
        fmt: the ``format`` query param the caller passed (defaults to
            ``yaml`` upstream).
    """
    # TODO: 'dict' branch is currently dead on the wire — handler returns text/plain.
    # Wire up JSONResponse in the handler if dict format over HTTP is ever needed.
    if fmt == 'dict':
        return {"government_distribution": distribution, "results": results}
    if fmt == 'yaml':
        sidecar = _yaml.dump(
            {"government_distribution": distribution},
            allow_unicode=True, default_flow_style=False,
        )
        results_str = results if isinstance(results, str) else _yaml.dump(
            results, allow_unicode=True, default_flow_style=False,
        )
        return sidecar + "results:\n" + "".join(
            "  " + line + "\n" for line in results_str.splitlines()
        )
    # text / text-short
    lines = ["[government_distribution]"]
    for d in distribution:
        lines.append(
            f"  ממשלה {d['government_number']} — {d.get('government', '')} "
            f"({d.get('doc_count', '')} מסמכים)"
        )
    lines.append("")
    return "\n".join(lines) + "\n\n" + (results or "")


@app.get("/retrieve/{bot}/{context}")
@app.get("/botnim/retrieve/{bot}/{context}")
async def search_datasets_handler(
    bot: str,
    context: str,
    query: str,
    num_results: Optional[int] = None,
    search_mode: Optional[str] = None,
    format: Optional[str] = Query('yaml', description="Format of the results: 'text-short', 'text', 'dict', or 'yaml'"),
    metadata_filter: Optional[str] = Query(None, description='JSON object for JSONB containment filter, e.g. {"decision_number":"550"}'),
) -> str:
    store_id = f"{bot}__{context}"
    try:
        parsed_filter = json.loads(metadata_filter) if metadata_filter else None
    except json.JSONDecodeError as exc:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_metadata_filter", "detail": str(exc), "store_id": store_id},
        )
    # Resolve search mode config
    mode_config = SEARCH_MODES.get(search_mode, DEFAULT_SEARCH_MODE) if search_mode else DEFAULT_SEARCH_MODE
    # Use num_results from mode if not provided
    if num_results is None:
        num_results = mode_config.num_results
    try:
        # run_query is sync (sqlalchemy + openai client). Run in a worker
        # thread so we can enforce a deadline via asyncio.wait_for instead
        # of leaving the event loop blocked. asyncio.TimeoutError is an
        # alias for TimeoutError on Python 3.11+, so a single except branch
        # below covers both the deadline-exceeded case (our wait_for) and
        # any inner TimeoutError raised by run_query itself.
        results = await asyncio.wait_for(
            asyncio.to_thread(
                run_query,
                store_id=store_id,
                query_text=query,
                num_results=num_results,
                format=format,
                search_mode=mode_config,
                metadata_filter=parsed_filter,
            ),
            timeout=RETRIEVE_TIMEOUT_SECONDS,
        )
    except ConnectionError as e:
        logger.error(f"Upstream connection error in search: {e}")
        return JSONResponse(
            status_code=502,
            content={"error": "upstream_connection_error", "detail": str(e), "store_id": store_id},
        )
    except TimeoutError as e:
        # Catches both:
        #  - asyncio.TimeoutError raised by our wait_for (str(e) is empty
        #    in CPython, so we synthesize a useful message)
        #  - inner TimeoutError raised by run_query / lower layers (carries
        #    its own detail string we forward as-is)
        # On Python 3.11+ asyncio.TimeoutError is an alias for TimeoutError,
        # so a single except branch is the simplest and correct option.
        detail = str(e) or (
            f"deadline exceeded after {RETRIEVE_TIMEOUT_SECONDS:.1f}s; "
            "retry, or try search_mode=METADATA_BROWSE for a SQL-only path"
        )
        logger.warning(
            "RETRIEVE_TIMEOUT store_id=%s detail=%s query=%r",
            store_id, detail, query[:80],
        )
        return JSONResponse(
            status_code=504,
            content={"error": "search_timeout", "detail": detail, "store_id": store_id},
        )
    except Exception as e:
        logger.error(f"Search failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "search_error", "detail": str(e), "store_id": store_id},
        )

    # Government-decisions multi-term sidecar: a ~3ms GROUP BY on indexed
    # jsonb. Only fires when the caller filtered by decision_number AND the
    # request targeted a government_decisions context. The sidecar helper
    # is sync (sqlalchemy under the hood), so dispatch it via to_thread to
    # avoid blocking the event loop. We deliberately do NOT put this under
    # the same wait_for deadline as run_query — the probe is cheap and a
    # rare slow query here shouldn't 504 the whole retrieve.
    decision_number = (parsed_filter or {}).get("decision_number")
    if decision_number and context.startswith("government_decisions"):
        try:
            distribution = await asyncio.to_thread(
                government_distribution_sidecar,
                store_id,
                decision_number,
            )
        except Exception:
            # Sidecar must never fail the parent retrieve. The helper itself
            # already swallows exceptions and returns None, so this is a
            # belt-and-braces guard.
            logger.warning("government_distribution_sidecar raised; ignoring", exc_info=True)
            distribution = None
        if distribution:
            results = _inject_distribution(results, distribution, format or 'yaml')

    if format == 'yaml':
        return Response(content=results, media_type="application/x-yaml")
    return Response(content=results, media_type="text/plain")


# ---------------------------------------------------------------------------
# Word-doc generation tool
#
# POST /tools/generate_word_doc — takes a structured {title, sections} body,
# renders a Hebrew RTL .docx via python-docx, uploads to the env-scoped S3
# bucket (set via WORD_DOCS_BUCKET), and returns a 7-day presigned URL the
# unified bot embeds in its chat reply. The bucket is provisioned by a
# separate terragrunt sub-stack with a 7-day lifecycle; if WORD_DOCS_BUCKET
# is unset the endpoint replies 503 (feature disabled), so partial deploys
# (image rolled out before the bucket exists) degrade cleanly.
# ---------------------------------------------------------------------------


@app.post("/tools/generate_word_doc", response_model=WordDocResponse)
@app.post("/botnim/tools/generate_word_doc", response_model=WordDocResponse)
def generate_word_doc(req: WordDocRequest) -> WordDocResponse:
    bucket = os.getenv("WORD_DOCS_BUCKET", "")
    if not bucket:
        raise HTTPException(
            status_code=503,
            detail="WORD_DOCS_BUCKET not configured; word-doc generation disabled",
        )
    try:
        body = render_word_doc(req)
    except Exception as e:
        logger.exception("word_doc render failed")
        raise HTTPException(
            status_code=500,
            detail=f"render failed: {type(e).__name__}",
        )

    filename = sanitize_filename(req.title)
    try:
        return upload_word_doc(bucket=bucket, body=body, filename=filename)
    except Exception as e:
        logger.exception("word_doc S3 upload failed")
        raise HTTPException(
            status_code=502,
            detail=f"upload failed: {type(e).__name__}",
        )


# ---------------------------------------------------------------------------
# Knesset plenum schedule — JIT live OData proxy
#
# The cached `plenary_schedule` context (refreshed by fap on each deploy) is
# the right tool for semantic queries like "which session covered חוק מימון
# מפלגות". For TIME-sensitive questions like "מה הישיבה הבאה במליאה" or
# "מה היה בישיבה לפני" the cached snapshot is too stale by design — fap
# only runs at deploy time. This endpoint hits Knesset OData live (no
# caching), filtered to a date range the LLM specifies. The agent should
# use this for "next/last/upcoming/past" type questions and the cached
# context for content-driven queries.
# ---------------------------------------------------------------------------


@app.get("/knesset/sessions")
@app.get("/botnim/knesset/sessions")
async def knesset_sessions_live(
    from_date: str = Query(..., alias="from",
        description="Start of date window (inclusive), ISO 8601 — e.g. 2026-04-01 or 2026-04-01T00:00:00."),
    to_date: str = Query(..., alias="to",
        description="End of date window (exclusive), ISO 8601."),
    include_items: bool = Query(True,
        description="If true, include each session's agenda items inline. If false, only sessions."),
    timeout: int = Query(60, ge=10, le=110,
        description="Upstream OData call timeout in seconds. Capped below the ALB idle (120) so we always have headroom to return a 504 cleanly."),
):
    """Live Knesset plenum sessions in [from, to). No caching.

    Pass-through to ``knesset.gov.il/Odata/ParliamentInfo.svc/`` with a
    date range filter on ``StartDate``. Returns sessions ordered by
    StartDate ascending plus their agenda items (when include_items=true)
    inline as `items: [...]`. Hebrew dates are added as
    ``StartDateHe``/``FinishDateHe`` for easier LLM rendering.
    """
    from datetime import datetime
    from botnim.document_parser.knesset_odata.process_odata import (
        fetch_plenum_sessions,
        fetch_session_items,
        fetch_session_stenograms,
        _hebrew_date,
        _DEFAULT_BASE,
    )

    def _parse(s: str, label: str) -> datetime:
        # Accept "YYYY-MM-DD" or full ISO; normalize to naive datetime since
        # the OData service stores StartDate as naive Israel-local timestamps.
        s = s.strip()
        if len(s) == 10:  # date only
            s = s + "T00:00:00"
        try:
            return datetime.fromisoformat(s.replace("Z", ""))
        except ValueError as e:
            raise HTTPException(status_code=400,
                detail=f"invalid {label} '{s}': expected YYYY-MM-DD or full ISO 8601 ({e})")

    start_dt = _parse(from_date, "from")
    end_dt = _parse(to_date, "to")
    if not (start_dt < end_dt):
        raise HTTPException(status_code=400, detail="'from' must be strictly before 'to'")
    if (end_dt - start_dt).days > 400:
        raise HTTPException(status_code=400, detail="window too wide; max 400 days")

    try:
        sessions = fetch_plenum_sessions(_DEFAULT_BASE, start_dt, end_dt, timeout=timeout)
    except requests.exceptions.Timeout as e:
        return JSONResponse(status_code=504,
            content={"error": "upstream_timeout", "detail": str(e)})
    except requests.exceptions.ConnectionError as e:
        return JSONResponse(status_code=502,
            content={"error": "upstream_connection_error", "detail": str(e)})

    if include_items and sessions:
        ids = [s["PlenumSessionID"] for s in sessions if s.get("PlenumSessionID") is not None]
        try:
            items = fetch_session_items(_DEFAULT_BASE, ids, timeout=timeout)
        except requests.exceptions.Timeout as e:
            return JSONResponse(status_code=504,
                content={"error": "upstream_timeout", "detail": str(e)})
        items_by_session: Dict[int, List[Dict[str, Any]]] = {}
        for it in items:
            items_by_session.setdefault(it["PlenumSessionID"], []).append(it)
        for s in sessions:
            sid = s.get("PlenumSessionID")
            s["items"] = items_by_session.get(sid, [])

    # Source-URL enrichment: KNS_DocumentPlenumSession (GroupTypeID=43,
    # סטנוגרמה) gives us the canonical Knesset transcript URL per session.
    # Sessions without a published stenogram (typically upcoming sittings)
    # simply lack `source_url`. We swallow stenogram fetch failures so a
    # transient outage on this extra OData call doesn't break the main
    # response.
    if sessions:
        ids = [s["PlenumSessionID"] for s in sessions if s.get("PlenumSessionID") is not None]
        try:
            stenograms = fetch_session_stenograms(_DEFAULT_BASE, ids, timeout=timeout)
            stenogram_url_by_session: Dict[int, str] = {}
            for doc in sorted(stenograms, key=lambda d: d.get("LastUpdatedDate") or ""):
                sid = doc.get("PlenumSessionID")
                fp = (doc.get("FilePath") or "").strip()
                if sid and fp:
                    stenogram_url_by_session[sid] = fp
            for s in sessions:
                url = stenogram_url_by_session.get(s.get("PlenumSessionID"))
                if url:
                    s["source_url"] = url
        except requests.exceptions.RequestException:
            # Best-effort enrichment; don't fail the main response if the
            # extra OData call has a transient issue.
            pass

    for s in sessions:
        s["StartDateHe"] = _hebrew_date(s.get("StartDate", ""))
        s["FinishDateHe"] = _hebrew_date(s.get("FinishDate", ""))

    return {
        "count": len(sessions),
        "from": from_date,
        "to": to_date,
        "sessions": sessions,
    }
    if format == 'yaml':
        return Response(content=results, media_type="application/x-yaml")
    return Response(content=results, media_type="text/plain")


# ---------------------------------------------------------------------------
# Admin refresh endpoint
#
# Called by the VPC-local `botnim-refresh-invoker` Lambda on an EventBridge
# schedule (daily). Runs the whole fetch-and-process + sync pipeline in a
# background thread so the HTTP response returns quickly. Failures are
# surfaced via ERROR-level "REFRESH_FAILED: ..." log lines that a CloudWatch
# Logs metric filter watches — see infra/envs/<env>/refresh.tf.
#
# With desired_count > 1 (post-2026-05-09) the Lambda call may land on any
# task and a Lambda retry can hit a different one mid-run. We guard the
# background body with a postgres advisory lock so only one task at a time
# actually runs the pipeline; the others log REFRESH_SKIPPED and exit.
# ---------------------------------------------------------------------------


# Stable bigint key for the daily-refresh advisory lock. CRC32 of a stable
# label gives us a deterministic 32-bit value (always fits in postgres bigint)
# that is very unlikely to collide with anything else on the cluster. No other
# code paths use pg advisory locks today; if that changes, document the
# keyspace inline so future labels can avoid collisions.
import zlib as _zlib  # local alias — already not imported elsewhere in this module
_REFRESH_LOCK_KEY = _zlib.crc32(b"botnim:refresh:daily")
_SANITY_LOCK_KEY = _zlib.crc32(b"botnim:sanity:scheduled")


def _try_run_with_advisory_lock(key: int, label: str, fn) -> None:
    """Run ``fn()`` while holding a session-level pg advisory lock.

    If the lock is already held, log ``{label}_SKIPPED`` and return without
    invoking ``fn``. Used to coordinate the daily refresh background job
    across multiple botnim-api Fargate tasks.

    Lock is released explicitly via ``pg_advisory_unlock`` and again
    implicitly when the connection closes.
    """
    from sqlalchemy import text as _text
    from botnim.db.session import get_engine
    engine = get_engine()
    conn = engine.connect()
    try:
        acquired = conn.execute(_text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar()
        if not acquired:
            logger.info("%s_SKIPPED: another task holds advisory lock %#x", label, key)
            return
        try:
            fn()
        finally:
            try:
                conn.execute(_text("SELECT pg_advisory_unlock(:k)"), {"k": key})
                conn.commit()
            except Exception:
                logger.warning("%s: pg_advisory_unlock failed; lock will release on session close", label, exc_info=True)
    finally:
        conn.close()


def _run_refresh_job() -> None:
    env = os.environ.get("ENVIRONMENT", DEFAULT_ENVIRONMENT)
    logger.info(f"REFRESH_START: env={env}")
    # fetch_and_process(environment, bot, context, kind). 'all' kind so any
    # newly-added fetcher type (bk_csv for government_decisions, future
    # additions) gets picked up without a code change here. Static fetchers
    # (lexicon, wikitext) re-run cheaply when nothing changed upstream.
    fetch_and_process(env, "all", "all", "all")
    # backend defaults to 'aurora' (sync.py:80) post-2026-04 migration.
    sync_agents(env, "all")
    logger.info("REFRESH_OK")


def _run_refresh_job_background() -> None:
    def _body() -> None:
        try:
            _run_refresh_job()
        except Exception as e:
            logger.error(f"REFRESH_FAILED: {type(e).__name__}: {e}", exc_info=True)

    try:
        _try_run_with_advisory_lock(_REFRESH_LOCK_KEY, "REFRESH", _body)
    except Exception as e:
        # Lock-acquisition failures must not silently drop the run — if the
        # DB is unreachable, REFRESH_FAILED is the right outcome (rather than
        # REFRESH_SKIPPED, which implies "another task is on it").
        logger.error(f"REFRESH_FAILED: lock acquisition failed: {type(e).__name__}: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Admin sanity endpoint
#
# Called by the `botnim-sanity-invoker-<env>` Lambda on EventBridge schedule
# (twice daily). Spawns a daemon thread that runs the full capture → judge →
# render → finalize pipeline. Status surfaces in CloudWatch logs as
# SANITY_START / SANITY_OK / SANITY_FAILED / SANITY_REGRESSION (filtered
# into the same SNS topic as refresh failures).
# ---------------------------------------------------------------------------


def _run_sanity_job_background() -> None:
    def _body() -> None:
        env = os.environ.get("ENVIRONMENT", DEFAULT_ENVIRONMENT)
        logger.info(f"SANITY_START: env={env}")
        try:
            from botnim.sanity.runner import run_sanity
            run_id = run_sanity(env=env, db_url=os.environ["DATABASE_URL"])
            logger.info(f"SANITY_OK: run_id={run_id}")
        except Exception as e:
            logger.error(f"SANITY_FAILED: {type(e).__name__}: {e}", exc_info=True)

    # Same desired_count > 1 rationale as REFRESH — only one task at a time
    # should run a sanity capture, otherwise two parallel runs would write
    # overlapping rows into the sanity_runs table and double-burn the
    # judge-side LLM budget.
    try:
        _try_run_with_advisory_lock(_SANITY_LOCK_KEY, "SANITY", _body)
    except Exception as e:
        logger.error(f"SANITY_FAILED: lock acquisition failed: {type(e).__name__}: {e}", exc_info=True)


@app.post("/admin/sanity", status_code=202)
@app.post("/botnim/admin/sanity", status_code=202)
async def trigger_sanity(
    _auth: None = Depends(require_sanity_api_key),
) -> Dict[str, str]:
    """Kick off a sanity DoD run in the background.

    Returns 202 Accepted immediately. Check CloudWatch logs for
    SANITY_START / SANITY_OK / SANITY_FAILED / SANITY_REGRESSION.
    """
    thread = threading.Thread(target=_run_sanity_job_background, daemon=True)
    thread.start()
    return {"status": "accepted"}


@app.post("/admin/refresh", status_code=202)
@app.post("/botnim/admin/refresh", status_code=202)
async def refresh(
    _auth: None = Depends(require_refresh_api_key),
) -> Dict[str, str]:
    """Kick off a full knesset-PDF refresh in the background.

    Returns 202 Accepted immediately. The actual refresh runs in a thread;
    check CloudWatch logs for REFRESH_START / REFRESH_OK / REFRESH_FAILED.
    """
    thread = threading.Thread(target=_run_refresh_job_background, daemon=True)
    thread.start()
    return {"status": "accepted"}


router = APIRouter(
    prefix="/admin",
)

class UserUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    role: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None

@router.get("/users")
async def get_users(
    firebase_user: FireBaseUser,
) -> List[Dict[str, Any]]:
    """Read all records from the 'users' collection in the datastore
    Args:
        firebase_user (FireBaseUser): The authenticated Firebase user
    Returns:
        List[Dict[str, Any]]: A list of user information
    """
    try:
        # Initialize Firestore client
        db = firestore.client()
        
        # Get all documents from the users collection
        users_ref = db.collection('users')
        users_docs = users_ref.stream()
        
        # Convert documents to list of dictionaries
        users = []
        for doc in users_docs:
            user_data = doc.to_dict()
            user_data['id'] = doc.id  # Include document ID
            user_data.pop('password', None)  # Remove password from user data
            users.append(user_data)
        
        return users
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching users: {str(e)}")

@router.post("/user/{user_id}/update")
async def update_user(
    user_id: str,
    update_request: UserUpdateRequest,
    firebase_user: FireBaseUser,
) -> Dict[str, Any]:
    """Update a user record in the 'users' collection in the datastore, or create if it doesn't exist
    Args:
        user_id (str): The ID of the user document to update or create
        update_request (UserUpdateRequest): The update data containing display name, role and password
        firebase_user (FireBaseUser): The authenticated Firebase user

        - locates the user document by user_id in the 'users' collection
        - if the user exists: updates the user document with the new display name, role and password (only non-None values)
        - if the user doesn't exist: creates a new user record with the provided data and default values
        - timestamps are automatically added for new records

    Returns:
        Dict[str, Any]: The updated or created user data
    """
    try:
        # Initialize Firestore client
        db = firestore.client()
        
        # Reference to the specific user document
        user_ref = db.collection('users').document(user_id)
        
        # Check if user exists first
        user_doc = user_ref.get()
        
        if user_doc.exists:
            # User exists - update only the provided fields
            updates = {}
            if update_request.display_name is not None:
                updates['display_name'] = update_request.display_name
            if update_request.role is not None:
                updates['role'] = update_request.role
            if update_request.email is not None:
                updates['email'] = update_request.email
            if update_request.password is not None:
                updates['password'] = update_request.password
            updates['updated_at'] = firestore.SERVER_TIMESTAMP

            # Only update if there are fields to update
            if updates:
                user_ref.update(updates)
        else:
            # User doesn't exist - create a new user record
            new_user_data = {
                'id': user_id,
                'created_at': firestore.SERVER_TIMESTAMP,
                'updated_at': firestore.SERVER_TIMESTAMP
            }
            if update_request.display_name is not None:
                new_user_data['display_name'] = update_request.display_name
            if update_request.role is not None:
                new_user_data['role'] = update_request.role
            if update_request.email is not None:
                new_user_data['email'] = update_request.email
            if update_request.password is not None:
                new_user_data['password'] = update_request.password

            user_ref.set(new_user_data)
        
        # Fetch the final document (whether updated or created)
        final_doc = user_ref.get()
        final_data = final_doc.to_dict()
        final_data['id'] = final_doc.id  # Include document ID
        return final_data
        
    except HTTPException:
        # Re-raise HTTP exceptions as they are
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating user: {str(e)}")

# Include the admin router in the main app
app.include_router(router)


# Run the server with:
# uvicorn server:app --reload



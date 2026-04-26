import dataclasses
import logging
import os
import threading
from fastapi import APIRouter, FastAPI, HTTPException, Response, Query, Body, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Dict, Any, Optional
from firebase_admin import firestore
from pydantic import BaseModel

from resolve_firebase_user import FireBaseUser
from refresh_auth import require_refresh_api_key
from botnim.query import run_query
from botnim.vector_store.search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE
from botnim.bot_config import load_bot_config
from botnim.config import AVAILABLE_BOTS, VALID_ENVIRONMENTS, DEFAULT_ENVIRONMENT
from botnim.fetch_and_process import fetch_and_process
from botnim.sync import sync_agents

logger = logging.getLogger(__name__)

app = FastAPI(openapi_url=None, redirect_slashes=False)

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


@app.get("/retrieve/{bot}/{context}")
@app.get("/botnim/retrieve/{bot}/{context}")
async def search_datasets_handler(
    bot: str,
    context: str,
    query: str,
    num_results: Optional[int] = None,
    search_mode: Optional[str] = None,
    format: Optional[str] = Query('yaml', description="Format of the results: 'text-short', 'text', 'dict', or 'yaml'")
) -> str:
    store_id = f"{bot}__{context}"
    # Resolve search mode config
    mode_config = SEARCH_MODES.get(search_mode, DEFAULT_SEARCH_MODE) if search_mode else DEFAULT_SEARCH_MODE
    # Use num_results from mode if not provided
    if num_results is None:
        num_results = mode_config.num_results
    try:
        results = run_query(
            store_id=store_id,
            query_text=query,
            num_results=num_results,
            format=format,
            search_mode=mode_config
        )
    except ConnectionError as e:
        logger.error(f"Upstream connection error in search: {e}")
        return JSONResponse(
            status_code=502,
            content={"error": "upstream_connection_error", "detail": str(e), "store_id": store_id},
        )
    except TimeoutError as e:
        logger.error(f"Timeout in search: {e}")
        return JSONResponse(
            status_code=504,
            content={"error": "search_timeout", "detail": str(e), "store_id": store_id},
        )
    except Exception as e:
        logger.error(f"Search failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "search_error", "detail": str(e), "store_id": store_id},
        )
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
# ---------------------------------------------------------------------------


def _run_refresh_job() -> None:
    env = os.environ.get("ENVIRONMENT", DEFAULT_ENVIRONMENT)
    logger.info(f"REFRESH_START: env={env}")
    # fetch_and_process(environment, bot, context, kind). 'all' kind so any
    # newly-added fetcher type (bk_csv for government_decisions, future
    # additions) gets picked up without a code change here. Static fetchers
    # (lexicon, wikitext) re-run cheaply when nothing changed upstream.
    fetch_and_process(env, "all", "all", "all")
    # sync_agents(environment, bots, backend='es')
    sync_agents(env, "all", backend="es")
    logger.info("REFRESH_OK")


def _run_refresh_job_background() -> None:
    try:
        _run_refresh_job()
    except Exception as e:
        logger.error(f"REFRESH_FAILED: {type(e).__name__}: {e}", exc_info=True)


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



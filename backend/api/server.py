import dataclasses
from fastapi import FastAPI, HTTPException, Response, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Dict, Any, Optional

from botnim.query import run_query
from botnim.vector_store.search_modes import SEARCH_MODES, DEFAULT_SEARCH_MODE

app = FastAPI(openapi_url=None, redirect_slashes=False)

# Enable CORS:
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this to the specific origins you want to allow
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/retrieve/{bot}/{context}")
async def search_datasets_handler(
    bot: str,
    context: str,
    query: str,
    num_results: Optional[int] = None,
    search_mode: Optional[str] = None,
    format: Optional[str] = Query('text-short', description="Format of the results: 'text-short', 'text', 'dict', or 'yaml'")
) -> str:
    store_id = f"{bot}__{context}"
    # Resolve search mode config
    mode_config = SEARCH_MODES.get(search_mode, DEFAULT_SEARCH_MODE) if search_mode else DEFAULT_SEARCH_MODE
    # Use num_results from mode if not provided
    if num_results is None:
        num_results = mode_config.num_results
    results = run_query(
        store_id=store_id,
        query_text=query,
        num_results=num_results,
        format=format,
        search_mode=mode_config
    )
    if format == 'yaml':
        return Response(content=results, media_type="application/x-yaml")
    return Response(content=results, media_type="text/plain")

@app.get("/search-modes")
async def list_search_modes():
    """Return all available search modes, their descriptions, and default num_results."""
    return [
        {
            "name": name,
            "description": config.description,
            "default_num_results": config.num_results
        }
        for name, config in SEARCH_MODES.items()
    ]

# Run the server with:
# uvicorn server:app --reload

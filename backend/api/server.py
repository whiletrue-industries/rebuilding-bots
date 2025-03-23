import dataclasses
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Dict, Any, Optional

from botnim.query import run_query

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
async def search_datasets_handler(bot: str, context: str, query: str, num_results: int=10) -> str:
    store_id = f"{bot}__{context}"
    results = run_query(store_id=store_id, query_text=query, num_results=num_results, format="text")
    return results

# Run the server with:
# uvicorn server:app --reload

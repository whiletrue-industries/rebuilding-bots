from __future__ import annotations

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field, conint


class WordDocSection(BaseModel):
    heading: str = Field(..., min_length=1, max_length=300)
    level: conint(ge=1, le=3) = 1
    body_md: str = Field(default="", max_length=200_000)


class WordDocRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=1000)
    sections: List[WordDocSection] = Field(..., min_length=1)


class WordDocResponse(BaseModel):
    url: str
    filename: str
    expires_at: datetime

from __future__ import annotations

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field, conint, field_validator


class WordDocSection(BaseModel):
    heading: str = Field(..., min_length=1, max_length=300)
    level: conint(ge=1, le=3) = 1
    body_md: str = Field(..., min_length=1, max_length=200_000)

    @field_validator("body_md")
    @classmethod
    def _body_md_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError(
                "body_md must contain non-whitespace content — every section "
                "needs at least one paragraph or list item. Empty sections "
                "produce useless heading-only artifacts."
            )
        return v


class WordDocRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=1000)
    sections: List[WordDocSection] = Field(..., min_length=1)


class WordDocResponse(BaseModel):
    url: str
    filename: str
    expires_at: datetime

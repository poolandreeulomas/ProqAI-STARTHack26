from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from supplier_engine import SupplierEngine

app = FastAPI()
engine = SupplierEngine()


class MatchRequest(BaseModel):
    request: dict[str, Any] = Field(...)


@app.post("/match")
def match(payload: MatchRequest) -> dict[str, Any]:
    request_dict = payload.request
    request_id = request_dict.get("request_id", "unknown")

    print(f"[match] start request_id={request_id}")
    result = engine.process(request_dict)
    print(f"[match] end request_id={request_id}")

    return result

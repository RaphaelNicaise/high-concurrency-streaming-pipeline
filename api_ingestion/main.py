from fastapi import FastAPI
from typing import Any, Dict

app = FastAPI(title="TapDrink Ingest API (Dummy)")


@app.get("/")
def read_root():
    return {"status": "ok", "message": "Ingest API is running"}


@app.post("/events")
def ingest_event(payload: Dict[str, Any]):
    # Endpoint dummy que recibe los eventos del chaos-simulator y responde 200 OK
    return {"status": "accepted", "event_id": payload.get("event_id")}

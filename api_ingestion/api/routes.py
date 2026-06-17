import asyncio
import json
from fastapi import APIRouter, Request, status
from sse_starlette.sse import EventSourceResponse

from core import redis
from models.events import TelemetryEvent, SimulatorCommand

router = APIRouter()

@router.get("/")
def read_root():
    return {"status": "ok", "message": "Ingest API is running with Redis Streams"}

@router.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(event: TelemetryEvent):
    # Convert to JSON string for Redis Streams
    payload = {"data": event.model_dump_json()}

    # 1. Push to Redis Stream
    await redis.redis_client.xadd("telemetry:events", payload)

    # 2. Increment global counter for metrics
    await redis.redis_client.incr("telemetry:events:count")

    return {"status": "accepted", "event_id": event.event_id}

@router.post("/simulator/control")
async def control_simulator(cmd: SimulatorCommand):
    state = cmd.command.upper()
    if state not in ["START", "STOP"]:
        return {"error": "Invalid command. Use START or STOP."}

    new_state = "RUNNING" if state == "START" else "PAUSED"
    await redis.redis_client.set("simulator:state", new_state)
    return {"status": "success", "new_state": new_state}

@router.get("/simulator/control")
async def get_simulator_state():
    state_bytes = await redis.redis_client.get("simulator:state")
    current_state = state_bytes.decode("utf-8") if state_bytes else "RUNNING"
    return {"state": current_state}

@router.get("/metrics/live")
async def metrics_live(request: Request):
    """
    Server-Sent Events endpoint to broadcast events/sec and simulator state.
    """
    async def event_generator():
        last_count = 0
        try:
            raw_count = await redis.redis_client.get("telemetry:events:count")
            last_count = int(raw_count) if raw_count else 0
        except Exception:
            pass

        while True:
            if await request.is_disconnected():
                break

            try:
                raw_count = await redis.redis_client.get("telemetry:events:count")
                current_count = int(raw_count) if raw_count else 0
                state_bytes = await redis.redis_client.get("simulator:state")
                current_state = state_bytes.decode("utf-8") if state_bytes else "UNKNOWN"

                events_per_sec = current_count - last_count
                last_count = current_count

                data = {
                    "events_per_sec": events_per_sec,
                    "total_events": current_count,
                    "simulator_state": current_state,
                }
                yield {"data": json.dumps(data)}
            except Exception as e:
                yield {"data": json.dumps({"error": str(e)})}

            await asyncio.sleep(1.0)

    return EventSourceResponse(event_generator())

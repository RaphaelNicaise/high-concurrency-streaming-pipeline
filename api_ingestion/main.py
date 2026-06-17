import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from pydantic import BaseModel
from redis.asyncio import ConnectionPool, Redis
from sse_starlette.sse import EventSourceResponse

# Environment variables
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Global redis connection
redis_client: Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    pool = ConnectionPool.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}", max_connections=100
    )
    redis_client = Redis(connection_pool=pool)
    # Ensure simulator starts in RUNNING state
    await redis_client.set("simulator:state", "RUNNING")
    yield
    await redis_client.aclose()


app = FastAPI(title="TapDrink Ingest API", lifespan=lifespan)


class TelemetryEvent(BaseModel):
    event_id: str
    user_id: str
    session_id: str
    event_type: str
    # Other fields are flexible, we allow anything using model_config
    model_config = {"extra": "allow"}


class SimulatorCommand(BaseModel):
    command: str  # START, STOP


@app.get("/")
def read_root():
    return {"status": "ok", "message": "Ingest API is running with Redis Streams"}


@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(event: TelemetryEvent):
    # We convert to a dict where all values are strings (Redis stream requirement)
    # We dump the whole original event as JSON inside the stream to avoid flattening issues
    payload = {"data": event.model_dump_json()}

    # 1. Push to Redis Stream
    await redis_client.xadd("telemetry:events", payload)

    # 2. Increment global counter for metrics
    await redis_client.incr("telemetry:events:count")

    return {"status": "accepted", "event_id": event.event_id}


@app.post("/simulator/control")
async def control_simulator(cmd: SimulatorCommand):
    state = cmd.command.upper()
    if state not in ["START", "STOP"]:
        return {"error": "Invalid command. Use START or STOP."}

    new_state = "RUNNING" if state == "START" else "PAUSED"
    await redis_client.set("simulator:state", new_state)
    return {"status": "success", "new_state": new_state}


@app.get("/simulator/control")
async def get_simulator_state():
    state_bytes = await redis_client.get("simulator:state")
    current_state = state_bytes.decode("utf-8") if state_bytes else "RUNNING"
    return {"state": current_state}


@app.get("/metrics/live")
async def metrics_live(request: Request):
    """
    Server-Sent Events endpoint to broadcast events/sec and simulator state.
    """

    async def event_generator():
        last_count = 0
        try:
            # Get initial count safely
            raw_count = await redis_client.get("telemetry:events:count")
            last_count = int(raw_count) if raw_count else 0
        except Exception:
            pass

        while True:
            if await request.is_disconnected():
                break

            try:
                # Read current state and counts
                raw_count = await redis_client.get("telemetry:events:count")
                current_count = int(raw_count) if raw_count else 0
                state_bytes = await redis_client.get("simulator:state")
                current_state = (
                    state_bytes.decode("utf-8") if state_bytes else "UNKNOWN"
                )

                # Calculate delta for throughput
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

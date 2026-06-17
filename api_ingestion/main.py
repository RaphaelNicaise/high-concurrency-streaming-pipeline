from contextlib import asynccontextmanager
from fastapi import FastAPI
from redis.asyncio import Redis

from core import redis
from api.routes import router

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = redis.get_redis_pool()
    redis.redis_client = Redis(connection_pool=pool)
    # Ensure simulator starts in RUNNING state
    await redis.redis_client.set("simulator:state", "RUNNING")
    yield
    await redis.redis_client.aclose()

app = FastAPI(title="TapDrink Ingest API", lifespan=lifespan)

app.include_router(router)

import os
from redis.asyncio import ConnectionPool, Redis

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Global redis client instance
redis_client: Redis | None = None

def get_redis_pool():
    return ConnectionPool.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}", max_connections=100
    )

import redis.asyncio as redis
from typing import AsyncGenerator

REDIS_URL = "redis://localhost:6379/0"

# Single shared client instance, reused across the app
redis_client: redis.Redis = redis.from_url(
    REDIS_URL,
    decode_responses=True,  # get back str instead of bytes
    socket_timeout=10,
)


async def get_redis() -> AsyncGenerator[redis.Redis, None]:
    """FastAPI dependency to inject the Redis client into routes."""
    yield redis_client


async def check_redis_connection() -> bool:
    """Quick sanity check that Redis is reachable."""
    pong = await redis_client.ping()
    return pong


async def close_redis() -> None:
    """Call this on app shutdown to close the connection pool cleanly."""
    await redis_client.aclose()
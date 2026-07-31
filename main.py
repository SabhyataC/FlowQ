from contextlib import asynccontextmanager
from fastapi import FastAPI

from core.queue import ensure_consumer_group
from db.redis_client import close_redis, redis_client

from api.routes import router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    await ensure_consumer_group( redis_client, 'stream:default', 'worker')
    yield 

    # --- shutdown ---
    await close_redis()

app = FastAPI(lifespan=lifespan)
app.include_router(router)
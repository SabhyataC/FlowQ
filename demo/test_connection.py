import asyncio
from sqlalchemy import text
from db.connection import engine

async def test_connection():
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT version();"))
        version = result.scalar()
        print("✅ Connected to Postgres successfully!")
        print(f"Postgres version: {version}")

asyncio.run(test_connection())

# import os
# from dotenv import load_dotenv

# load_dotenv()
# print("DATABASE_URL:", repr(os.getenv("DATABASE_URL")))
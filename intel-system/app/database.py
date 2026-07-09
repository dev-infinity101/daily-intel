import asyncio
import ssl
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

def _build_connect_args() -> dict:
    # Use SSL for any non-localhost database (Neon, RDS, etc.)
    is_local = "localhost" in settings.database_url or "127.0.0.1" in settings.database_url
    if is_local:
        return {
            "ssl": False,
            "timeout": 10,
            "command_timeout": 10,
            "server_settings": {"jit": "off"},
        }
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    return {
        "ssl": ssl_context,
        "timeout": 10,
        "command_timeout": 10,
        "server_settings": {"jit": "off"},
    }


engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_pre_ping=True,
    connect_args=_build_connect_args(),
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session

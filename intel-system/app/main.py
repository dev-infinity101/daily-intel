from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from fastapi import FastAPI

from app.routers import admin, health, ingest, news_admin
from app.scheduler.runner import start_scheduler
from app.utils.logging import configure_logging
from app.utils.tracing import RequestIDMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    configure_logging()
    start_scheduler()
    yield


app = FastAPI(title="Daily Intel System", version="0.1.0", lifespan=lifespan)
app.add_middleware(RequestIDMiddleware)
app.include_router(health.router)
app.include_router(ingest.router)
app.include_router(admin.router)
app.include_router(news_admin.router)

"""FastAPI entrypoint for the WHT exemption lookup."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.concurrency import run_in_threadpool

from api.core.logging import configure_logging
from api.dependencies import get_model_service
from api.routes import checklist, grant, health, models, predict

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await run_in_threadpool(get_model_service().warm)
        log.info("Serving model loaded")
    except Exception as exc:
        log.warning("Serving model not loaded, /predict will return 503: %s", exc)
    yield
    if get_model_service.cache_info().currsize:
        get_model_service().close()


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="WHT exemption", lifespan=lifespan)
    app.include_router(health.router)
    app.include_router(grant.router)
    app.include_router(predict.router)
    app.include_router(models.router)
    app.include_router(checklist.router)
    return app


app = create_app()

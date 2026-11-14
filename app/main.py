"""FastAPI application factory.

Each process runs the full HTTP app plus one verification worker thread.
Two processes share the same SQLite files on a mounted volume; the run-claim
protocol and the data-version ledger keep their conclusions consistent.
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .database import init_databases
from .errors import LeakCheckError
from .models import Health
from .routers import release, status, uploads
from .services.worker import VerificationWorker

logging.basicConfig(
    level=os.environ.get("LEAKCHECK_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("leakcheck")

DEMO_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "demo")


def create_app() -> FastAPI:
    # The worker is owned by the lifespan so its thread starts/stops cleanly
    # and only after the databases have been initialised.
    worker = VerificationWorker()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_databases()
        if os.environ.get("LEAKCHECK_DISABLE_WORKER") != "1":
            worker.start()
        app.state.worker = worker
        try:
            yield
        finally:
            worker.stop()

    app = FastAPI(
        title="病理样本跨组泄漏核查 API",
        description=(
            "上传 LIMS 衍生关系与分组，基于无向连通性自动核查同源切片"
            "是否跨训练/验证/测试组；每次提交即失效旧放行，核查通过后方可下载"
            "冻结的分组 CSV。"
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.exception_handler(LeakCheckError)
    async def domain_error_handler(request: Request, exc: LeakCheckError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": type(exc).__name__, "detail": str(exc)},
        )

    app.include_router(uploads.router)
    app.include_router(status.router)
    app.include_router(release.router)

    @app.get("/health", response_model=Health, tags=["system"])
    def health() -> Health:
        return Health(status="ok", worker_id=worker.worker_id)

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index() -> HTMLResponse:
        with open(os.path.join(DEMO_DIR, "index.html"), encoding="utf-8") as fh:
            return HTMLResponse(fh.read())

    if os.path.isdir(os.path.join(DEMO_DIR, "static")):
        app.mount(
            "/static",
            StaticFiles(directory=os.path.join(DEMO_DIR, "static")),
            name="static",
        )

    app.state.worker = worker
    return app


app = create_app()

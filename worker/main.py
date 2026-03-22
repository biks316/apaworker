from __future__ import annotations

import logging

import uvicorn
from fastapi import FastAPI

from worker.config import get_settings
from worker.orchestrator import WorkerOrchestrator


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


settings = get_settings()
configure_logging(settings.log_level)

app = FastAPI(title="APA Worker Internal API", version="0.1.0")
orchestrator = WorkerOrchestrator(settings)


@app.on_event("startup")
def on_startup() -> None:
    orchestrator.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    orchestrator.stop()


@app.get("/internal/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/internal/ready")
def ready() -> dict[str, object]:
    return {
        "ready": orchestrator.is_ready(),
        "inflight_jobs": orchestrator.inflight_count(),
        "max_workers": settings.max_workers,
    }


if __name__ == "__main__":
    uvicorn.run("worker.main:app", host=settings.health_host, port=settings.health_port, reload=False)

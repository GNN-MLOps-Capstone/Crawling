from fastapi import FastAPI

from app.api.recommend import router as recommend_router
from app.core.config import settings
from app.core.logging import setup_logging

setup_logging()
app = FastAPI(title=settings.app_name, version=settings.app_version)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(recommend_router)

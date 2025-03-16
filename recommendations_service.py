import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel

from recsyslib.recomendations import Recommendations


class Recommendation(BaseModel):
    track_id: int
    track: str
    score: float

@dataclass
class Config:
    database: Path = Path("recommendations.db")
    item_data: Path = Path("ym/catalog_names.parquet")
    personal_recs: Path = Path("recommendations/personal_als.parquet")
    similar_items: Path = Path("data/similar_items.parquet")


_REC_STORE = Recommendations(Config.database)

def _update_store():
    _REC_STORE.load_items(Config.item_data)
    _REC_STORE.load_personal(Config.personal_recs)
    _REC_STORE.load_similar_items(Config.similar_items)

logger = logging.getLogger("uvicorn.error")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting")
    if not Config.database.exists():
        _update_store()
    try:
        yield
    finally:
        logger.info("Stopping")

app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/update_store")
def update_store():
    _update_store()
    return {"message": "OK"}

@app.get("/recommendations")
async def recommendations(user_id: int, k: int = 100) -> list[Recommendation]:
    recs = _REC_STORE.get(user_id, k)
    return [Recommendation(track_id=rec.id, track=rec.name, score=rec.score) for rec in recs.itertuples()]

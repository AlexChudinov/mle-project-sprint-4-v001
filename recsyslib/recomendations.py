import logging as logger
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from .parquet import parquet_upploader
from .sqlite import read_sqlite3_dump, sqlite3_connection


@dataclass
class ReccomendationsStats:
    request_personal_count: int = 0
    request_similar_count: int = 0


class Recommendations:

    def __init__(self, database_path: Path):
        self._stats = ReccomendationsStats()
        self._database_path = database_path

    def load_items(self, path: Path):
        logger.info(f"Loading items from file: '{path}'")

        if not path.exists():
            raise ValueError(f"Items file '{path}' not found")
        items = pd.read_parquet(path)

        if set(items.columns) != {"id", "type", "name"}:
            raise ValueError(f"Items data file '{path}' has invalid columns: {items.columns}")

        with sqlite3_connection(self._database_path) as conn:
            items.to_sql("items", conn, if_exists="replace", index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS items_idx ON items (type, id)")

        logger.info(f"Items loaded")

    def load_personal(self, path: Path):
        logger.info(f"Loading personal recommendations from file: '{path}'")
    
        if not path.exists():
            raise ValueError(f"Personal recommendations file '{path}' not found")
        personal = pd.read_parquet(path)
        personal["score"] /= personal["score"].max()

        if set(personal.columns) != {"user_id", "item_id", "score"}:
            raise ValueError(f"Personal recommendations data file '{path}' has invalid columns: {personal.columns}")

        with sqlite3_connection(self._database_path) as conn:
            personal.to_sql("personal", conn, if_exists="replace", index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS personal_idx ON personal (user_id, item_id)")

        logger.info(f"Personal loaded")

    def load_user_types(self, path: Path):
        logger.info(f"Loading user types from file: '{path}'")

        if not path.exists():
            raise ValueError(f"User types file '{path}' not found")
        user_types = pd.read_parquet(path)

        if set(user_types.columns) != {"user_id", "type"}:
            raise ValueError(f"User types data file '{path}' has invalid columns: {user_types.columns}")

        with sqlite3_connection(self._database_path) as conn:
            user_types.to_sql("user_types", conn, if_exists="replace", index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS user_types_idx ON user_types (user_id)")

        logger.info(f"User types loaded")

    def load_online_events(self, path: Path, split_date: datetime):
        logger.info(f"Loading online events from file: '{path}'")

        for batch in parquet_upploader(path, 1000000):
            with sqlite3_connection(self._database_path) as conn:
                batch  = batch.to_pandas()[pd.to_datetime(batch["started_at"]) >= split_date][["user_id", "item_id"]]
                batch.to_sql("online_events", conn, if_exists="append", index=False)

            with sqlite3_connection(self._database_path) as conn:
                conn.execute("CREATE INDEX IF NOT EXISTS online_events_idx ON online_events (user_id, item_id)")

        logger.info(f"Online events loaded")

    def load_similar_items(self, path: Path):
        if not path.exists():
            raise ValueError(f"Similar items file '{path}' not found")
        similar_items = pd.read_parquet(path)

        if set(similar_items.columns) != {"item_id", "similar_item_id", "score"}:
            raise ValueError(f"Similar items data file '{path}' has invalid columns: {similar_items.columns}")
        similar_items["score"] /= similar_items["score"].max()

        similar_items = similar_items[similar_items["item_id"] != similar_items["similar_item_id"]]

        with sqlite3_connection(self._database_path) as conn:
            similar_items.to_sql("similar_items", conn, if_exists="replace", index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS similar_items_idx ON similar_items (item_id, similar_item_id)")

        logger.info(f"Similar items loaded")

    def get(self, user_id: int, k: int=100) -> pd.DataFrame:
        personal = read_sqlite3_dump(
            self._database_path,
            f"""
                SELECT
                    i.id, i.name, p.score
                FROM personal p
                JOIN items i ON i.id = p.item_id
                WHERE
                    p.user_id = {user_id}
                    AND EXISTS (SELECT 1 FROM user_types ut WHERE ut.user_id = p.user_id AND ut.type != 'cold')
                    AND i.type = 'track'
                ORDER BY p.score DESC
                LIMIT {k};
            """)

        similar = read_sqlite3_dump(
            self._database_path,
            f"""
                SELECT
                    i.id, i.name, s.score
                FROM online_events e
                JOIN similar_items s ON s.item_id = e.item_id
                JOIN items i ON i.id = s.similar_item_id
                WHERE e.user_id = {user_id} AND i.type = 'track'
                ORDER BY s.score DESC
                LIMIT {k};
            """)

        self._stats.request_personal_count += bool(len(personal))
        self._stats.request_similar_count += bool(len(similar))

        return pd.concat([personal, similar], ignore_index=True).sort_values("score", ascending=False)[:k]

    def stats(self):
        logger.info("Recommendations stats:")
        for name, value in asdict(self._stats).items():
            logger.info(f"{name:<30} {value} ")

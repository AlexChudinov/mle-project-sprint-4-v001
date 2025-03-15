import logging as logger
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

from .sqlite import read_sqlite3_dump, sqlite3_connection


@dataclass
class ReccomendationsStats:
    request_personal_count: int = 0
    request_default_count: int = 0

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

        logger.info(f"Loaded")

    def load_personal(self, path: Path):
        logger.info(f"Loading personal recommendations from file: '{path}'")
    
        if not path.exists():
            raise ValueError(f"Personal recommendations file '{path}' not found")
        personal = pd.read_parquet(path)

        if set(personal.columns) != {"user_id", "item_id", "score"}:
            raise ValueError(f"Personal recommendations data file '{path}' has invalid columns: {personal.columns}")

        with sqlite3_connection(self._database_path) as conn:
            personal.to_sql("personal", conn, if_exists="replace", index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS personal_idx ON personal (user_id)")

        logger.info(f"Loaded")

    def get(self, user_id: int, k: int=100) -> pd.DataFrame:
        recs = read_sqlite3_dump(
            self._database_path,
            f"""
                SELECT
                    i.name, p.score
                FROM personal p
                JOIN items i ON i.id = p.item_id
                WHERE p.user_id = {user_id} AND i.type = 'track'
                ORDER BY p.score DESC
                LIMIT {k}
            """
        )

        self._stats.request_personal_count += bool(len(recs))

        return recs

    def stats(self):
        logger.info("Recommendations stats:")
        for name, value in asdict(self._stats).items():
            logger.info(f"{name:<30} {value} ")

import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pandas as pd


@contextmanager
def sqlite3_connection(dump_file_name: str | Path):
    connection = sqlite3.connect(dump_file_name)
    try:
        yield connection
    finally:
        connection.commit()
        connection.close()

def read_sqlite3_dump(dump_file_name: str | Path, query: str) -> pd.DataFrame:
    with sqlite3_connection(dump_file_name) as connection:
        return pd.read_sql_query(query, connection)

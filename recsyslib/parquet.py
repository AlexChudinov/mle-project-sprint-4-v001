from pathlib import Path
from typing import Generator

import pandas as pd
import pyarrow as pa
from pyarrow.parquet import ParquetFile


def parquet_upploader(file_name: str, batch_size: int = 1000) -> Generator:
    pf = ParquetFile(file_name) 
    yield from pf.iter_batches(batch_size=batch_size)

def upload_nrows_to_df(file_name: str | Path, nrows: int = 1000) -> pd.DataFrame:
    for first_nrows in parquet_upploader(file_name, batch_size=nrows):
        return pa.Table.from_batches([first_nrows]).to_pandas()

def get_parquet_row_count(file_path):
    parquet_file = ParquetFile(file_path)
    num_rows = parquet_file.metadata.num_rows
    return num_rows

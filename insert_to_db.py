import os

import polars as pl
import psycopg2
import pyarrow.parquet as pq
from psycopg2.extras import execute_values
from tqdm import tqdm

PARQUET_FILE = os.environ.get("RPLS_PARQUET_FILE", "2025_rpls_lazy.parquet")
TABLE_NAME = os.environ.get("RPLS_TABLE_NAME", "rpls_logement")
CHUNK_SIZE = int(os.environ.get("RPLS_CHUNK_SIZE", "5000"))
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://rpls:rpls@localhost:5432/rpls"
)

COLUMNS = [
    "DEP_CODE",
    "DEPCOM",
    "CODEPOSTAL",
    "LIBCOM",
    "NUMVOIE",
    "NOMVOIE",
    "ETAGE",
    "LIEUDIT",
    "QPV_CODE",
    "NBPIECE",
    "PLG_IRIS2024_CODE",
    "TYPECONST_CODE",
    "CUS",
    "result_id",
]

# Colonnes du parquet qui portent un nom différent dans la table SQL.
SQL_COLUMN_OVERRIDES = {
    "result_id": "ban_id",
}

SQL_COLUMNS = [SQL_COLUMN_OVERRIDES.get(c, c.lower()) for c in COLUMNS]

INSERT_SQL = "INSERT INTO {table} ({columns}) VALUES %s".format(
    table=TABLE_NAME,
    columns=", ".join(SQL_COLUMNS),
)

CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS {table} ({columns})".format(
    table=TABLE_NAME,
    columns=", ".join(f"{c} TEXT" for c in SQL_COLUMNS),
)
CREATE_INDEX_SQL = "CREATE INDEX idx_ban_id ON {table} (ban_id)".format(
    table=TABLE_NAME,
)

def create_table_if_not_exists(cursor) -> None:
    cursor.execute(CREATE_TABLE_SQL)

def create_index_ban_id(cursor) -> None:
    cursor.execute(CREATE_INDEX_SQL)


def iter_chunks(path: str, columns: list[str], chunk_size: int):
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=chunk_size, columns=columns):
        yield pl.from_arrow(batch)


def insert_chunk(cursor, chunk: pl.DataFrame) -> None:
    execute_values(cursor, INSERT_SQL, chunk.rows())


def main() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    total_rows = pq.ParquetFile(PARQUET_FILE).metadata.num_rows

    try:
        with conn.cursor() as cursor, tqdm(total=total_rows, desc="Insertion en base") as pbar:
            create_table_if_not_exists(cursor)
            conn.commit()
            for chunk in iter_chunks(PARQUET_FILE, COLUMNS, CHUNK_SIZE):
                insert_chunk(cursor, chunk)
                conn.commit()
                pbar.update(chunk.height)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

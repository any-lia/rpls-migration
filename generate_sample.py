import io
import time
import polars as pl
import datetime as dt
import requests
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from polars import DataFrame
from tqdm import tqdm

BAN_HELPER_URL = "http://localhost:7878/"

df = pl.read_csv("2025-rpls.csv", separator=";", infer_schema=False).select(
    pl.col("DEP_CODE"),
    pl.col("DEPCOM"),
    pl.col("CODEPOSTAL"),
    pl.col("LIBCOM"),
    pl.col("NUMVOIE"),
    pl.col("NOMVOIE"),
    pl.col("ETAGE"),
    pl.col("LIEUDIT"),
    pl.col("QPV_CODE"),
    pl.col("NBPIECE"),
    pl.col("PLG_IRIS2024_CODE"),
    pl.col("TYPECONST_CODE"),
    pl.col("CUS"),
)

df.limit(1000).write_csv("2025-rpls-sample.csv", separator=";")
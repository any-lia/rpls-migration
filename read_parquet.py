import io
import time
import polars as pl
import datetime as dt
import requests
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from polars import DataFrame
from tqdm import tqdm
import pandas as pd

df = pd.read_parquet("2025_rpls_lazy.parquet")

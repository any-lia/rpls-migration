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

DEFAULT_SCHEMA = {
    "ETAGE": pl.String,
    "NUMVOIE": pl.String,
    "DEP_CODE": pl.String,
    "DEPCOM": pl.String,
    "UU2020": pl.String,
    "SRU_ALINEA": pl.String,
    "CODSEGPATRIM": pl.String,
    "AAV2020": pl.String,
    "EPCI_CODE": pl.String,
    "PLG_IRIS2024_CODE": pl.String,
    "result_housenumber": pl.String,
}


def fetch_ban_id(row) -> str:
    numero_voie = row["NUMVOIE"]
    voie = row["NOMVOIE"]
    code_postal = row["CODEPOSTAL"]
    city = row["LIBCOM"]

    params = {
      "q": f"{numero_voie} {voie}",
      "autocomplete": "1",
      "index": "address",
      "limit": "2",
      "returntruegeometry": "false",
      "postcode": post_code,
      "city": city,
    }
    response = requests.get(f"{BAN_HELPER_URL}search", params=params)

    if response.status_code == 200:
        return {
            "CODEPOSTAL": code_postal,
            "LIBCOM": city,
            "NUMVOIE": numero_voie,
            "NOMVOIE": voie,
            "ban_id": response.json().get("features")[0].get("properties")["id"]
        }
    return None


def batch_fetch_ban_id(dataframe: DataFrame) -> DataFrame:
    url = BAN_HELPER_URL + "search/csv"

    csv_buffer = io.StringIO()
    dataframe.select(
        pl.col("CODEPOSTAL"),
        pl.col("DEPCOM"),
        pl.col("LIBCOM"),
        pl.col("NUMVOIE"),
        pl.col("NOMVOIE")
    ).write_csv(csv_buffer)
    csv_data = csv_buffer.getvalue()

    headers = {"accept": "text/csv"}
    data = {
        "citycode": "DEPCOM",
        "postcode": "CODEPOSTAL",
        "category": "address",
        "data": "data.csv;type=text/csv",
    }

    files = {
        'data': ('data.csv', csv_data, 'text/csv'),
    }

    response = requests.post(url, headers=headers, files=files, data=data)

    pbar.update(len(dataframe))

    if response.status_code == 200:
        df_response = pl.read_csv(io.BytesIO(response.content), infer_schema=False)
        return dataframe.join(df_response.select(
            pl.col("result_id"),
            pl.col("CODEPOSTAL"),
            pl.col("DEPCOM"),
            pl.col("LIBCOM"),
            pl.col("NUMVOIE"),
            pl.col("NOMVOIE"),
        ), on=["CODEPOSTAL", "DEPCOM", "LIBCOM", "NUMVOIE", "NOMVOIE"], how="inner")
    else:
        print(f"Error while fetching BANs : {response.status_code} - {response.content}")

    return dataframe.with_columns(
        result_id=pl.lit("")
    )

pbar = tqdm(desc="Traitement des ban")

def lazy_batch(file):
    # does not support custom chunk size
    df = pl.scan_csv(file, separator=";", infer_schema=False).select(
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

    pbar.total = 5492000

    df = df.map_batches(
        batch_fetch_ban_id,
        schema={
            **df.collect_schema(),
            "result_id": pl.String,
        },
        streamable=True,
    ).sink_parquet("2025_rpls_lazy.parquet")

def iter_batch(file):
    results = []
    CHUNK_SIZE = 2500

    df = pl.read_csv(file, separator=";", infer_schema=False).select(
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
    pbar.total = len(df)

    chunks = df.iter_slices(n_rows=CHUNK_SIZE)

    with ThreadPoolExecutor(max_workers=12) as executor:
        concurrent_results = list(executor.map(batch_fetch_ban_id, chunks))

    # 2. On réassemble le DataFrame
    df_final = pl.concat(concurrent_results)
    df_final.write_parquet("2025_rpls_iter.parquet")

#lazy_batch("2025-rpls.csv")
lazy_batch("2025-rpls.csv")

pbar.close()
#
#df_final = (
#    df.join(df_results, on=["CODEPOSTAL", "LIBCOM", "NUMVOIE", "NOMVOIE"], how="left")
#)
#df_final.write_csv("2025-rpls_with_banid.csv")



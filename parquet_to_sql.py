import pandas as pd
import numpy as np
import math
import time, timeit
import argparse

from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")

    df = pd.read_parquet("./yellow_tripdata_2021-01.parquet")

    df = df.rename(
        {
            "VendorID": "vendorid",
            "RatecodeID": "ratecodeid",
            "PULocationID": "pulocationid",
            "DOLocationID": "dolocationid",
        },
        axis=1,
    )

    def col_astype(col, dtype):
        df[col] = df[col].astype(dtype)

    col_astype("vendorid", pd.Int16Dtype())
    col_astype("passenger_count", pd.Int8Dtype())
    col_astype("ratecodeid", pd.Int8Dtype())
    col_astype("payment_type", pd.Int8Dtype())
    col_astype("pulocationid", pd.Int16Dtype())
    col_astype("dolocationid", pd.Int16Dtype())

    df_chunks = np.array_split(df, math.ceil(df.shape[0] / n_chunks))

    df.head(0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")

    for each in df_chunks:
        start = time.time()
        each.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")
        end = time.time()

        print(f"pushed a chunk.......{end - start:.3f}s")


pd.set_option("display.max_rows", 15)
pd.set_option("display.max_column", 10)

n_chunks = 100000

parser = argparse.ArgumentParser(description="Ingest Parquet to PostGreSQL")

# user
# password
# host
# port
# database name
# table name
# url of the csv

parser.add_argument("user", help="user for PostGres")
parser.add_argument("password", help="password for PostGres")
parser.add_argument("host", help="host for PostGres")
parser.add_argument("port", help="port for PostGres")
parser.add_argument("db", help="database name for PostGres")
parser.add_argument("table_name", help="table name for PostGres")
parser.add_argument("url", help="url of the csv for PostGres")

args = parser.parse_args()
print(args.accumulate(args.integers))

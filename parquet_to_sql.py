import pandas as pd
import numpy as np
import math
import time, timeit
import argparse
import os

from sqlalchemy import create_engine
from prefect import flow, task


@task(name="PG Connection")
def connection(params):
    """Takes in parameters, creates pg enginer and returns table_name, url, n_chunks, engine

    Args:
        params (tuple): user, password, host, port, db, table_name, url, n_chunks

    Returns:
        tuple: table_name, url, n_chunks, engine
    """

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    n_chunks = params.chunks

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    return table_name, url, n_chunks, engine


@task(name="Download CSV")
def download_csv(url, csv_name):
    """wgets url and output into output.csv

    Args:
        url (str): valid url whole address
    """
    # csv_name = "output.csv"

    # download csv
    os.system(f"wget {url} -O {csv_name}")


@task
def clean_taxi(csv_name):
    df = pd.read_parquet(csv_name)

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

    return df


@task(log_prints=True, retries=3)
def ingest_data(df, table_name, n_chunks, engine):
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    if n_chunks:
        df_chunks = np.array_split(df, math.ceil(df.shape[0] / n_chunks))
        for each in df_chunks:
            start = time.time()
            each.to_sql(name=table_name, con=engine, if_exists="append")
            end = time.time()

            print(f"pushed a chunk.......{end - start:.3f}s")
    else:
        start = time.time()
        df.to_sql(name=table_name, con=engine, if_exists="replace")
        end = time.time()

        print(f"finished in.......{end - start:.3f}s")


pd.set_option("display.max_rows", 15)
pd.set_option("display.max_column", 10)


@flow(name="Ingest Flow")
def main():
    parser = argparse.ArgumentParser(description="Ingest Parquet to PostGreSQL")

    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the csv
    # number of chunks, default 100000

    parser.add_argument("--user", help="user for PostGres")
    parser.add_argument("--password", help="password for PostGres")
    parser.add_argument("--host", help="host for PostGres")
    parser.add_argument("--port", help="port for PostGres")
    parser.add_argument("--db", help="database name for PostGres")
    parser.add_argument("--table_name", help="table name for PostGres")
    parser.add_argument("--url", help="url of the csv for PostGres")
    parser.add_argument("--chunks", help="url of the csv for PostGres", default=0)

    args = parser.parse_args()

    csv_name = "output.csv"

    table_name, url, n_chunks, engine = connection(args)
    download_csv(url, csv_name)
    df = clean_taxi(csv_name)
    ingest_data(df, table_name, n_chunks, engine)


if __name__ == "__main__":
    main()

import pandas as pd
import numpy as np
import math
import time, timeit
import argparse
import os
import pyarrow

from pathlib import Path
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_aws.s3 import S3Bucket, MinIOCredentials
from datetime import timedelta


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
    """wgets url and output into 'csv_name' (name of variable)

    Args:
        url (str): valid url whole address
    """
    # csv_name = "output.csv"

    # download csv
    os.system(f"wget {url} -O {csv_name}")


@task(name="Schema Typing")
def pre_taxi(csv_name):
    if csv_name.split(".")[-1] == "csv":
        df = pd.read_csv(csv_name)
    else:
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


@task(name="Transform")
def transform_data(df: pd.DataFrame):
    # Do some transforms
    return df


@task(
    name="Ingest to PostGreSQL",
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
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


@task(name="Create Minio block")
def create_minio_block(user="admin", password="password"):
    MinIOCredentials(minio_root_user=user, minio_root_password=password).save(
        "minio-default"
    )

    minio_credentials = MinIOCredentials.load("minio-default")
    s3_bucket = S3Bucket(bucket_name="data", minio_credentials=minio_credentials)
    return s3_bucket


@flow(name="Ingest to MinIO")
def ingest_minio(df: pd.DataFrame):
    minio_block = create_minio_block()
    path = "transformed.parquet"
    df.to_parquet(path)

    minio_block.upload_from_path(from_path=path, to_path=Path(f"parquet/{path}"))

    return


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
    # number of chunks, default 0 which is no chunking

    parser.add_argument("--user", help="user for PostGres")
    parser.add_argument("--password", help="password for PostGres")
    parser.add_argument("--host", help="host for PostGres")
    parser.add_argument("--port", help="port for PostGres")
    parser.add_argument("--db", help="database name for PostGres")
    parser.add_argument("--table_name", help="table name for PostGres")
    parser.add_argument("--url", help="url of the data")
    parser.add_argument(
        "--chunks",
        help="size of chunks to break the data into, default is whole",
        default=0,
    )

    args = parser.parse_args()
    table_name, url, n_chunks, engine = connection(args)

    csv_name = f'output.{url.split(".")[-1]}'

    download_csv(url, csv_name)
    df = transform_data(pre_taxi(csv_name))

    # path = Path(f'data/{csv_name}')
    # ingest_data(df, table_name, n_chunks, engine)
    ingest_minio(df)


if __name__ == "__main__":
    main()

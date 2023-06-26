import pandas as pd
import numpy as np
import math
import time, timeit

from sqlalchemy import create_engine

pd.set_option("display.max_rows", 15)
pd.set_option("display.max_column", 10)

n_chunks = 100000

engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")
engine.connect()

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

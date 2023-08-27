FROM python:3.9-slim-bullseye

RUN apt-get update \
    && apt-get install -y wget
RUN pip install pandas sqlalchemy psycopg psycopg2-binary pyarrow

WORKDIR /app
COPY parquet_to_sql.py parquet_to_sql.py

ENTRYPOINT [ "python", "parquet_to_sql.py" ]
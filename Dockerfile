FROM python:3.9-slim-bullseye

RUN pip install pandas
RUN docker pull dpage/pgadmin4

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]
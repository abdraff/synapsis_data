FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir \
    pandas==2.1.3 \
    clickhouse-connect==0.6.23 \
    mysql-connector-python==8.2.0 \
    requests==2.31.0 \
    python-dotenv==1.0.0 \
    pydantic==2.5.0 \
    schedule==1.2.0 \
    great-expectations==0.18.8 \
    psycopg2-binary \
    apache-airflow-providers-postgres \
    apache-airflow-providers-mysql

WORKDIR /opt/airflow
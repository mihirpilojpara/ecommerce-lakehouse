FROM apache/airflow:3.0.2-python3.11

# Spark needs a Java runtime.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# PySpark + Delta Lake in Airflow's environment (they only pull in py4j).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# dbt lives in its own virtualenv so its dependencies never clash with Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-duckdb==1.9.4

# Spark downloads the Delta and Postgres JDBC jars here on first run.
RUN mkdir -p /home/airflow/.ivy2

# Airflow 2.x + OpenJDK 17 + PySpark 3.5 (+ Kafka connector jars) + dbt-duckdb.
FROM apache/airflow:2.11.2-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/java-17
ENV JAVA_HOME=/usr/lib/jvm/java-17

# dbt in its own venv; `dbt` on PATH for every user.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN /usr/local/bin/python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

USER airflow
COPY requirements.txt requirements-dev.txt /tmp/
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt -r /tmp/requirements-dev.txt

# Spark's Kafka source is not part of the pyspark wheel: bake the exact jars in so
# spark-submit never needs to reach Maven at run time.
ARG SPARK_VERSION=3.5.8
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL_VERSION=2.11.1
RUN set -eu; \
    jars="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    m=https://repo1.maven.org/maven2; \
    for url in \
      "$m/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "$m/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "$m/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "$m/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar"; \
    do curl -fsSL -o "$jars/$(basename "$url")" "$url"; done

ENV PYTHONPATH=/opt/project \
    DATA_DIR=/opt/data \
    DBT_PROFILES_DIR=/opt/project/dbt \
    PYSPARK_PYTHON=python
WORKDIR /opt/project

# Spark 3.4.1, Scala 2.12, Hadoop 3.3, JupyterLab
FROM apache/spark:3.5.0-python3

USER root

ARG PROJECT_CODE_PATH

COPY ${PROJECT_CODE_PATH}/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY ./platform/airflow/include/dependencies/* /opt/bitnami/spark/jars

ENV DEBIAN_FRONTEND=noninteractive

ENV PYSPARK_DRIVER_PYTHON_OPTS=""

RUN apt-get update && apt-get install -y wget netcat-traditional &&\
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/bitnami/spark/logs && \
    chown -R 1000:100 /opt/bitnami/spark/logs
    
# USER jovyan

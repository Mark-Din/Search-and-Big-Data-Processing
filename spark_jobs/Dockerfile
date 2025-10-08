# Spark 3.4.1, Scala 2.12, Hadoop 3.3, JupyterLab
FROM bitnami/spark:3.5.0

USER root

COPY ./spark_jobs/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY ./airflow/include/dependencies/* /opt/bitnami/spark/jars

# COPY ./dist/bdcommon-0.1.0-py3-none-any.whl /tmp/
# RUN pip install --no-cache-dir /tmp/bdcommon-0.1.0-py3-none-any.whl

ENV DEBIAN_FRONTEND=noninteractive

ENV PYSPARK_DRIVER_PYTHON_OPTS=""

RUN apt-get update && apt-get install -y wget netcat-traditional &&\
    rm -rf /var/lib/apt/lists/*
    # Cleaning up the apt cache and removing /var/lib/apt/lists helpes keep the image size down.
    # wget -qO /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    # wget -qO /opt/bitnami/spark/jars/delta-spark_2.12-3.1.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar &&\
    # chown -R 1000:100 /opt/bitnami/spark/jars

RUN mkdir -p /opt/bitnami/spark/logs && \
    chown -R 1000:100 /opt/bitnami/spark/logs
    
# USER jovyan

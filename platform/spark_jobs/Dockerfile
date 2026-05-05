# Spark 3.4.1, Scala 2.12, Hadoop 3.3
FROM apache/spark:3.5.0-python3

USER root

COPY ./spark_jobs/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY ./airflow/include/dependencies/* /opt/spark/jars
COPY ./spark_jobs/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

ENV DEBIAN_FRONTEND=noninteractive

ENV PYSPARK_DRIVER_PYTHON_OPTS=""

RUN apt-get update && apt-get install -y wget netcat-traditional &&\
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/spark/logs && \
    chown -R 1000:100 /usr/local/spark/logs
    
# USER jovyan

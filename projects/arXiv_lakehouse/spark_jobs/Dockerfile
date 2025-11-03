# Spark 3.4.1, Scala 2.12, Hadoop 3.3
FROM apache/spark:3.5.0-python3

USER root

# RUN apt-get update && \
#     apt-get install -y software-properties-common && \
#     add-apt-repository ppa:deadsnakes/ppa -y && \
#     apt-get update && \
#     apt-get install -y python3.11 python3.11-distutils && \
#     rm -rf /var/lib/apt/lists/* && \
#     ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
#     ln -sf /usr/bin/python3.11 /usr/bin/python

# # Ensure Spark uses Python 3.11
# ENV PYSPARK_PYTHON=python3.11
# ENV PYSPARK_DRIVER_PYTHON=python3.11

COPY ./spark_jobs/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY ./airflow/include/dependencies/* /opt/spark/jars

ENV DEBIAN_FRONTEND=noninteractive

ENV PYSPARK_DRIVER_PYTHON_OPTS=""

RUN apt-get update && apt-get install -y wget netcat-traditional &&\
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/bitnami/spark/logs && \
    chown -R 1000:100 /opt/bitnami/spark/logs
    
# USER jovyan

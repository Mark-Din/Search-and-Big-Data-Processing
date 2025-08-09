# Spark 3.4.1, Scala 2.12, Hadoop 3.3, JupyterLab
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ./dependencies/* /usr/local/spark/jars/

ENV DEBIAN_FRONTEND=noninteractive

ENV PYSPARK_DRIVER_PYTHON_OPTS=""

# RUN wget -qO /usr/local/spark/jars/delta-core_2.12-3.1.0.jar       https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
#     wget -qO /usr/local/spark/jars/delta-storage-2.4.0.jar          https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar && \
RUN wget -qO /usr/local/spark/jars/hadoop-aws-3.3.4.jar             https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -qO /usr/local/spark/jars/aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    chown -R 1000:100 /usr/local/spark/jars

RUN mkdir -p /usr/local/spark/logs && \
    chown -R 1000:100 /usr/local/spark/logs
    
USER jovyan

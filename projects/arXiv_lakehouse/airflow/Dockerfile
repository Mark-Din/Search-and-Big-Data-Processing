# Dockerfile
# FROM astrocrpublic.azurecr.io/runtime:3.0-6
FROM quay.io/astronomer/astro-runtime:12.8.0-python-3.11

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64 
RUN export JAVA_HOME

RUN mkdir -p /mnt/spark-tmp/local /usr/local/airflow/logs && \
    chown -R astro:astro /mnt/spark-tmp /usr/local/airflow/logs
    
RUN apt-get update && apt-get install -y wget &&\
    rm -rf /var/lib/apt/lists/*
    # Cleaning up the apt cache and removing /var/lib/apt/lists helpes keep the image size down.
    # wget -qO /usr/local/airflow/include/dependencies/aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    # wget -qO /usr/local/airflow/include/dependencies/delta-spark_2.12-3.1.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar &&\
    # chown -R 1000:100 /usr/local/airflow/include/dependencies

RUN airflow db migrate || airflow db init

# USER astro
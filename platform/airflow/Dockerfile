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

RUN mkdir -p /mnt/spark-tmp/local && \
    chmod -R 777 /mnt/spark-tmp 
    
RUN apt-get update && apt-get install -y wget &&\
    rm -rf /var/lib/apt/lists/*

USER astro
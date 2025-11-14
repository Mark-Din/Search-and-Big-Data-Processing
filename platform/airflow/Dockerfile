FROM apache/airflow:3.1.2

USER root

# Install OpenJDK-17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/* \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64 
RUN export JAVA_HOME

RUN mkdir -p /mnt/spark-tmp/local && \
    chmod -R 777 /mnt/spark-tmp 
    
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt




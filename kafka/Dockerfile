FROM confluentinc/cp-kafka-connect:7.5.0

# Install Debezium MySQL connector
RUN curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.6.2.Final/debezium-connector-mysql-2.6.2.Final-plugin.tar.gz \
    -o /tmp/debezium-mysql.tar.gz \
 && mkdir -p /usr/share/confluent-hub-components/debezium-connector-mysql \
 && tar -xzf /tmp/debezium-mysql.tar.gz -C /usr/share/confluent-hub-components/debezium-connector-mysql \
 && rm /tmp/debezium-mysql.tar.gz

# Install unzip (UBI-based images)
USER root
RUN microdnf install -y unzip && microdnf clean all

# Copy ES connector ZIP from host into image
COPY ./confluentinc-kafka-connect-elasticsearch-14.1.0 /usr/share/confluent-hub-components/elasticsearch

# Unzip into plugin path
# RUN mkdir -p /usr/share/confluent-hub-components/elasticsearch \
#  && unzip /tmp/confluentinc-kafka-connect -d /usr/share/confluent-hub-components/elasticsearch \
#  && rm /tmp/confluentinc-kafka-connect

USER appuser

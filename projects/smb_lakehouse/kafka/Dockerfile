FROM confluentinc/cp-kafka:7.6.0

USER root

WORKDIR /app

COPY ./kafka/init_kafka.sh /app

RUN chmod +x /app/init_kafka.sh

# CMD ["/bin/bash", "-c",  "/app/init_kafka.sh"]
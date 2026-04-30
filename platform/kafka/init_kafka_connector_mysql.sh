#!/bin/bash

echo "Waiting for kafka connector!"
sleep 5

echo "Delete current mysql connector in kafka-connect container if exists"
curl -X DELETE http://localhost:8083/connectors/mysql-connector

echo "Create mysql connector"


curl -X POST "http://kafka-connect_qidu:8083/connectors" \
  -H "Content-Type: application/json" \
  -d '{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres_db_container",
    "database.port": "5432",
    "database.user": "root",
    "database.password": "!QAZ2wsx",
    "database.dbname": "storage_search",

    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "debezium_publication",

    "schema.include.list": "public",
    "table.include.list": "public.files, public.logs",

    "topic.prefix": "postgres",

    "snapshot.mode": "always",

    "schema.history.internal.kafka.bootstrap.servers": "kafka_qidu:9092",
    "schema.history.internal.kafka.topic": "schema-changes.postgres"
  }
}'


# curl -X POST http://localhost:8083/connectors \
#  -H "Content-Type: application/json" \
#  -d '{
#    "name": "mysql-connector",
#    "config": {
#      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
#      "database.hostname": "mysql_db",
#      "database.port": "3306",
#      "database.user": "root",
#      "database.password": "!QAZ2wsx",
#      "database.server.id": "184054",
#      "topic.prefix": "mysql",
#      "database.include.list": "whole_corp",
#      "table.include.list": "whole_corp.whole_corp_",

#      "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
#      "schema.history.internal.kafka.topic": "schema-changes.mysql",

#      "schema.history.internal.producer.key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
#      "schema.history.internal.producer.value.serializer": "org.apache.kafka.common.serialization.StringSerializer",
#      "schema.history.internal.producer.acks": "all",

#      "include.schema.changes": "true",
#      "snapshot.mode": "initial",
#      "database.allowPublicKeyRetrieval": "true",
#      "database.sslMode": "disable"
#    }
#  }'
 
# echo "✅ MySQL connector created."
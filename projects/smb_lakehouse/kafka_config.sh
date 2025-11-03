Invoke-RestMethod `
  -Method POST `
  -Uri "http://localhost:8083/connectors" `
  -Headers @{ "Content-Type" = "application/json" } `
  -Body '{
    "name": "mysql-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "mysql_db_container",
      "database.port": "3306",
      "database.user": "root",
      "database.password": "!QAZ2wsx",
      "database.server.id": "184054",
      "topic.prefix": "mysql",
      "database.include.list": "whole_corp",
      "table.include.list": "whole_corp.whole_corp_",
      "database.history.kafka.bootstrap.servers": "kafka:9092",
      "database.history.kafka.topic": "schema-changes.mysql"
    }
  }'

Invoke-RestMethod `
  -Method POST `
  -Uri "http://localhost:8083/connectors" `
  -Headers @{ "Content-Type" = "application/json" } `
  -Body '{
    "name": "es-sink",
    "config": {
      "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
      "topics": "mysql.whole_corp.whole_corp_",
      "connection.url": "http://elasticsearch:9200",
      "connection.username": "elastic",
      "connection.password": "gAcstb8v-lFCVzCBC__a",
      "type.name": "_doc",
      "key.ignore": false,
      "schema.ignore": true,
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false"
    }
  }'


# ==========================Put==============================
Invoke-RestMethod `
  -Method PUT `
  -Uri "http://localhost:8083/connectors/mysql-connector/config" `
  -Headers @{ "Content-Type" = "application/json" } `
  -Body '{
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql_db_container",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "!QAZ2wsx",
    "database.server.id": "184054",
    "topic.prefix": "mysql",
    "database.include.list": "whole_corp",
    "table.include.list": "whole_corp.whole_corp_",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.mysql"
  }'



"""
🔑 Difference between POST and PUT

POST /connectors → used to create a new connector.
The body must include both "name" and "config".

PUT /connectors/{name}/config → used to update an existing connector.
The body must be just the "config" object (not the outer "name").
"""
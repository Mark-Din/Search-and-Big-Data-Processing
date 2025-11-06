# big-data-etl-automation-pipeline
End-to-end ETL solution integrating Airflow, Spark, Delta Lake, and object storage (MinIO) for big data workflows. 

---

## Tech Stack
| Category | Tools |
|-----------|-------|
| Programming | Python 3.x |
| Orchestration | Apache Airflow |
| Data Processing | PySpark, Delta Lake |
| Data streaming | Kafka |
| Database | MySQL |
| Object Storage | MinIO |
| Search Engine | Elasticsearch |
| Frontend | Streamlit + FastAPI |
| Visualization | Altair |
| Environment | Docker & Docker Compose |
| Dependencies | All dependencies are needed for spark running |
==Check specifically what is being used in each project==


## Architecture
See the architecture in each project's readme

## How to Run
1. Clone the Repository
```bash
git clone https://github.com/Mark-Din/big-data-ai-integration-platform.git
cd etl-automation-airflow
```
2. See in each project

## Note
Enter Kafka connector for both mysql and ES if not automatically set
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "mysql_db",
      "database.port": "3306",
      "database.user": "root",
      "database.password": "!QAZ2wsx",
      "database.server.id": "184054",
      "topic.prefix": "mysql",
      "database.include.list": "whole_corp",
      "table.include.list": "whole_corp.whole_corp_",

      "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
      "schema.history.internal.kafka.topic": "schema-changes.mysql",

      "include.schema.changes": "true",
      "snapshot.mode": "initial",
      "database.allowPublicKeyRetrieval": "true",
      "database.sslMode": "disable"
    }
  }'
```
```bash
curl -X POST http://localhost:8083/connectors \
 -H "Content-Type: application/json" \
 -d '{
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
```
Pipeline
```bash
MySQL (binlog)
   │
   ▼
[ Debezium MySQL Source Connector ]  ← inside cp-kafka-connect
   │
   ▼
Kafka topic: mysql.whole_corp.whole_corp_
   │
   ▼
[ Elasticsearch Sink Connector ]     ← inside cp-kafka-connect
   │
   ▼
Elasticsearch index: whole_corp
```

## ⚠ Disclaimer
This project is for demonstration purposes only. It uses synthetic data and does NOT include proprietary business logic or production code.


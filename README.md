# big-data-etl-automation-pipeline
End-to-end ETL solution integrating Airflow, Spark, Delta Lake, and object storage (MinIO) for big data workflows.

## Overview
“This project demonstrates an automated ETL … This project has streamlit, fastapi and elasticsearch for overall and similarity searh.”

## Features
- **Automated ETL** using Apache Airflow DAGs  
- **Extract** data from CSV (dummy data provided), loaded to MySQL first  
- **Transform** with Pandas (cleaning, type casting), if data is big then use PySpark
- **Load** into MySQL  
- **Data streaming** data from MySQL to ES for regular sync
- **Search**: vectorization & clustering with Elasticsearch 
- **Environment**: Dockerized environment for easy deployment 

## Tech Stack
Programming: Python 3.x
Workflow Orchestration: Apache Airflow
Database: MySQL, Elasticsearch
Object Storage: MinIO
Libraries: Pandas, SQLAlchemy, PySpark
Data streaming: Kafka
Dependencies : All dependencies are needed for spark running, hadoop-aws-3.3.2.jar is specially for airflow only
Containerization: Docker, Docker Compose

## Architecture
![alt text](<workflow_architecture.png>)
Data should be in mysql by default as user's input.
From mysql to MinIO => Using Spark ETL for about twice a week 
For regular ETL : Web APP → MySQL→ Extract (Airflow Task) → Transform (Pandas) → Load with structured data(MySQL)
                                                          → Save format (Delta format) → Load with structured/unstructured data(MinIO)
For big data/ML process : MySQL → Transform (Apache Spark) → Load with structured data(MySQL)
Data streaming will in process: MuSQL → ES

## How to Run
1. Clone the Repository
```bash
git clone https://github.com/Mark-Din/big-data-ai-integration-platform.git
cd etl-automation-airflow
```
2. Start Airflow and PostgreSQL with Docker
```bash
astro dev start
```
3. Access Airflow UI
URL: http://localhost:8080
Enable and run etl_pipeline DAG

4. Start other services
```bash
docker-compose up --build
```

5. minio UI
http://localhost:9001/

6. Spark UI
http://localhost:8080/

7. Enter Kafka config in connect container
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

## Results
Pipeline processes dummy_data.csv and loads clean data into mysql
ETL execution is fully automated and can be scheduled

## ⚠ Disclaimer
This project is for demonstration purposes only. It uses synthetic data and does NOT include proprietary business logic or production code.


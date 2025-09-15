# big-data-etl-automation-pipeline
End-to-end ETL solution integrating Airflow, Spark, Delta Lake, and object storage (MinIO) for big data workflows.

## Overview
This project demonstrates an automated ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline extracts data from MySQL, performs transformations, and loads the processed data into a MinIO. This project has streamlit, fastapi and elasticsearch for overall and similarity searh.
The goal is to showcase data engineering practices such as workflow automation, scheduling, error handling, and logging. Plus you can find this easily within your data.

## Features
Main storage type: MySQL
Automated ETL workflow using Apache Airflow DAGs
Extract data from CSV (dummy data provided)
Transform data with Pandas (cleaning, type casting)
Load data into MySQL
Error handling & logging for pipeline reliability
Dockerized environment for easy deployment
vectorize and cluster data for search
Use Elasticserach to extract data with vector build-in field type

## Tech Stack
Programming: Python 3.x
Workflow Orchestration: Apache Airflow
Database: MySQL, Elasticsearch
Object Storage: MinIO
Libraries: Pandas, SQLAlchemy, PySpark
Containerization: Docker, Docker Compose

## Project Structure

## Architecture
![alt text](<workflow_architecture.png>)
For regular ETL : Web APP → MySQL→ Extract (Airflow Task) → Transform (Pandas) → Load with structured data(PostgreSQL)
                                                          → Save format (Delta format) → Load with structured/unstructured data(MinIO)
For big data/ML process : MySQL → Transform (Apache Spark) → Load with structured data(PostgreSQL)

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

4. minio UI
http://localhost:9001/
5. Spark UI
http://localhost:8080/

## Results
Pipeline processes dummy_data.csv and loads clean data into mysql
ETL execution is fully automated and can be scheduled

##⚠ Disclaimer
This project is for demonstration purposes only. It uses synthetic data and does NOT include proprietary business logic or production code.


# big-data-etl-automation-pipeline
End-to-end ETL solution integrating Airflow, Spark, Delta Lake, and object storage (MinIO) for big data workflows.

## Overview
This project demonstrates an automated ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline extracts data from a CSV file, performs transformations, and loads the processed data into a PostgreSQL database.
The goal is to showcase data engineering practices such as workflow automation, scheduling, error handling, and logging.

## Features
Automated ETL workflow using Apache Airflow DAGs
Extract data from CSV (dummy data provided)
Transform data with Pandas (cleaning, type casting)
Load data into PostgreSQL
Error handling & logging for pipeline reliability
Dockerized environment for easy deployment

## Tech Stack
Programming: Python 3.x
Workflow Orchestration: Apache Airflow
Database: PostgreSQL
Libraries: Pandas, SQLAlchemy
Containerization: Docker, Docker Compose

## Project Structure

## Architecture
<img width="970" height="578" alt="image" src="https://github.com/user-attachments/assets/4b14954d-9295-4ad2-afd0-817141f65f82" />
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

## Results
Pipeline processes dummy_data.csv and loads clean data into PostgreSQL
ETL execution is fully automated and can be scheduled

##⚠ Disclaimer
This project is for demonstration purposes only. It uses synthetic data and does NOT include proprietary business logic or production code.

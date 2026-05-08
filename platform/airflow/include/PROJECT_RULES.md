# Project Rules

## Python Imports
- Use absolute imports only
- Python package root = airflow/include
- Import format:
  from code.xxx import yyy

## Airflow Rules
- DAGs contain orchestration only
- No business logic inside DAG files
- Use PythonOperator unless otherwise specified
- Timezone = Asia/Taipei
- catchup=False

## File Modification Rules
- Do not modify unrelated files
- Do not auto-refactor architecture
- Do not create compatibility shim files
- Do not duplicate config files
- Do not create __init__.py unless explicitly requested

## Architecture Rules
- common/ contains shared utilities
- analytics/ contains ETL analytics logic
- nexva_ingestion/ contains ingestion pipelines
<!-- - lakehouse/ contains Spark/Iceberg logic
- streaming/ contains Kafka and CDC logic -->

## Validation Rules
- Ensure imports compile
- Keep changes minimal and isolated
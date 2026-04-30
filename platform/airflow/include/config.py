config_mysql = {
    "host": "mysql_db_container",
    "user": "root",
    "password": "!QAZ2wsx",
    "database": "arXiv"
}

config_es = {
  "ES_PASSWORD":"gAcstb8v-lFCVzCBC__a",
  "ES_HOST":"http://10.11.60.43:9200",
  "ES_HOST_local":"http://localhost:9200",
  "ES_CA_CERT":""
}

config_pg = {
    'host': '10.11.60.43',
    'port': '5432',
    'user': 'root',
    'password': '!QAZ2wsx',  # Replace with your actual PostgreSQL password
    'db': 'storage_search'
}

config_minio = {
    'MINIO_ENDPOINT': '10.11.60.43:9000',
    'MINIO_ACCESS_KEY': 'minioadmin',
    'MINIO_SECRET_KEY': 'minioadmin',
    'MINIO_SECURE': False
}    

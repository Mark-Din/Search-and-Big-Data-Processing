import os
from pathlib import Path
from dotenv import load_dotenv


# Load .env from project root or current working directory
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None or value == "":
        return default

    return value.lower() in ("true", "1", "yes", "y")


config_mysql = {
    "host": require_env("MYSQL_HOST"),
    "user": require_env("MYSQL_USER"),
    "password": require_env("MYSQL_PASSWORD"),
    "database": require_env("MYSQL_DATABASE"),
}


config_es = {
    "ES_PASSWORD": require_env("ES_PASSWORD"),
    "ES_HOST": require_env("ES_HOST"),
    "ES_HOST_local": os.getenv("ES_HOST_LOCAL", "http://localhost:9200"),
    "ES_CA_CERT": os.getenv("ES_CA_CERT", ""),
}


config_pg = {
    "host": require_env("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "user": require_env("POSTGRES_USER"),
    "password": require_env("POSTGRES_PASSWORD"),
    "db": require_env("POSTGRES_DB"),
}


config_minio = {
    "MINIO_ENDPOINT": require_env("MINIO_ENDPOINT"),
    "MINIO_ACCESS_KEY": require_env("MINIO_ACCESS_KEY"),
    "MINIO_SECRET_KEY": require_env("MINIO_SECRET_KEY"),
    "MINIO_SECURE": get_bool_env("MINIO_SECURE", False),
}


conf_41_ssh = {
    'ip': '10.11.60.41',
    'port': 22,
    "username" :"joe",
    "password" : "qZ3*nRPrqpCQQ",
    "remote_dir" : "/home/joe/nexva-2.0/backend/dist/web_upload",
}

config_41_mysql = {
    'host': '10.11.60.41',
    'port': '3307',   
    'user': 'root',  
    'password': 'Inf0p0werc@rp',  # Replace with your actual MySQL password
    'db': 'nexva',
    'belongs_to': 'mitac',
}

config_43_mysql = {
    'host': '10.11.60.43',
    'port': '3306',   
    'user': 'root',  
    'password': '!QAZ2wsx',  # Replace with your actual MySQL password
    'db': 'qidu_quickreport',
}

config_pg = {
    # 'host': 'localhost',
    'host': '10.11.60.43',
    'port': '5432',
    'user': 'root',
    'password': '!QAZ2wsx',  # Replace with your actual PostgreSQL password
    'db': 'storage_search'
}

config_43_minio = {
    'MINIO_ENDPOINT': '10.11.60.43:9000',
    # 'MINIO_ENDPOINT': 'localhost:9000',
    'MINIO_ACCESS_KEY': 'minioadmin',
    'MINIO_SECRET_KEY': 'minioadmin',
    'MINIO_SECURE': False,
    "bucket" : "nexva"
}    
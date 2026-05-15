from sqlalchemy import create_engine, text
from urllib.parse import quote

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, SSLError
from minio import Minio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

import sys
import time

sys.path.append('/opt/airflow/include/')
sys.path.append(r'D:\markding_git\big-data-ai-integration-platform\platform\airflow\include\code\common')
from config import config_es, config_pg
from init_log import initlog

class DatabaseConnection:
    """Handles database connections."""

    @staticmethod
    def create_connection_string(user, password, host, port, db_name=None, driver=''):
        password = str(password)
        password = quote(password)  # Ensure the password is URL encoded
        db_path = f"{host}:{port}"
        if db_name:
            db_path += f"/{db_name}"
        return f"{driver}://{user}:{password}@{db_path}"

    @staticmethod
    def mysql_connection(config, db_name=None):
        password = config['password']
        driver = 'mysql+mysqlconnector'
        user = config['user']
        host = config['host']
        port = config['port']
        db_name = db_name if db_name is not None else config.get('db')
        return create_engine(DatabaseConnection.create_connection_string(user, password, host, port, db_name, driver))

    @staticmethod
    def postgres_connection(config, db_name=None):
        password = config['password']
        driver = 'postgresql+psycopg2'
        user = config['user']
        host = config['host']
        port = config['port']
        db_name = db_name if db_name is not None else config.get('db')
        return create_engine(DatabaseConnection.create_connection_string(user, password, host, port, db_name, driver))
    
    @staticmethod
    def ssh_connection(ssh, conf_201_ssh):
        import paramiko

        hostname = conf_201_ssh["hostname"]
        port = conf_201_ssh["port"]
        username = conf_201_ssh["username"]
        password = conf_201_ssh["password"]

        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, port, username, password)
        sftp = ssh.open_sftp()

        ssh.close()

        return sftp
    
def create_database(engine, db_name):
    """Creates a database if it does not exist."""
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))



logger = initlog('fastAPI_file_UploadAndProcess')


class ElasticSearchConnectionManager:
    def __init__(self, config):
        self.host = config['ES_HOST']
        self.password = config['ES_PASSWORD']
        self.user = config['ES_USER']
        self.retries = 3
        self.delay = 0.5
        self.client = None

    def connect(self):
        for attempt in range(1, self.retries + 1):
            logger.info(f'Trying to connect to elasticserach instance {attempt}')
            try:
                es = Elasticsearch(
                    self.host,
                    basic_auth=(self.user, self.password),
                    verify_certs=False,
                    request_timeout=10,
                    max_retries=self.retries,
                    retry_on_timeout=True
                )

                if es.ping():
                    logger.info('ES conneciton established')
                    self.client = es
                    return es
                
            except Exception as e:
                logger.error(
                    f"Connection failed to established",
                    exc_info=True
                )
    
        time.sleep(self.delay)
        
        raise ConnectionError("[ES] Could not connect after retries")


class SQLAlchemyConnection:
    def __init__(self, config):
        conn_str = (
            f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}"
        )

        self.engine = create_engine(
            conn_str
        )

        self.SessionaLocal = sessionmaker(
            autocommit=False, autoflush= False, bind= self.engine
        )

        self.Base = declarative_base()

    def get_db(self):
        try:
            db = self.SessionaLocal()
            yield db
        finally:
            db.close()

class MinIOConnection:
    def __init__(self, config):
        self.endpoint_internal = config['MINIO_INTERNAL']
        self.endpoint_public = config['MINIO_PUBLIC']
        self.password = config['MINIO_SECRET_KEY']
        self.access_key = config['MINIO_ACCESS_KEY']
        self.secure = config['MINIO_SECURE']

    def connect(self, type_ = 'public'):
        logger.info(f"type_==============: {type_}")

        # Save = internal, any data operations
        if type_ == "save":
            endpoint = self.endpoint_internal

        # Signing file URL for preview/download
        elif type_ == "public":
            endpoint = self.endpoint_public
        else:
            raise ValueError("Invalid MinIO client type")
        
        print(f"MinIO endpoint selected: {endpoint}")

        client = Minio(
            endpoint,
            access_key=self.access_key,
            secret_key=self.password,
            secure=self.secure
        )

        try:
            client.list_buckets()
            logger.info("MinIO connection established")
        except Exception as e:
            logger.error(f"MinIO connection failed: {e}", exc_info=True)
            raise ConnectionError("{[MinIO]} Could not connect to MinIO server")
        
        return client


class ConnectionManager:
    _es_instance = None
    _db_instance = None
    
    @classmethod
    def elastic(cls):
        if cls._es_instance is None:
            cls._es_instance = ElasticSearchConnectionManager(config_es).connect()
        return cls._es_instance
    
        
    @classmethod
    def sqlalchemy(cls):
        if cls._db_instance is None:
            cls._db_instance = SQLAlchemyConnection(config_pg)
        return cls._db_instance



    


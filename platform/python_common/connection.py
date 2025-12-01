from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, SSLError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from common.conf import config_es, config_pg
import time
import logging

logger = logging.getLogger(__name__)

class ElasticsearchConnection:
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

                if es.ping:
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

        self.SessionalLocal = sessionmaker(
            autocommit=False, autoflush= False, bind= self.engine
        )

        self.Base = declarative_base()

    def get_db(self):
        try:
            db = self.SessionalLocal()
            yield db
        finally:
            db.close()


class ConnectionManager:
    _es_instance = None
    _db_instance = None
    
    @classmethod
    def elastic(cls):
        if cls._es_instance is None:
            es_conn = ElasticsearchConnection(
                config_es
            )
            cls._instance = es_conn.connect()
        return cls._instance
    
        
    @classmethod
    def sqlalchemy(cls):
        if cls._db_instance is None:
            cls._db_instance = SQLAlchemyConnection(config_pg)
        return cls._db_instance
    

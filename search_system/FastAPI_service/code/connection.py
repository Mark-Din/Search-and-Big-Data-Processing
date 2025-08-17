from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, SSLError
from ssl import create_default_context, CERT_NONE
import os
import time
import logging

from pymongo import MongoClient
from common.logger import initlog
logger = initlog('connection')


class ElasticSearchConnectionManager:
    _instance = None
    _es_nodes = [
                    {"ip": os.getenv('ES_HOST'), "cafile": os.getenv('ES_CA_CERT')}, # for docker
                    {"ip": "https://172.105.226.161:9200", "cafile": r'C:\Users\infopower\ESG_Backends\FastAPI_service\http_ca.crt'} # for local test
                ]
    _max_attempts = 2

    def __new__(cls, *args, **kwargs):
        raise RuntimeError("Cannot create instance of this class. Please use the class method 'get_instance'.")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls._create_es_connection()
        return cls._instance

    @staticmethod
    def _create_es_connection():
        for attempt in range(ElasticSearchConnectionManager._max_attempts):
            for node in ElasticSearchConnectionManager._es_nodes:
                logger.info(f"Attempting to connect to Elasticsearch at {node['ip']} with CA file {node['cafile']} (Attempt {attempt + 1})")
                try:
                    # context = create_default_context(cafile=node['cafile'])
                    # context.check_hostname = False
                    # context.verify_mode = CERT_NONE

                    es = Elasticsearch(
                        [node['ip']],
                        http_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD")),
                        # ssl_context=context,
                        verify_certs=False
                    )
                    if es.ping():
                        logger.info(f"Elasticsearch connection established to {node['ip']}.")
                        return es
                except (ConnectionError, SSLError, FileNotFoundError) as e:
                    logger.error(f"Failed to connect to Elasticsearch at {node['ip']} on attempt {attempt + 1}: {e}")
            time.sleep(0.5)

        raise ConnectionError("Failed to connect to Elasticsearch after several attempts.")

        # For QuickReport service to connect to MongoDB
    @staticmethod
    def create_mongo_connection():
        try:
            client = MongoClient("mongodb://root:infopower@172.234.84.110:27016/?authSource=admin")
            db = client["reporting"]
            collection = db["shareReport"]
            logger.info("MongoDB connection established.")
            return collection
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        
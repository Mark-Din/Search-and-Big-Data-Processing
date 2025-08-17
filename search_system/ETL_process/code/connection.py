from mysql.connector import connect
from elasticsearch import Elasticsearch
from ssl import create_default_context, CERT_NONE
from elasticsearch.exceptions import ConnectionError, SSLError
import time
import json

from common.logger import initlog
logger = initlog('connection')

with open('/app/config.json', 'r') as config:
    config_json = json.loads(config.read())

class ElasticSearchConnectionManager:
    _instance = None
    _es_nodes = [
                    {"ip": config_json['ES_HOST'], "cafile": config_json['ES_CA_CERT']} # for docker
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
                    # context.check_hostname = False
                    # context.verify_mode = CERT_NONE

                    es = Elasticsearch(
                        [node['ip']],
                        http_auth=('elastic', config_json['ES_PASSWORD']),
                        verify_certs=False
                    )
                    if es.ping():
                        logger.info(f"Elasticsearch connection established to {node['ip']}.")
                        return es
                except (ConnectionError, SSLError, FileNotFoundError) as e:
                    logger.error(f"Failed to connect to Elasticsearch at {node['ip']} on attempt {attempt + 1}: {e}", exc_info=True)
            time.sleep(0.5)

        raise ConnectionError("Failed to connect to Elasticsearch after several attempts.")

    # Function to create a MySQL connection
    @staticmethod
    def mysql_connection_nexva():
        try:
            conn = connect(host='mysql_container',
                port='3306',
                user='root',
                password='!QAZ2wsx',
                database='nexva'
            )
        except:
            conn = connect(host='mysql_container',
                port='3306',
                user='root',
                password='Inf0p0werc@rp',
                database='nexva'
            )
            
        return conn
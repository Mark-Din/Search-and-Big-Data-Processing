from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, SSLError
import os

import logging
logger = logging.getLogger(__name__)


class ElasticSearchConnectionManager:
    _instance = None
    _es_nodes = {"ip": os.getenv('ES_HOST', 'http://localhost:9200'), "cafile": os.getenv('ES_CA_CERT')} # for docker
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
            node = ElasticSearchConnectionManager._es_nodes
            logger.info(f"Attempting to connect to Elasticsearch at {node['ip']} with CA file {node['cafile']})")
            try:
                # context = create_default_context(cafile=node['cafile'])
                # context.check_hostname = False
                # context.verify_mode = CERT_NONE
                http_auth=(os.getenv("ES_USERNAME", 'elastic'), os.getenv("ES_PASSWORD",'gAcstb8v-lFCVzCBC__a'))
                print(http_auth)
                es = Elasticsearch(
                    [node['ip']],
                    http_auth=http_auth,
                    # ssl_context=context,
                    verify_certs=False
                )
                if es.ping():
                    logger.info(f"Elasticsearch connection established to {node['ip']}.")
                    return es
            except (ConnectionError, SSLError, FileNotFoundError) as e:
                logger.error(f"Failed to connect to Elasticsearch at {node['ip']} on attempt {attempt + 1}: {e}")

        raise ConnectionError("Failed to connect to Elasticsearch after several attempts.")

        # For QuickReport service to connect to MongoDB
        
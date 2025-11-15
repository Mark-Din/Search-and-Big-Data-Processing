import time
from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch, helpers
import logging

logger = logging.getLogger(__name__)

def safe_json_deserializer(m):
    if not m:
        return None
    try:
        return json.loads(m.decode('utf-8'))
    except json.JSONDecodeError:
        logger.info(f"Skipped non-JSON message: {m}")
        return None

def main():
    KafkaConsumer(
            'mysql.whole_corp.whole_corp_',
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='python-consumer',
            value_deserializer=safe_json_deserializer
        )


    es = Elasticsearch(
        ["http://elasticsearch:9200"],
        basic_auth=("elastic", "gAcstb8v-lFCVzCBC__a")
    )

    logger.info("Connected to Kafka and Elasticsearch")

    while True:
        for message in consumer:
            if not message.value:
                continue
            payload = message.value.get('payload', {}).get('after', None)
            if payload:
                actions = [{
                    "_index": "whole_corp",
                    "_id": payload["統一編號"],
                    "_source": payload
                }]
                try:
                    helpers.bulk(es, actions)
                    logger.info(f"Indexed document with 統一編號: {payload['統一編號']}")
                except Exception as e:
                    logger.error(f"Error indexing document: {e}", exc_info=True)
        logger.info("No new messages — still listening...")
        time.sleep(2)

if __name__ == "__main__":
    main()
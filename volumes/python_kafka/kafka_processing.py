import time
from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch, helpers

def safe_json_deserializer(m):
    if not m:
        return None
    try:
        return json.loads(m.decode('utf-8'))
    except json.JSONDecodeError:
        print(f"⚠️ Skipped non-JSON message: {m}")
        return None

consumer = KafkaConsumer(
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

print("Connected to Kafka and Elasticsearch")

while True:
    for message in consumer:
        if not message.value:
            continue
        payload = message.value.get('payload', {}).get('after', None)
        if payload:
            print(f"Processing message with 統一編號: {payload.get('統一編號')}")
            actions = [{
                "_index": "whole_corp",
                "_id": payload["統一編號"],
                "_source": payload
            }]
            try:
                helpers.bulk(es, actions)
                print(f"Indexed document with 統一編號: {payload['統一編號']}")
            except Exception as e:
                print(f"Error indexing document: {e}")
    print("No new messages — still listening...")
    time.sleep(5)

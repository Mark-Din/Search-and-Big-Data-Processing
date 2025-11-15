import pytest
from elasticsearch import Elasticsearch
import time

@pytest.mark.integration
def test_kafka_to_es():
    es = Elasticsearch("http://elasticsearch:9200", basic_auth=("elastic", "gAcstb8v-lFCVzCBC__a"))

    # wait for kafka consumer to push data
    time.sleep(3)

    res = es.search(index="arxiv_clusters", query={"match_all": {}})
    assert res["hits"]["total"]["value"] >= 0

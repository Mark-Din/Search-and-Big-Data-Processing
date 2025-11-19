#!/bin/bash

echo "Waiting for Elasticsearch"
until curl -s http://elasticsearch:9202/_cluster/health?wait_for_status=yellow; do
    sleep 2
done

echo "Creating index..."
curl -X PUT "http://elasticsearch:9200/arxiv_clusters" \
  -u elastic:gAcstb8v-lFCVzCBC__a \
  -H "Content-Type: application/json" \
  -d @/app/mapping.json

echo "Done."
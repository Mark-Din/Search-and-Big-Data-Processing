# Dockerfile.elastic
FROM docker.elastic.co/elasticsearch/elasticsearch:8.12.2

# Set environment variables for Elasticsearch, Using --batch to avoid interactive prompts
RUN elasticsearch-plugin install --batch analysis-icu 

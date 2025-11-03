#/bin/bash

echo "Waiting for kafka!"
sleep 10

echo "Start running kafka_processing"
python /app/kafka_processing.py
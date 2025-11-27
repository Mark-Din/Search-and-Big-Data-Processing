#!/bin/bash

export SPARK_HOME=/opt/spark
export SPARK_CONF=/opt/spark/conf

# Ensure Spark picks up our Hive config
export HIVE_HOME=/opt/spark/conf

# Allow external connection for driver
export SPARK_DRIVER_BIND_ADDRESS=0.0.0.0

# Fixes UI conflicts when mutiple jobs run
export SPARK_UI_PORT=4040


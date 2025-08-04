#!/bin/bash

# Ensure the cron log directory and file exist
mkdir -p /app/cron_log
touch /app/cron_log/cron.log

# This ensures the script runs at startup for immediate testing
/usr/local/bin/python3 /app/etl_mysql_to_es.py >> /app/cron_log/startup_etl.log 2>&1

# Start cron
cron -f


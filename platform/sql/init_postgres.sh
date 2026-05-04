#!/bin/sh

sleep 5;
PGPASSWORD=$POSTGRES_PASSWORD psql \
  -h $ICEBERG_DB_HOST \
  -U $POSTGRES_USER \
  -d postgres \
  -tc "SELECT 1 FROM pg_database WHERE datname='$ICEBERG_DB_NAME'" \
  | grep -q 1 || \

PGPASSWORD=$POSTGRES_PASSWORD psql \
  -h $ICEBERG_DB_HOST \
  -U $POSTGRES_USER \
  -d postgres \
  -c "CREATE DATABASE $ICEBERG_DB_NAME;"
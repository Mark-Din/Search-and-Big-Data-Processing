from pyspark.sql import SparkSession
import os

def spark_session():
    driver_host = os.getenv("SPARK_DRIVER_HOST", "scheduler")
    # Stop any old session so new configs take effect in notebooks
    return (
    SparkSession.builder
    .appName("MySQL_to_Delta_on_MinIO")
    .master("spark://spark-master:7077")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.host", driver_host)
    .getOrCreate()
)




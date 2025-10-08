from pyspark.sql import SparkSession
import os

def spark_session():
    driver_host = os.getenv("SPARK_DRIVER_HOST", "scheduler")
    # Stop any old session so new configs take effect in notebooks
    return (
        SparkSession.builder
        .appName("MySQL_to_Delta_on_MinIO")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages",
                ",".join([
                    # Delta
                    "io.delta:delta-spark_2.12:3.1.0",
                    # MySQL JDBC
                    # "mysql:mysql-connector-java:8.0.33",
                    # S3A / MinIO (versions must match your Hadoop)
                    "org.apache.hadoop:hadoop-aws:3.3.2",  # It is for 
                    "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
                ]))
        # Delta integration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO (S3A) configs
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.ui.port", "4040")                 # fix the port
        .config("spark.driver.bindAddress", "0.0.0.0")   # listen on all ifaces
        .config("spark.driver.host", "172.30.0.2")          # OR "spark-master" – the container's DNS name
        .config("spark.ui.showConsoleProgress", "true")
        # Resources
        # .config("spark.executor.cores", "2")
        # .config("spark.executor.memory", "2g")
        # .config("spark.executor.memoryOverhead", "1536m")
        # .config("spark.network.timeout", "600s")
        .config("spark.executor.cores", "1")           # 1 task per executor (more stable for trees)
        .config("spark.executor.memory", "2g")
        .config("spark.executor.memoryOverhead", "1g")  # or omit in Standalone
        .config("spark.sql.shuffle.partitions", "50")
        # .config("spark.local.dir", "/mnt/spark-tmp/local") # For giving it much more space to run CV
        .config("spark.network.timeout", "600s")
        .getOrCreate()
    )

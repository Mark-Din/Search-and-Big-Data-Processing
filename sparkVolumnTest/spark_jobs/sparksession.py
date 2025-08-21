from pyspark.sql import SparkSession

def spark_session():
    # Stop any old session so new configs take effect in notebooks
    return (
        SparkSession.builder
        .appName("MySQL_to_Delta_on_MinIO")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages",
                ",".join([
                    # Delta
                    "io.delta:delta-spark_2.12-3.1.0",
                    # MySQL JDBC
                    "mysql-connector-j-8.3.0",
                    # S3A / MinIO (versions must match your Hadoop)
                    "org.apache.hadoop:hadoop-aws:3.3.2",
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
        # Resources
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.memoryOverhead", "512m")
        .getOrCreate()
    )
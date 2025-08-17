from pyspark.sql import SparkSession, functions as F
from common.logger import get_logger

logger = get_logger(__name__)

def spark_session():
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
                    "mysql:mysql-connector-java:8.0.33",
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

def read_from_mysql(spark):
    # 2) Read MySQL
    return (spark.read.format("jdbc")
      .option("url", "jdbc:mysql://mysql-business-only:3306/whole_corp"
                      "?useUnicode=true&characterEncoding=utf8"
                      "&serverTimezone=Asia/Taipei"
                      "&useSSL=false&allowPublicKeyRetrieval=true")
      .option("dbtable", "whole_corp")
      .option("user", "root")
      .option("password", "!QAZ2wsx")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load())


def bronze_to_silver(s):
    # Read Bronze (from MinIO or local)
    bronze_path = os.getenv("BRONZE_PATH", "s3a://deltabucket/bronze/wholeCorp_delta")
    df = s.read.parquet(bronze_path)

    # Coerce types
    to_int = ["資本額","實收資本總額","員工","年營收"]
    for c in to_int:
        if c in df.columns:
            df = df.withColumn(c, F.regexp_replace(F.col(c), r"[^\d]", "").cast("long"))

    if "成立年份" in df.columns:
        df = df.withColumn("公司年齡", F.lit(F.year(F.current_date())) - F.col("成立年份").cast("int"))

    # Trim strings
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))

    silver_path = os.getenv("SILVER_PATH", "s3a://deltabucket/silver/wholeCorp_delta")
    df.write.format("delta").mode("overwrite").save(silver_path)


def store_in_minio(df):
    # 3) Write Delta to MinIO
    (df.write.format("delta")
       .mode("overwrite")
       .save("s3a://deltabucket/bronze/wholeCorp_delta"))

def main():
    try:
        s = spark_session()
        df = read_from_mysql(s)
        store_in_minio(df)
        bronze_to_silver(s)
        s.stop()
    except Exception as e:
        logger.error(f"Error in spark job: {e}")
    finally:
        if 's' in locals():
            s.stop()
            
if __name__ == "__main__":
    main()

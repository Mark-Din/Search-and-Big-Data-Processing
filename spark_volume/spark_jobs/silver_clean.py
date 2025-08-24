from pyspark.sql import SparkSession, functions as F
from common.logger import initlog
from sparksession import spark_session
import os
logger = initlog(__name__)

s = spark_session()

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
       .save("s3a://deltabucket/bronze/wholeCorp_delta_raw"))

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

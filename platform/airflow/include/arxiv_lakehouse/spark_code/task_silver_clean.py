import datetime
import time
from pyspark.sql import functions as F, Window
import os
from include.init_log import initlog
from sparksession import spark_session
from include.mysql_log import store_metadata
from include.config import mysql_conf
from task_co_authorship import main as co_main

logger = initlog(__name__)

# ------------------------------
# Read MySQL tables (Bronze)
# ------------------------------
def read_from_mysql(spark):
    url = (
        f"jdbc:mysql://{mysql_conf['host']}:3306/{mysql_conf['database']}"
        "?useUnicode=true&characterEncoding=utf8"
        "&serverTimezone=Asia/Taipei"
        "&useSSL=false&allowPublicKeyRetrieval=true"
    )

    props = {
        "user": mysql_conf['user'],
        "password": mysql_conf['password'],
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    logger.info("Reading papers, authors, and versions tables from MySQL...")

    papers_df = spark.read.jdbc(url=url, table="papers", properties=props)
    authors_df = spark.read.jdbc(url=url, table="authors", properties=props)
    versions_df = spark.read.jdbc(url=url, table="versions", properties=props)

    return papers_df, authors_df, versions_df


# ------------------------------
# Transform and Clean (Silver)
# ------------------------------
def bronze_to_silver(spark):
    papers_df, authors_df, versions_df = read_from_mysql(spark)

    # --- Clean and normalize string columns ---
    for df_name, df in [("papers", papers_df), ("authors", authors_df), ("versions", versions_df)]:
        for c, t in df.dtypes:
            if t == "string":
                df = df.withColumn(c, F.trim(F.col(c)))
        if df_name == "papers":
            papers_df = df
        elif df_name == "authors":
            authors_df = df
        else:
            versions_df = df

    # --- Handle missing/null values ---
    papers_df = papers_df.dropna(subset=["id", "title"])
    authors_df = authors_df.dropna(subset=["paper_id", "name"])
    versions_df = versions_df.dropna(subset=["paper_id", "version"])
    authors_df = authors_df.withColumn("name", F.regexp_replace('name','\n',' '))
    
    logger.info(f'auther dataframe : {authors_df.show(5)}')
    # --- Clean and unify version strings (e.g., ensure uppercase) ---
    versions_df = versions_df.withColumn("version", F.upper(F.col("version")))

    # --- Join all tables for analytical use (optional) ---
    silver_joined = (
        papers_df.alias("p")
        .join(authors_df.alias("a"), F.col("p.id") == F.col("a.paper_id"), "left")
        .join(versions_df.alias("v"), F.col("p.id") == F.col("v.paper_id"), "left")
        .select(
            F.col("p.id").alias("paper_id"),
            F.col("p.title"),
            F.col("p.abstract"),
            F.col("p.updated"),
            F.col("p.published"),
            F.col("p.categories"),
            F.col("a.name").alias("author_name"),
            F.col("v.version"),
            F.col("v.created").alias("version_created"),
            F.col("p.comments"),
            F.col("p.journal_ref"),
            F.col("p.report_no"),
        )
    )

    # Deduplicate to keep latest version per paper
    w = Window.partitionBy("paper_id").orderBy(F.col("version_created").desc())

    latest_versions = (
        silver_joined
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # --- Drop duplicates if any ---
    latest_versions = latest_versions.dropDuplicates(["paper_id", "author_name", "version"])

    # --- Drop NA columns if all null ---
    latest_versions = latest_versions.na.drop(how="all")

    # --- Write to MinIO (Silver layer) ---
    silver_path = os.getenv("SILVER_PATH", "s3a://deltabucket/silver/arxiv_cleaned")
    (
        latest_versions.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    logger.info(f"✅ Silver data successfully written to {silver_path}")

    return latest_versions.count()

# ------------------------------
# Main entrypoint
# ------------------------------
def main():
    start = time.time()
    
    spark = None
    run_id = f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}"
    status = 'F'

    try:
        start = time.time()
        spark = spark_session()
        logger.info("Starting Bronze to Silver ETL process...")
        len_df = bronze_to_silver(spark)
        
        logger.info("Inserting into metadata...")

        status = 'S'

    except Exception as e:
        logger.error(f"❌ ETL job failed: {e}", exc_info=True)
        import sys
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")
        try:
            
            end = time.time()
            duration = end - start   # in seconds (float)

            store_metadata(
                run_id=run_id,
                stage_name='bronze_to_silver',
                record_count=len_df if 'len_df' in locals() else 0,
                duration=duration if 'duration' in locals() else 0,
                component='spark',
                note='Bronze to Silver ETL job',
                status= status
            )
        except Exception as me:
            logger.error(f"❌ Error storing metadata: {me}", exc_info=True)

if __name__ == "__main__":
    main()

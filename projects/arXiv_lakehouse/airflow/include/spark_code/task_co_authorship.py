# === task_build_coauthorship_spark.py ===
import time
from pyspark.sql import SparkSession, functions as F
from include.init_log import initlog
from include.mysql_log import store_metadata
from include.config import mysql_conf
from sparksession import spark_session
from include.init_log import initlog

logger = initlog(__name__)

def coauthor_stats(df):
    
    authors =authors.groupBy("paper_id").agg(F.collect_set("name").alias("authors"))
    
    # (optional) minimal cleaning to mirror pandas .astype(str) + strip + remove newlines
    authors = (authors
        .withColumn("name",
            F.trim(F.regexp_replace(F.regexp_replace(F.col("name").cast("string"), r"\r|\n", " "), r"\s+", " "))
        )
        .filter(F.col("name").isNotNull() & (F.col("name") != "")))

    # 2) One row per paper with a SET of unique author names (you used set() in Python)
    grp = authors.groupBy("paper_id").agg(F.collect_set("name").alias("authors")) \
                .filter(F.size("authors") > 1)

    # 3) Spark equivalent of itertools.combinations(authors, 2)
    #    Do an index-based self-join of posexploded arrays and keep j > i
    left  = grp.select("paper_id", F.posexplode("authors").alias("i", "a"))
    right = grp.select("paper_id", F.posexplode("authors").alias("j", "b"))

    pairs_df = (left.join(right, on="paper_id")
                .where(F.col("j") > F.col("i"))
                .select(
                    F.least(F.col("a"), F.col("b")).alias("source"),
                    F.greatest(F.col("a"), F.col("b")).alias("target")
                ))

    pairs_df = pairs_df.dropDuplicates()
    

def main(authors_df):

    start = time.time()
    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
    status = F

    try:
        start = time.time()
        spark = spark_session()

        coauthor_stats(authors_df)

    except Exception as e:
        logger.error(f"❌ Error storing metadata: {e}", exc_info=True)
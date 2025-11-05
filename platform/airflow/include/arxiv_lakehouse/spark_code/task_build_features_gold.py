import os, json
import time
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    RegexTokenizer, StopWordsRemover, HashingTF, IDF, VectorAssembler, StandardScaler
)
import boto3
from urllib.parse import urlparse
from sparksession import spark_session
from include.init_log import initlog

from include.mysql_log import store_metadata

logger = initlog(__name__)

# -------------------------------
# Read Silver data
# -------------------------------
def read_silver(spark):
    silver = os.getenv("SILVER_PATH", "s3a://deltabucket/silver/arxiv_cleaned")
    df = spark.read.format("delta").option("versionAsOf", 1).load(silver)

    # Drop null abstracts or titles
    df = df.dropna(subset=["title", "abstract"])
    return df


# -------------------------------
# Build text-based features
# -------------------------------
def vectorize(df, stopwords_list=None):
    # Combine text fields for embedding input
    df = df.withColumn(
        "text_str",
        F.concat_ws(" ", F.col("title"), F.col("abstract"), F.coalesce(F.col("categories"), F.lit("")))
    )

    # Tokenize, remove stopwords, TF-IDF vectorization
    tok = RegexTokenizer(inputCol="text_str", outputCol="tokens", pattern="\\W")
    stop = StopWordsRemover(inputCol="tokens", outputCol="clean_tokens")
    if stopwords_list:
        stop = stop.setStopWords(stop.getStopWords() + stopwords_list)

    tf = HashingTF(inputCol="clean_tokens", outputCol="tf", numFeatures=1 << 15)
    idf = IDF(inputCol="tf", outputCol="tfidf")

    # Combine into single vector column for ML or ANN
    assembler = VectorAssembler(inputCols=["tfidf"], outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features")

    pipe = Pipeline(stages=[tok, stop, tf, idf, assembler, scaler])
    model = pipe.fit(df)
    out = model.transform(df)

    # Only keep necessary columns
    out = out.select(
        "paper_id", "title", "abstract", "categories", "version",
        "version_created", "updated", "published", "comments", "features"
    )

    return out, model


# -------------------------------
# Save Delta Gold output
# -------------------------------
def save_gold(df):
    gold = os.getenv("GOLD_PATH", "s3a://deltabucket/gold/arxiv_features")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold)
    )
    logger.info(f"✅ Saved Gold data to {gold}")


# -------------------------------
# Export learned params to MinIO
# -------------------------------
def export_params_to_minio(model, s3a_uri="s3a://deltabucket/models/arxiv_tfidf_params/model_params.json"):
    tfm = next(s for s in model.stages if s.__class__.__name__ == "HashingTF")
    scal = next(s for s in model.stages if s.__class__.__name__ == "StandardScalerModel")

    payload = {
        "num_features": tfm.getNumFeatures(),
        "with_mean": scal.getWithMean(),
        "with_std": scal.getWithStd(),
    }

    u = urlparse(s3a_uri.replace("s3a://", "s3://"))
    bucket = u.netloc
    key = u.path.lstrip("/")
    if not key or key.endswith("/"):
        key = key.rstrip("/") + "/model_params.json"

    s3 = boto3.client(
        "s3",
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"✅ Exported model params to {bucket}/{key}")


# -------------------------------
# Main
# -------------------------------
def main():
    start = time.time()

    s = None
    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
    status = 'F'
    
    try:
        s = spark_session()
        logger.info(">>> Reading Silver data ...")
        df = read_silver(s)

        logger.info(">>> Building TF-IDF features ...")
        out, model = vectorize(df)

        logger.info(">>> Saving Spark model to MinIO ...")
        model.write().overwrite().save("s3a://deltabucket/models/arxiv_tfidf_model")

        logger.info(">>> Exporting params to MinIO ...")
        export_params_to_minio(model)

        logger.info(">>> Saving Gold layer ...")
        save_gold(out)

        record_count = out.count()  # ✅ light check (not full count)
        logger.info("Gold feature build complete.")

        status = 'S'

    except Exception as e:
        logger.error(f"❌ Error in Spark job: {e}", exc_info=True)
    finally:
        if s:
            s.stop()
            logger.info("Spark session stopped.")
        # ✅ Safe metadata logging
        try:
            end = time.time()
            duration = end - start if 'start' in locals() else 0

            store_metadata(
                run_id=run_id,
                stage_name='silver_to_gold',
                record_count=record_count if 'df' in locals() else 0,
                duration=duration if 'duration' in locals() else 0,
                status=status,
                component='spark',
                note='Silver to Gold feature build job'
            )
        except Exception as e:
            logger.error(f"⚠️ Failed to write metadata: {e}", exc_info=True)


if __name__ == "__main__":
    main()

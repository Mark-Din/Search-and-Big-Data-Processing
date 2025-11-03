import os, json, pathlib
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF, VectorAssembler, StandardScaler
import boto3
from urllib.parse import urlparse

zh_stopwords = [
        "的", "了", "在", "是", "我", "有", "和", "就", "不", "人",
        "都", "一", "上", "也", "很", "到", "他", "年", "就是", "而",
        "我們", "這個", "可以", "這些", "自己", "沒有", "這樣", "著",
        "多", "對", "下", "但", "要", "被", "讓", "她", "向", "以",
        "所以", "把", "跟", "之", "其", "又", "在這裡", "這", "能",
        "應該", "則", "然後", "只是", "那", "在那裡", "這種", "因為",
        "這是", "而且", "如何", "誰", "它", "不是", "這裡", "如此",
        "每個", "這一點", "即使", "大", "小", "因此", "可能", "其他",
        "不過", "他們", "最後", "使用", "至於", "此", "其中", "大家",
        "或者", "最", "且", "雖然", "那麼", "這些", "一些", "通過",
        "為什麼", "什麼", "進行", "再", "已經", "不同", "整個", "以及",
        "從", "這樣的", "不能", "他的", "我們的", "自", "這邊", "那邊",
        "對於", "所有", "能夠", "請", "給", "在此", "上面", "以下",
        "儘管", "不需要", "不管", "與此同時", "關於", "有關", "將",
        "沒事", "沒關係", "這邊", "那邊", "有時候", "有時", "為", "可能性"
]

# --- IO ---
def read_silver(spark):
    silver = os.getenv("SILVER_PATH","s3a://deltabucket/silver/wholeCorp_delta")
    return spark.read.format("delta").load(silver)

def save_gold(df):
    gold = os.getenv("GOLD_PATH","s3a://deltabucket/gold/wholeCorp_delta")
    (df.select("統一編號","公司名稱","features")
       .write.format("delta").mode("overwrite").save(gold))

# --- Fit + Transform ---
def vectorize(df, zh_stopwords:list=None):
    def has(col): return (F.col(col).isNotNull() & (F.length(F.col(col)) > 0)).cast("int")

    df = (df
          .withColumn("has_官網", has("官網"))
          .withColumn("has_電話", has("電話"))
          .withColumn("log_資本額", F.log1p(F.col("資本額")))
          .fillna({"log_資本額": 0})
    )

    df = df.withColumn("text_str", F.coalesce(F.col("類別_全").cast("string"), F.lit("")))

    tok  = RegexTokenizer(inputCol="text_str", outputCol="tok", pattern="\\s+", gaps=True, toLowercase=True)
    stop = StopWordsRemover(inputCol="tok", outputCol="tok_clean")
    if zh_stopwords:
        stop = stop.setStopWords(stop.getStopWords() + zh_stopwords)

    tf   = HashingTF(inputCol="tok_clean", outputCol="tf", numFeatures=1<<15) # numFeatures is for creating dimensions, it means 2**15
    idf  = IDF(inputCol="tf", outputCol="tfidf")

    num_cols = [c for c in ["log_資本額","log_實收資本總額"] if c in df.columns]
    bin_cols = ["has_官網","has_電話"]

    assembler = VectorAssembler(inputCols=["tfidf"] + num_cols + bin_cols, outputCol="features_raw")
    scaler    = StandardScaler(inputCol="features_raw", outputCol="features")  # withMean=False by default

    pipe = Pipeline(stages=[tok, stop, tf, idf, assembler, scaler])
    model = pipe.fit(df)
    out   = model.transform(df)
    return out, model

# --- Export learned params for Python ETL (no Spark needed later) ---
def export_params_to_minio(model, s3a_uri="s3a://deltabucket/models/sparseVector_params/model_params.json"):
    tfm  = next(s for s in model.stages if s.__class__.__name__ == "HashingTF")
    scal = next(s for s in model.stages if s.__class__.__name__ == "StandardScalerModel")

    payload  = {
        "num_features": tfm.getNumFeatures(),
        "with_mean":    scal.getWithMean(),
        "with_std":     scal.getWithStd(),
    }

    print('payload ', payload )
    
    u = urlparse(s3a_uri.replace("s3a://", "s3://"))
    bucket = u.netloc
    key    = u.path.lstrip("/")
    if not key or key.endswith("/"):
        key = key.rstrip("/") + "/model_params.json"  # default file name
    
    s3 = boto3.client(
        "s3",
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    s3.put_object(Bucket=bucket, Key=key,
                  Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                  ContentType="application/json")

def main():
    from sparksession import spark_session
    s = None
    try:
        s = spark_session()
        df = read_silver(s)
        out, model = vectorize(df, zh_stopwords=[])

        # 1) Save the Spark model for Spark-side reuse
        model.write().overwrite().save("s3a://deltabucket/models/sparseVector")

        # 2) Export minimal params for Python-only ETL
        export_params_to_minio(model)

        # 3) Save features to GOLD (optional)
        save_gold(out)
    except Exception as e:
        print(f"Error in spark job: {e}")
    finally:
        if s: s.stop()

if __name__ == '__main__':
    main()
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
import pyspark.pandas as ps
from sparksession import spark_session

# Read Silver data
def read_silver(spark):
    silver = os.getenv("SILVER_PATH","s3a://deltabucket/silver/wholeCorp_delta")
    return spark.read.format("delta").load(silver)

# Basic flags
def vevtorize(df):
    def has(col): return (F.col(col).isNotNull() & (F.length(F.col(col))>0)).cast("int")
    df = (df
          .withColumn("has_官網", has("官網"))
          .withColumn("has_電話", has("電話"))
          .withColumn("log_資本額", F.log1p(F.col("資本額")))
    )
    df = df.fillna({"log_資本額": 0})
    
    # Text
    text_col = F.coalesce(F.col("類別_全"))
    df = df.withColumn("text_all", text_col)
    df = df.withColumn("text_str", F.col("text_all").cast("string"))
    df = df.fillna({"text_str": ""})  # or drop: df = df.dropna(subset=["text_str"])
    
    tok = RegexTokenizer(inputCol="text_str", outputCol="tok",
                         pattern="\\s+", gaps=True, toLowercase=True)
    stop = StopWordsRemover(inputCol="tok", outputCol="tok_clean")
    tf = HashingTF(inputCol="tok_clean", outputCol="tf", numFeatures=1<<18)
    idf = IDF(inputCol="tf", outputCol="tfidf")
    
    # Categorical
    cats = []
    for c in ["縣市名稱", "區域名稱", "上市櫃_基本資料"]:
        if c in df.columns: cats.append(c)
    
    # indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cats]
    # encoders = [OneHotEncoder(inputCols=[f"{c}_idx"], outputCols=[f"{c}_ohe"]) for c in cats]
    
    num_cols = [c for c in ["log_資本額","log_實收資本總額"] if c in df.columns]
    bin_cols = ["has_官網","has_電話"]
    # ohe_cols = [f"{c}_ohe" for c in cats]
    
    assembler = VectorAssembler(inputCols=["tfidf"] + num_cols + bin_cols,
                                outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features")
    
    pipe = Pipeline(stages=[tok, stop, tf, idf]+ [assembler, scaler])
    model = pipe.fit(df)
    out = model.transform(df)
    return out
    
def save_gold(out):
    gold = os.getenv("GOLD_PATH","s3a://deltabucket/gold/wholeCorp_delta")
    (out.select("統一編號","公司名稱","features")
        .write.format("delta").mode("overwrite").save(gold))

def main():
    try:
        s = spark_session()
        df = read_silver(s)
        out = vevtorize(df)
        save_gold(out)
        s.stop()
    except Exception as e:
        print(f"Error in spark job: {e}")
    finally:
        if 's' in locals():
            s.stop()
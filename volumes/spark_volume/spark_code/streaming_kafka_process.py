from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from elasticsearch import Elasticsearch, helpers

# 1. Spark session
def spark_session():
    return (
    SparkSession.builder
    .appName("test")
    .master("spark://spark-master:7077")
    .config("spark.executorEnv.LANG", "zh_TW.UTF-8") \
    .config("spark.executorEnv.LC_ALL", "zh_TW.UTF-8") \
    .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
    .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
    )

spark = spark_session()

# # 2. Define schema of CDC event
schema = StructType([
    StructField("統一編號", StringType()),     # company id
    StructField("公司名稱", StringType()),     # company name
    StructField("負責人", StringType()),    # address
    StructField("登記地址", StringType()),    # address
    StructField("資本額", DoubleType()),    # capital amount
    StructField("營業項目及代碼表", StringType()),    # address
    StructField("類別_全", StringType()),    # address
    StructField("縣市名稱", StringType()),    # address
    StructField("區域名稱", StringType()),    # address
    StructField("縣市區域", StringType()),    # address
    StructField("官網", StringType()),    # address
    # StructField("cluster", StringType()),    # address
    # StructField("vector", StringType()),    # address
    StructField("updatedAt", StringType())   # timestamp
])

outer_schema = StructType([
    StructField("schema", StringType()),
    StructField("payload", StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("source", StringType()),
        StructField("op", StringType()),
        StructField("ts_ms", StringType())
    ]))
])

# 3. Read CDC events from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "mysql.whole_corp.whole_corp_")
    .option("startingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING) as json_str")
)

print('schema')
kafka_df.printSchema()

# Step 5. Parse 'payload.after'
parsed_df = kafka_df.select(from_json(col("json_str"), outer_schema).alias("data"))

after_df = parsed_df.select("data.payload.after")

final_df = after_df.select(from_json(col("after"), schema).alias("data")).select("data.*")

# Step 6. Filter out nulls
final_df = final_df.filter(col("統一編號").isNotNull())

# Debug preview: show streaming rows in console like .show()
final_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

print("final_df after filtering nulls:")
final_df.printSchema()

# 7. Parse Kafka messages using foreachBatch
def write_to_es(batch_df, batch_id):
    docs = [row.asDict() for row in batch_df.collect()]
    print(f"Batch {batch_id} with {len(docs)} documents {docs}")
    if docs:
        es = Elasticsearch(
            ["http://elasticsearch:9200"],
            http_auth=("elastic", "gAcstb8v-lFCVzCBC__a")
        )
        actions = [
            {
                "_index": "whole_corp",
                "_id": doc["統一編號"],  # Use company id as document ID
                "_source": doc
            }
            for doc in docs
        ]
        print(f'actions:=============={actions}')
        helpers.bulk(es, actions)

# 8. Write to Elasticsearch (streaming sink)
query = (
    final_df.writeStream
    .foreachBatch(write_to_es) # Use foreachBatch instead of direct ES sink for solving jar matching issue
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_es")
    .outputMode("append")
    .start()
)

query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Spark session
def spark_session():
    return (
    SparkSession.builder
    .appName("test")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.jars.packages",
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "mysql:mysql-connector-java:8.0.33")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
    )

spark = spark_session()

# 2. Define schema of CDC event
schema = StructType([
    StructField("統一編號", StringType()),         # company id
    StructField("公司名稱", StringType()),       # company name
    StructField("負責人", StringType()),    # address
    StructField("登記地址", StringType()),    # address
    StructField("資本額", DoubleType()),    # capital amount
    StructField("營業項目及代碼表", StringType()),    # address
    StructField("類別_全", StringType()),    # address
    StructField("縣市名稱", StringType()),    # address
    StructField("區域名稱", StringType()),    # address
    StructField("縣市區域", StringType()),    # address
    StructField("官網", StringType()),    # address
    StructField("cluster", StringType()),    # address
    StructField("vector", StringType()),    # address
    StructField("updatedAt", StringType())   # timestamp
])

# 3. Read CDC events from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "mysql.companies.cdc")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parse Kafka messages
json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Make sure that 統一編號 always exists
json_df = json_df.filter(col("統一編號").isNotNull())

# 6. Write to Elasticsearch (streaming sink)
query = (
    json_df.writeStream
    .format("es")
    .option("checkpointLocation", "/tmp/spark-checkpoints/companies")
    .option("es.nodes", "elasticsearch")
    .option("es.port", "9200")
    .option("es.resource", "companies/_doc")
    .option("es.mapping.id", "統一編號")  # Use company id as document ID
    .option("es.nodes.wan.only", "true")
    .option("es.net.http.auth.user", "elastic") 
    .option("es.net.http.auth.pass", "gAcstb8v-lFCVzCBC__a")
    .outputMode("append")
    .start()
)

query.awaitTermination()
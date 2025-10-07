from pyspark.sql import SparkSession

def spark_session():
    return (
    SparkSession.builder
    .appName("test")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
    )

spark = spark_session()

raw = (spark.read
       .format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "mysql.whole_corp.whole_corp_")
       .option("startingOffsets", "latest")
       .load()
       )
raw = raw.selectExpr("CAST(value AS STRING) as v")

raw.show(5, truncate=False)

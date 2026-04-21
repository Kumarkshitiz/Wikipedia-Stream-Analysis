from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "wiki_raw") \
    .option("startingOffsets", "latest") \
    .load()

df.selectExpr("CAST(value AS STRING) as message") \
  .writeStream \
  .format("console") \
  .outputMode("append") \
  .start() \
  .awaitTermination()
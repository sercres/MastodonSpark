import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import from_json, col, window, to_timestamp, struct, expr
from pyspark.sql import SparkSession

# Initialize Spark Context
conf = SparkConf()
conf.setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Initialize Spark Session for Structured Streaming
app_name = "activity3_4" + "screspia_rafi_34" # Replace with your Spark app name must include the username of the members of the group

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .getOrCreate()

# Define schema for the incoming data
schema = StructType([
    StructField("window", StructType([
        StructField("start", StringType(), True),
        StructField("end", StringType(), True)
    ]), True),
    StructField("mastodon_instance", StringType(), True),
    StructField("count", IntegerType(), True)
])

# Define Kafka parameters
toots_original_topic = "mastodon_toots_original_domain"
toots_retoot_topic = "mastodon_toots_retoot_domain"
kafka_bootstrap_servers = 'Cloudera02:9092' # Replace with your Kafka bootstrap servers

# Create streaming DataFrame by reading original toots data from Kafka
toots_original = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", toots_original_topic) \
    .load()

# Parse the value column as JSON and apply the infered schema. Then select the columns we need.
toots_original_df = toots_original \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss"))

# Create streaming DataFrame by reading retoots data from Kafka
toots_retoot = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", toots_retoot_topic) \
    .load()

# Parse the value column as JSON and apply the infered schema. Then select the columns we need.
toots_retoot_df = toots_retoot \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss"))

# Add watermarks
toots_original_df_with_watermark = toots_original_df \
    .withWatermark("timestamp", "1 minute")

toots_retoot_df_with_watermark = toots_retoot_df \
    .withWatermark("timestamp", "1 minute")

# Join the two streams
toots_join_df = toots_original_df_with_watermark.alias("original") \
    .join(
        toots_retoot_df_with_watermark.alias("retweet"),
        (col("original.window") == col("retweet.window")) &
        (col("original.mastodon_instance") == col("retweet.mastodon_instance")),
        "leftOuter"
    ).select(
        col("original.window"),
        col("original.mastodon_instance"),
        col("original.count").alias("original_count"),
        col("retweet.count").alias("retweet_count"),
        (col("original.timestamp").cast("long")).alias("original_timestamp"),
        (col("retweet.timestamp").cast("long")).alias("retweet_timestamp")
    )

# Apply watermark conditions on the join keys
toots_join_df_with_watermark = toots_join_df \
    .withWatermark("original_timestamp", "1 minute") \
    .withWatermark("retweet_timestamp", "1 minute")

# Final df with the watermark
toots_join_df = toots_join_df_with_watermark \
    .filter(
        (col("original_timestamp") <= col("retweet_timestamp")) &
        (col("retweet_timestamp") - col("original_timestamp") <= expr("interval 1 minute"))
    ) \
    .select(
        col("window"),
        col("mastodon_instance"),
        col("original_count"),
        col("retweet_count")
    )


try:
    # Start running the query that prints the running counts to the console
    query = toots_join_df\
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
    sc.stop()
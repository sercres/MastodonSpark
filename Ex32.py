import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql import SparkSession

conf = SparkConf()
conf.setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Initialize Spark Session for Structured Streaming
app_name = "activity3_2" +  "screspia_rafi_32" # Replace with your Spark app name must include the username of the members of the group

spark = SparkSession \
    .builder \
    .appName(app_name) \
    .getOrCreate()

# Define Kafka parameters
kafka_topic = 'mastodon_toots'
kafka_bootstrap_servers = 'Cloudera02:9092'  # Replace with your Kafka bootstrap servers

# Read a small batch of data from Kafka for schema inference!
batch_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Infer schema
schema = spark.read.json(batch_df.selectExpr("CAST(value AS STRING)").rdd.map(lambda x: x[0])).schema

# Create streaming DataFrame by reading data from Kafka
toots = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets",  "latest") \
    .load()

# Parse the value column as JSON and apply the infered schema. Then select the columns we need.
toots_df = toots\
    .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))\
    .filter(~col("parsed_value.reblog").isNotNull())\
    .select(col("parsed_value.language").alias("language"))\
    .groupBy("language")\
    .agg(count("*").alias("count"))\
    .orderBy(col("count").desc())
    

try:
    # Open stream to console (you need to execute it in a terminal to see the output)
    query = toots_df \
            .writeStream \
            .outputMode("complete") \
            .format("console")\
            .trigger(processingTime="10 seconds")\
            .start()

    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()
    spark.stop()
    sc.stop()
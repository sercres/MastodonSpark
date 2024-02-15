import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# Initialize SparkContext and StreamingContext with a 1-second batch interval
app_name = "tootsEx25_screspia_raufi"  # Name of your application

# Create the SparkContext
try:
    sc = SparkContext("local[2]", appName=app_name)
except ValueError:
    sc.stop()
    sc = SparkContext("local[2]", appName=app_name)

sc.setLogLevel("ERROR")

batch_interval = 5  # Batch interval in seconds
ssc = StreamingContext(sc, batch_interval)
ssc.checkpoint("checkpoint")  # Necessary for updateStateByKey operation

# Define Kafka parameters
kafka_server = 'Cloudera02:9092'  # Kafka server address
kafka_topic = 'mastodon_toots'   # Kafka topic
kafka_group = 'crespi_rafi'   # Kafka consumer group, first surname of each member of the group separated by an underscore.

kafkaParams = {
    "metadata.broker.list": kafka_server,
    "group.id": kafka_group
} 

# Create a DStream that connects to Kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], kafkaParams)

# Function to extract the information
def extract_info(toot):
    user = toot.get('account', {}).get('username', '')
    followers_count = toot.get('account', {}).get('followers_count', 0)
    content_length = len(toot.get('content', ''))
    language = toot.get('language', 'unknown')
    return (language, (followers_count, content_length, user))

# Function to calculate the average length
def process_toots(rdd):
    toots_by_lang = rdd.groupByKey()

    # Calculating the average length
    lang_stats = toots_by_lang.mapValues(lambda values: (
        len(values),
        sum([v[1] for v in values]) / len(values) if len(values) > 0 else 0,
        max(values, key=lambda x: x[0]) if values else ('', 0)
    ))

    # Sorting and limiting
    sorted_lang_stats = lang_stats.takeOrdered(10, key=lambda x: -x[1][0])

    # Printing the table
    print("lang\t| num_toots\t| avg_len_content\t| user\t| followers")
    for lang, stats in sorted_lang_stats:
        print("{}\t| {}\t\t| {:.2f}\t\t\t| {}\t| {}".format(
        lang, stats[0], stats[1], stats[2][2], stats[2][1]
        ))

# Final processing
toot_info = kafkaStream\
    .map(lambda x: json.loads(x[1]))\
    .filter(lambda toot: toot.get('reblog') is None)\
    .map(extract_info)

# Printing the toots result
toot_info.foreachRDD(process_toots)

# Start the computation
try:
    ssc.start()
    ssc.awaitTermination()
except KeyboardInterrupt:
    ssc.stop()
    sc.stop()
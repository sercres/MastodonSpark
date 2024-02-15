#START <FILL IN>
# Create a consumer that subscribes to the Kafka topic and digest the toots
from tokenize import group
from kafka import KafkaConsumer
import json
from bs4 import BeautifulSoup

# Extract the text from the HTML content
def extract_text_from_html(html_content):
    soup = BeautifulSoup(html_content)
    text = soup.get_text(separator=' ', strip=True)
    return text

# Kafka Configuration
kafka_server = 'Cloudera02:9092'  # Kafka server address
kafka_topic = 'mastodon_toots'   # Kafka topic
kafka_group = 'crespi_rafi'   # Kafka consumer group, first surname of each member of the group separated by an underscore.

# Create a Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_server,
    group_id=kafka_group,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Print the toots to the console
try:
    print("Streaming started.")
    for message in consumer:
        # Convert the message to a JSON object
        json_msg = message.value
        toot_text = json_msg.get('content', '')
        text = extract_text_from_html(toot_text)
        print("Text from the toot:", text)
        
except KeyboardInterrupt:
    # Close the consumer
    print("Streaming stopped.")
    consumer.close()

consumer.close()
#END <FILL IN>
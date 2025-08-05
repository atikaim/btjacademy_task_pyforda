from kafka import KafkaConsumer
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json

load_dotenv()

# define variabel yang diperlukan
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")

# membuat consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME,  # mandatory topic name
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,  # Kafka server address
    group_id=KAFKA_CONSUMER_GROUP,  # Kafka consumer group
    auto_offset_reset="earliest",  # Start reading at the earliest message if consumer is new
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

# producer untuk alert temperature tinggi
alert_temp_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

try:
    for message in consumer:
        data = message.value
        print("-" * 40)
        print("Showing message ...", data)
        if data["temperature"] > 95:
                print("ALERT!!! Temperature is high (>95)! Sending to", KAFKA_ALERT_TOPIC)
                alert_temp_producer.send(KAFKA_ALERT_TOPIC, value=data)
                alert_temp_producer.flush()

except Exception as e:
    print(f"Error consuming messages: {e}")

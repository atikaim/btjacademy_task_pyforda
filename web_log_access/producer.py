from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json
import random
import time

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")

# Hardcoded IP dan URL
ip_address = os.getenv("IP_ADDRESS")
url_address = os.getenv("URL_ADDRESS")

print(KAFKA_TOPIC_NAME, ip_address)
# Setup Kafka
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

while True:
    try:
        web_access_data = {
            "time_stamp": datetime.utcnow().replace(microsecond=0).isoformat(),
            "url_address": url_address,
            "ip_address": ip_address
        }

        producer.send(KAFKA_TOPIC_NAME, value=json.dumps(web_access_data).encode("utf-8"))
        producer.flush()
        print("SENT!", web_access_data)
        print("-" * 40)

        # Delay acak 0-5 detik
        time.sleep(random.randint(0, 5))

    except Exception as e:
        print("Error sending message:", e)
        break

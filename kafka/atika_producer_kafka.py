from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json
import random
import time

load_dotenv()

# define variabel yang diperlukan
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")

# define scheduler yang digunakan
scheduler = BlockingScheduler()

# define producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    
def producer_kafka():
    try:
        # dummy data
        for i in range(5):
            log_data = {
                "id" : f"{i}",
                "device_type" : random.choice(["TemperatureSensor","HumiditySensor","MotionDetector","GasSensor","LightSensor"]),
                "location": random.choice(["Jakarta", "Surabaya", "Semarang", "Tangerang", "Surakarta", "Bandung"]),
                "temperature": round(random.uniform(0, 100.0)),
                "humidity": round(random.uniform(30.0, 90.0), 2),
                "battery_level": random.randint(0, 100),
                "timestamp": datetime.utcnow().isoformat()
            }
            producer.send(KAFKA_TOPIC_NAME, value=json.dumps(log_data).encode("utf-8"))
        producer.flush()
        now = datetime.now()
        print("SUCCESS!! Message has just sent to Kafka at", now.strftime("%Y-%m-%d %H:%M:%S"))
        print("-"*20)
    except Exception as e:
        print("Error creating message:",e)

try:
    # menjalankan scheduler 
    scheduler.add_job(producer_kafka, trigger='cron', second=0)
    scheduler.start()
except Exception as e:
    print("Error scheduling message or starting job:",e)
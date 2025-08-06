from kafka import KafkaConsumer
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json
import time
import redis
import psycopg2

load_dotenv()

# config Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")

# config redis
REDIS_HOSTNAME = os.getenv("REDIS_HOSTNAME")
REDIS_PORT = int(os.getenv("REDIS_PORT"))

# config postgres
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_NAME_DB = os.getenv("POSTGRES_NAME_DB")

# membuat consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME,  # mandatory topic name
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,  # Kafka server address
    group_id=KAFKA_CONSUMER_GROUP,  # Kafka consumer group
    auto_offset_reset="earliest",  # Start reading at the earliest message if consumer is new
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

# koneksi ke POSTGRES
try:
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_NAME_DB
    )
    cursor = connection.cursor()
    print("Connection successful!")

except psycopg2.Error as e:
    print(f"ERROR! Failed connecting to the database: {e}")


# create function insert redis to db
def insert_to_blockdb(ip_address):
    now = datetime.now()
    insert_query = """insert into blocked_ip_address (ip_address, blocked_at) 
        values (%s, %s)"""
    insert_data = (ip_address, now)
    
    cursor.execute(insert_query, insert_data)

    connection.commit()
    print("SUCCESS! Blocked IP has been inserted!")


# connect to redis
r = redis.Redis(host=REDIS_HOSTNAME, port=REDIS_PORT, decode_responses=True)

# read message 
try:
    for message in consumer:
        data = message.value
        ip_address = data.get("ip_address")
        time_stamp = datetime.fromisoformat(data.get("time_stamp"))
        print("Showing message ...", data)

        key = f"count:{ip_address}:{time_stamp.strftime('%Y%m%d%H%M')}"
        count = r.incr(key)
        print(f"IP {ip_address} accessed at {time_stamp}, count = {count}")
        r.expire(key, 60)  # expired after 60 detik biar auto reset
        print("Mengecek TTL:",r.ttl(key))
        print("-"*40)

        # Jika count lebih dari 10 insert ke DB
        block_key = block_key = f"blocked:{ip_address}"
        if count > 10 and not r.exists(block_key):
            insert_to_blockdb(ip_address)
            r.set(block_key, 1, ex=3600)
            print(f"BLOCKED: {ip_address}")

except Exception as e: 
    print("ERROR reading message:", e)

# close koneksi ke postgres
cursor.close()
connection.close()


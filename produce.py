import random
from datetime import datetime

from confluent_kafka import Producer
import json
import time

from constants import KAFKA_BROKER, KAFKA_TOPIC

# Kafka configuration
conf = {
    'bootstrap.servers':KAFKA_BROKER,  # Adjust to your broker
    'client.id': 'python-producer'
}

# Create Producer instance
producer = Producer(conf)

# Delivery callback

# Produce messages
def produce_messages(topic):
    for i in range(10):
        log = { "service": "auth-service", "timestamp": str(datetime.now()),
                "status_code": random.choice([200,300,400, 500]),
                "latency_ms": random.randint(100,1000),
                "error": False }
        producer.produce(topic,  value=json.dumps(log))
        print(f"Produced message: {log}")

    producer.flush()

if __name__ == '__main__':
    produce_messages(KAFKA_TOPIC)  # Replace with your Kafka topic name
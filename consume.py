from confluent_kafka import Consumer, KafkaError
import json

from constants import KAFKA_BROKER, THRESHOLD, KAFKA_TOPIC

# Kafka consumer configuration
conf = {
    'bootstrap.servers': KAFKA_BROKER,     # Adjust to your Kafka broker
    'group.id': 'python-consumer-group',       # Consumer group name
    'auto.offset.reset': 'earliest'            # Start from earliest if no offset found
}

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to topic
topic = KAFKA_TOPIC
consumer.subscribe([topic])

def validate(config):
    status_code = config.get("status_code")
    latency_ms = config.get("latency_ms")

    if not (200 <= status_code < 300):
        print(f"Invalid status code: {status_code}")
        return False  # Invalid status code
    if latency_ms > THRESHOLD:
        print(f"Latency exceeds threshold: {latency_ms} ms")
        return False  # Latency exceeds threshold
    return True

try:
    print(f"Consuming from topic: {topic}")
    while True:
        msg = consumer.poll(1.0)  # Timeout in seconds

        if msg is None:
            continue  # No message, continue polling
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                continue
            else:
                # Error
                print(f"Error: {msg.error()}")
                break

        # Proper message
        key = msg.key().decode('utf-8') if msg.key() else None
        value = json.loads(msg.value().decode('utf-8'))
        validate(value)
        print(f"Received message: key={key}, value={value}")

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    # Clean up
    consumer.close()
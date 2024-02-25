from kafka import KafkaConsumer
import json
from datetime import datetime, timezone

# Kafka configuration
bootstrap_servers = ['localhost:9092']  # Adjust if your setup is different
topic_name = 'snowboard_ski_store'

# Initialize Kafka consumer with a robust deserializer
consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: deserialize(x))

def deserialize(message):
    """Safely deserialize Kafka message value to Python object."""
    if message is None or not message.strip():
        return None  # None or empty string
    try:
        return json.loads(message.decode('utf-8'))
    except json.JSONDecodeError:
        print("Error decoding JSON")
        return None

def count_orders():
    order_count = 0
    start_of_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # Poll for messages with a timeout
    while True:
        poll_data = consumer.poll(timeout_ms=1000)  # Adjust the timeout as needed

        if not poll_data:
            break  # Exit the loop if no new messages

        for tp, messages in poll_data.items():
            for message in messages:
                try:
                    order = message.value
                    if order is None:  # Skip non-JSON or empty messages
                        continue

                    order_timestamp = datetime.fromtimestamp(order['timestamp'], tz=timezone.utc)

                    if order_timestamp < start_of_day:
                        continue  # Skip orders before today

                    order_count += 1
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue  # Skip to the next message if an error occurs

    # Print the total number of orders since 00:00 of the current day
    print(f"Total orders since 00:00: {order_count}")



if __name__ == '__main__':
    count_orders()

# consumer_update_stock.py

from kafka import KafkaConsumer
import json

bootstrap_servers = ['localhost:9092']
topic_name = 'snowboard_ski_store'

consumer = KafkaConsumer(topic_name,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         consumer_timeout_ms=10000)

# Initialize stock levels for each product
product_stock = {f'P{num}': 100 for num in range(101, 201)}  # Starting with 100 units for simplicity

def update_stock():
    for message in consumer:
        order = message.value
        product_id = order['product_id']
        quantity_sold = order['quantity']

        # Update the stock level, ensuring it doesn't fall below 0
        product_stock[product_id] = max(product_stock[product_id] - quantity_sold, 0)
        print(f"Updated {product_id} stock: {product_stock[product_id]}")

if __name__ == '__main__':
    update_stock()

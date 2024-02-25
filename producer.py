import random
import json
from kafka import KafkaProducer
import time

# Kafka Producer Configuration
bootstrap_servers = ['localhost:9092']
topic_name = 'snowboard_ski_store'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Product types and attributes for generating names
product_types = ['Snowboard', 'Ski', 'Helmet', 'Goggles']
attributes = ['Beginner', 'Intermediate', 'Advanced']
colors = ['Black', 'White', 'Red', 'Blue', 'Green']

# Generate 100 different products with names, IDs, and varying prices
products = []
for num in range(101, 201):
    product_type = random.choice(product_types)
    attribute = random.choice(attributes)
    color = random.choice(colors)
    name = f"{attribute} {color} {product_type}"
    price = round(random.uniform(50, 500), 2)
    products.append({'product_id': f'P{num}', 'name': name, 'price': price})

# Sort products by price, more expensive items will have lower starting stock
products.sort(key=lambda x: x['price'], reverse=True)

def generate_order():
    product = random.choices(products, weights=[(100 - i // 2) for i in range(len(products))], k=1)[0]
    order = {
        'order_id': random.randint(1000, 9999),
        'product_id': product['product_id'],
        'product_name': product['name'],
        'quantity': random.randint(1, 50),
        'sale_price': product['price'],
        'timestamp': int(time.time())
    }
    return order

def simulate_orders(n=10):
    for _ in range(n):
        order = generate_order()
        producer.send(topic_name, value=order)
        print(f"Sent order: {order}")
        time.sleep(random.uniform(0.1, 1.0))

if __name__ == '__main__':
    simulate_orders(50)

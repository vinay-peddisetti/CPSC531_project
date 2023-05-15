import random
import time
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Define the menu items and their prices
menu = {
    'burger': 5.99,
    'fries': 2.99,
    'salad': 4.99,
    'pizza': 12.99,
    'pasta': 9.99,
    'soda': 1.99,
    'water': 0.99,
    'steak': 18.99,
    'chicken wings': 8.99,
    'ice cream': 3.99,
    'coffee': 2.49,
    'tea': 1.99,
    'fish and chips': 10.99
}

# Define the Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    # Generate a random order with up to 4 items
    order_items = random.sample(list(menu.keys()), random.randint(1, 4))
    
    # Generate a random order ID
    order_id = random.randint(0, 999999)
    
    # Calculate the total price of the order
    order_price = sum([menu[item] for item in order_items])
    
    # Get the current time and format it as a string
    order_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Create a dictionary for the order
    order = {
        'id': order_id,
        'items': order_items,
        'prices': [menu[item] for item in order_items],
        'total_price': order_price,
        'time': order_time
    }
    
    # Convert the order to a JSON string
    order_json = json.dumps(order)
    
    # Send the order to the Kafka topic
    try:
        producer.send('orders', value=order_json.encode('utf-8'))
    except KafkaError as e:
        print(f'Failed to send message to Kafka: {e}')
    
    # Wait for a random interval between 5 to 15 seconds
    time.sleep(random.randint(5, 15))

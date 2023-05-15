from tracemalloc import start
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import MySQLdb._mysql
import time
from datetime import datetime
import json
# Create a Kafka consumer instance
consumer = KafkaConsumer('orders', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',)


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

# Create a dictionary to store the statistics for each restaurant
stats = {
    'restaurant1': {'revenue': 0, 'top_seller': '', 'top_seller_count': 0},
    'restaurant2': {'revenue': 0, 'top_seller': '', 'top_seller_count': 0}
}

# Define a function to update the statistics for each restaurant
def update_stats(restaurant, order):
    # Increment the revenue for the restaurant
    stats[restaurant]['revenue'] += order['total_price']
    
    # Update the top seller if the order has more items than the current top seller
    if len(order['items']) > stats[restaurant]['top_seller_count']:
        stats[restaurant]['top_seller'] = order['items']
        stats[restaurant]['top_seller_count'] = len(order['items'])



# Connect to the SQL server
db = MySQLdb.connect(
    host='localhost',
    user='root',
    password='MailPa$$w0rd',
    database='Resturent'
)

# Create a cursor to execute SQL queries
cursor = db.cursor()

# Check if the orders table exists, if not, create it
cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INT(11) NOT NULL,
        items TEXT NOT NULL,
        prices TEXT NOT NULL,
        total_price FLOAT NOT NULL,
        time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
    );
""")

# Check if the stats table exists, if not, create it
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ind_stats (
        id INT(11) NOT NULL AUTO_INCREMENT,
        restaurant VARCHAR(255) NOT NULL,
        revenue FLOAT NOT NULL,
        top_seller TEXT NOT NULL,
        top_seller_count INT(11) NOT NULL,
        time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
    );
""")

# Check if the combined_stats table exists, if not, create it
cursor.execute("""
    CREATE TABLE IF NOT EXISTS combined_stats (
        id INT(11) NOT NULL AUTO_INCREMENT,
        revenue FLOAT NOT NULL,
        top_seller TEXT NOT NULL,
        top_seller_count INT(11) NOT NULL,
        time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
    );
""")
 # Create the table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS menu_item_count(
        item TEXT, count INTEGER,
        time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
''')

start_time = time.time()
last_write_time = start_time
last_count_write_time= start_time
counts={}

# Process each message from the orders topic
for message in consumer:
    # Convert the message value to a dictionary
    order = json.loads(message.value)
    
    # Determine which restaurant the order belongs to
    if order['id'] < 5000:
        restaurant = 'restaurant1'
    else:
        restaurant = 'restaurant2'
    
    # Update the statistics for the restaurant
    update_stats(restaurant, order)

    # Check if order already exists in the orders table
    cursor.execute('SELECT * FROM orders WHERE id=%s', (order['id'],))
    result = cursor.fetchone()
    if result:
        continue  # ignore duplicate order

    # Insert the order into the orders table
    sql = 'INSERT INTO orders (id, items, prices, total_price, time) VALUES (%s, %s, %s, %s, %s)'
    values = (order['id'], json.dumps(order['items']), json.dumps(order['prices']), order['total_price'], order['time'])
    cursor.execute(sql, values)
    db.commit()

    current_time = time.time()
    # Check if 2 minutes have passed since the last write to the stats table
    if current_time - last_write_time >= 30:
        # Calculate the individual statistics for each restaurant
        restaurant_stats = {
            'restaurant1': {
                'revenue': stats['restaurant1']['revenue'],
                'top_seller': stats['restaurant1']['top_seller'],
                'top_seller_count': stats['restaurant1']['top_seller_count']
            },
            'restaurant2': {
                'revenue': stats['restaurant2']['revenue'],
                'top_seller': stats['restaurant2']['top_seller'],
                'top_seller_count': stats['restaurant2']['top_seller_count']
            }
        }

        # Insert the individual statistics for each restaurant into the stats table
       
        sql = 'INSERT INTO ind_stats (restaurant, revenue, top_seller, top_seller_count) VALUES (%s, %s, %s, %s)'
        values = ('restaurant1', stats['restaurant1']['revenue'], json.dumps(stats['restaurant1']['top_seller']), stats['restaurant1']['top_seller_count'])
        cursor.execute(sql, values)
        db.commit()
        sql = 'INSERT INTO ind_stats (restaurant, revenue, top_seller, top_seller_count) VALUES (%s, %s, %s, %s)'
        values = ('restaurant2', stats['restaurant2']['revenue'], json.dumps(stats['restaurant2']['top_seller']), stats['restaurant2']['top_seller_count'])
        cursor.execute(sql, values)
        db.commit()
        # Calculate the combined statistics for both restaurants
        combined_stats = {
            'revenue': stats['restaurant1']['revenue'] + stats['restaurant2']['revenue'],
            'top_seller': max(stats.items(), key=lambda x: x[1]['top_seller_count'])[1]['top_seller'],
            'top_seller_count': max(stats.items(), key=lambda x: x[1]['top_seller_count'])[1]['top_seller_count']
        }

        # Insert the combined statistics into the stats table
        sql = 'INSERT INTO combined_stats (revenue, top_seller, top_seller_count) VALUES (%s, %s, %s)'
        values = (combined_stats['revenue'], json.dumps(combined_stats['top_seller']), combined_stats['top_seller_count'])
        cursor.execute(sql, values)
        db.commit()
        current_time = time.time()
        
        for item in order['items']:
                if item in counts:
                    counts[item] += 1
                else:
                    counts[item] = 1
        
        # Check if 5 minutes have elapsed
        if current_time - last_count_write_time >= 300:
            for item, count in counts.items():
                order_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute('''INSERT INTO menu_item_count (item, count)
                             VALUES (%s, %s)''', (item, count))
            db.commit()
            counts.clear()
            
            # Reset the start time
            last_count_write_time = current_time
        

        last_write_time = current_time

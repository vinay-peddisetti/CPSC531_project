from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, FloatType, TimestampType
import os
from pyspark.sql.functions import from_json, expr,col
import MySQLdb._mysql

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Create a SparkSession
spark = SparkSession.builder.appName("OrderProcessing").getOrCreate()

# Create a StreamingContext with a batch interval of 1 second
ssc = StreamingContext(spark.sparkContext, 1)

# Define Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "orders",
    "auto.offset.reset": "earliest"
}

# Define the Kafka topics to consume from
topics = ["orders"]

# Create a Kafka direct stream
#kafka_stream = spark.read.format("kafka").options(**kafka_params).load()
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("auto.offset.reset", "earliest") \
    .load()
# Define the schema for the orders
order_schema = StructType([
    StructField("id", IntegerType()),
    StructField("items", ArrayType(StringType())),
    StructField("prices", ArrayType(FloatType())),
    StructField("total_price", FloatType()),
    StructField("time", TimestampType())
])

# Parse the value column as JSON and apply the schema
order_stream = kafka_stream \
    .select(from_json(col("value").cast("string"), order_schema).alias("order")) \
    .select("order.*")

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
def update_stats(orders):
    # Connect to the MySQL server
    db = MySQLdb.connect(
        host='localhost',
        user='root',
        password='MailPassword',
        database='Restuarant'
    )

    # Create a cursor to
    # execute SQL queries
    cursor = db.cursor()

    # Process each order in the RDD
    for order in orders:
        # Determine which restaurant the order belongs to
        if order['id'] < 8000:
            restaurant = 'restaurant1'
        else:
            restaurant = 'restaurant2'
        
        # Update the statistics for the restaurant
        stats[restaurant]['revenue'] += order['total_price']
        
        # Update the top seller if the order has more items than the current top seller
        if len(order['items']) > stats[restaurant]['top_seller_count']:
            stats[restaurant]['top_seller'] = order['items']
            stats[restaurant]['top_seller_count'] = len(order['items'])

        # Check if order already exists in the orders table
        cursor.execute('SELECT * FROM orders WHERE id=%s', (order['id'],))
        result = cursor.fetchone()
        if result:
            continue  # ignore duplicate order

        # Insert the order into the orders table
        cursor.execute(
            'INSERT INTO orders (id, items, prices, total_price, time) VALUES (%s, %s, %s, %s, %s)',
            (order['id'], json.dumps(order['items']), json.dumps(order['prices']), order['total_price'], order['time'])
        )
        db.commit()

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
    values = [
        ('restaurant1', stats['restaurant1']['revenue'], json.dumps(stats['restaurant1']['top_seller']),
         stats['restaurant1']['top_seller_count']),
        ('restaurant2', stats['restaurant2']['revenue'], json.dumps(stats['restaurant2']['top_seller']),
         stats['restaurant2']['top_seller_count'])
    ]
    cursor.executemany(sql, values)
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
        
     # Check if 10 minutes have elapsed
    if current_time - last_count_write_time >= 90:
        for item, count in counts.items():
            order_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''INSERT INTO menu_item_count (item, count)
                             VALUES (%s, %s)''', (item, count))
            db.commit()
            counts.clear()
            
        # Reset the start time
        last_count_write_time = current_time
        

    last_write_time = current_time
    # Close the database connection
    cursor.close()
    db.close()


# Register the UDF with Spark
spark.udf.register("update_stats", update_stats)

# Apply the UDF to the order stream
processed_orders = order_stream.withColumn("result", expr("update_stats(struct(*))"))

# Start the streaming query
query = processed_orders.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epochId: df.select("result").show()) \
    .start()

# Wait for the streaming to finish
query.awaitTermination()


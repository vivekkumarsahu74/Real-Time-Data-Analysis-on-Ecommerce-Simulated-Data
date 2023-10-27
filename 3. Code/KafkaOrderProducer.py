from confluent_kafka import Producer
import json
import random
import time

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'client.id': 'order_producer'
}

# Create Kafka producer instance
producer = Producer(conf)

# Function to generate a random order
def generate_order():
    order_id = random.randint(1000, 9999)
    customer_id = random.randint(1, 100)
    order_date = time.strftime('%Y-%m-%d %H:%M:%S')
    total_amount = round(random.uniform(10, 1000), 2)
    payment_method = random.choice(['Credit Card', 'PayPal', 'Cash'])
    shipping_address = f"Address for Customer {customer_id}"
    order_status = random.choice(['Pending', 'Shipped', 'Delivered'])

    order = {
        'Order ID': order_id,
        'Customer ID': customer_id,
        'Order Date': order_date,
        'Total Amount': total_amount,
        'Payment Method': payment_method,
        'Shipping Address': shipping_address,
        'Order Status': order_status
    }

    return order

# Produce orders to Kafka topic
while True:
    order = generate_order()
    producer.produce('Order', key=str(order['Order ID']), value=json.dumps(order))
    producer.flush()
    print(f"Produced order: {order}")
    time.sleep(2)  # Simulate real-time generation

# Close the producer when done
producer.close()

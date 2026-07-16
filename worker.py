from confluent_kafka import Consumer
import json
import psycopg2
import os
from dotenv import load_dotenv
import time

load_dotenv()

def get_db_connection():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    return conn

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fulfillment-worker-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(consumer_config)
consumer.subscribe(['orders'])

print("Fulfillment worker started. Listening for orders...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        order_event = json.loads(msg.value().decode('utf-8'))
        order_id = order_event['order_id']
        product_name = order_event['product_name']
        quantity = order_event['quantity']

        # Idempotency check: has this order already been completed?
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
        current_status = cursor.fetchone()

        if current_status and current_status[0] == "completed":
            print(f"Order {order_id} already completed — skipping duplicate processing.")
            cursor.close()
            conn.close()
            consumer.commit(msg)
            continue

        cursor.close()
        conn.close()

        print(f"Processing order {order_id}: {quantity} x {product_name}")

        # Simulate fulfillment work (in real life: notify warehouse, send email, etc.)
        time.sleep(1)

        # Update order status to 'completed'
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE orders SET status = %s WHERE id = %s",
            ("completed", order_id)
        )
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Order {order_id} marked as completed.")

        consumer.commit(msg)

except KeyboardInterrupt:
    print("Shutting down worker...")

finally:
    consumer.close()
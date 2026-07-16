from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import os
from dotenv import load_dotenv

import redis
import json

from confluent_kafka import Producer

kafka_producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

load_dotenv()

app = FastAPI()

def get_db_connection():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    return conn

# This defines what data the client must send when placing an order
class OrderRequest(BaseModel):
    idempotency_key: str
    product_name: str
    quantity: int

@app.get("/")
def read_root():
    return {"message": "FORGE is alive"}

@app.get("/test-db")
def test_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orders;")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return {"total_orders": count}

@app.post("/order")
def create_order(order: OrderRequest):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT id, stock FROM products WHERE name = %s FOR UPDATE",
            (order.product_name,)
        )
        product = cursor.fetchone()

        if not product:
            conn.rollback()
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Product not found")

        product_id, available_stock = product

        if available_stock < order.quantity:
            conn.rollback()
            cursor.close()
            conn.close()
            raise HTTPException(status_code=400, detail=f"Insufficient stock. Only {available_stock} left.")

        cursor.execute(
            "UPDATE products SET stock = stock - %s WHERE id = %s",
            (order.quantity, product_id)
        )

        cursor.execute(
            "INSERT INTO orders (idempotency_key, product_name, quantity, status) VALUES (%s, %s, %s, %s) RETURNING id",
            (order.idempotency_key, order.product_name, order.quantity, "pending")
        )
        new_order_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()

        # Invalidate the cache since stock just changed
        redis_client.delete(f"product:{order.product_name}")

        # Publish order event to Kafka
        order_event = {
            "order_id": new_order_id,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "idempotency_key": order.idempotency_key
        }
        kafka_producer.produce(
            'orders',
            key=str(new_order_id),
            value=json.dumps(order_event),
            callback=delivery_report
        )
        kafka_producer.flush()

        return {
            "message": "Order created successfully",
            "order_id": new_order_id,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "status": "pending"
        }

    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        cursor.execute(
            "SELECT id, product_name, quantity, status FROM orders WHERE idempotency_key = %s",
            (order.idempotency_key,)
        )
        existing_order = cursor.fetchone()
        cursor.close()
        conn.close()

        return {
            "message": "Order already exists (duplicate request detected)",
            "order_id": existing_order[0],
            "product_name": existing_order[1],
            "quantity": existing_order[2],
            "status": existing_order[3]
        }
import time

@app.get("/product/{product_name}")
def get_product(product_name: str):
    cache_key = f"product:{product_name}"
    lock_key = f"lock:{product_name}"

    # Step 1: Check Redis cache first
    cached_product = redis_client.get(cache_key)

    if cached_product:
        print("CACHE HIT — serving from Redis")
        return json.loads(cached_product)

    # Step 2: Cache miss — try to acquire a lock before hitting the database
    # nx=True means "only set if this key doesn't already exist"
    lock_acquired = redis_client.set(lock_key, "locked", nx=True, ex=5)

    if not lock_acquired:
        # Someone else is already fetching this data — wait briefly and retry from cache
        print("LOCK HELD BY ANOTHER REQUEST — waiting...")
        for _ in range(20):  # wait up to ~2 seconds
            time.sleep(0.1)
            cached_product = redis_client.get(cache_key)
            if cached_product:
                print("CACHE HIT after waiting")
                return json.loads(cached_product)
        # If still nothing after waiting, fall through and query DB anyway (safety net)

    # Step 3: We hold the lock (or waited and still got nothing) — fetch from PostgreSQL
    print("CACHE MISS — fetching from PostgreSQL")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, name, stock, price FROM products WHERE name = %s",
        (product_name,)
    )
    product = cursor.fetchone()
    cursor.close()
    conn.close()

    if not product:
        redis_client.delete(lock_key)
        raise HTTPException(status_code=404, detail="Product not found")

    product_data = {
        "id": product[0],
        "name": product[1],
        "stock": product[2],
        "price": float(product[3])
    }

    # Step 4: Save to cache and release the lock
    redis_client.setex(cache_key, 60, json.dumps(product_data))
    redis_client.delete(lock_key)

    return product_data
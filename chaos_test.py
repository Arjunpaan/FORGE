import requests
import threading
import time
import random

URL = "http://127.0.0.1:8000/order"

success_count = 0
failure_count = 0
lock = threading.Lock()

def place_order(i):
    global success_count, failure_count
    payload = {
        "idempotency_key": f"chaos-{i}",
        "product_name": "Mouse",
        "quantity": 1
    }
    try:
        response = requests.post(URL, json=payload, timeout=5)
        with lock:
            if response.status_code == 200:
                success_count += 1
            else:
                failure_count += 1
        print(f"Order {i}: {response.status_code}")
    except Exception as e:
        with lock:
            failure_count += 1
        print(f"Order {i}: FAILED - {e}")

threads = []
for i in range(50):
    t = threading.Thread(target=place_order, args=(i,))
    threads.append(t)

print("Starting chaos test with 50 concurrent orders...")
print("!!! Kill the primary database NOW using: docker stop forge_postgres !!!")
time.sleep(3)  # gives you a moment to trigger the crash manually

for t in threads:
    t.start()

for t in threads:
    t.join()

print(f"\n--- RESULTS ---")
print(f"Successful orders: {success_count}")
print(f"Failed orders: {failure_count}")
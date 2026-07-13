import requests
import threading


URL = "http://127.0.0.1:8000/order"

def place_order(i):
    payload = {
        "idempotency_key": f"stock-race-{i}",
        "product_name": "Keyboard",
        "quantity": 1
    }
    response = requests.post(URL, json=payload)
    print(f"Thread {i}: {response.status_code} - {response.json()}")

threads = []
for i in range(10):
    t = threading.Thread(target=place_order, args=(i,))
    threads.append(t)

for t in threads:
    t.start()

for t in threads:
    t.join()
    
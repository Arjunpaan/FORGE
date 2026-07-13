import requests
import threading

URL = "http://127.0.0.1:8000/product/Keyboard"

def fetch_product(i):
    response = requests.get(URL)
    print(f"Request {i}: {response.status_code}")

threads = []
for i in range(15):
    t = threading.Thread(target=fetch_product, args=(i,))
    threads.append(t)

for t in threads:
    t.start()

for t in threads:
    t.join()
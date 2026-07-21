import requests
import time

BASE = "http://127.0.0.1:8000"

# Trigger chaos
r1 = requests.post(f"{BASE}/chaos/trigger")
print("Chaos trigger response:", r1.json())

# Check status immediately
r2 = requests.get(f"{BASE}/chaos/status")
print("Chaos status:", r2.json())

# Try placing an order immediately
r3 = requests.post(f"{BASE}/order", json={
    "idempotency_key": f"chaos-script-test-{time.time()}",
    "product_name": "Wireless Mouse",
    "quantity": 1
})
print("Order response:", r3.status_code, r3.json())

print("\nWaiting 11 seconds for chaos to auto-expire...")
time.sleep(11)

r4 = requests.get(f"{BASE}/chaos/status")
print("Chaos status after wait:", r4.json())

r5 = requests.post(f"{BASE}/order", json={
    "idempotency_key": f"chaos-recovery-test-{time.time()}",
    "product_name": "Wireless Mouse",
    "quantity": 1
})
print("Order response after recovery:", r5.status_code, r5.json())
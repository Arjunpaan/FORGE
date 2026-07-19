
# FORGE — Distributed Order Processing System

**A backend engine that guarantees correct order processing under real failure conditions — database crashes, worker failures, cache stampedes, and concurrent load — without losing, duplicating, or corrupting a single order.**

---

## What is FORGE?

FORGE is not another e-commerce app. It's the invisible backbone that platforms like Amazon, Flipkart, and Zepto rely on internally — the engine that answers one deceptively hard question:

> **"When an order is placed, how do you guarantee it's fulfilled exactly once — even when the database crashes, the network glitches, or a thousand requests arrive at the same millisecond?"**

This project implements and *stress-tests* the real engineering patterns used to solve that problem: idempotency, distributed caching, event-driven decoupling via Kafka, and database replication — each one verified under actual concurrent load and simulated failure, not just written and assumed to work.

---

## Architecture

```mermaid
graph TD
    Client[Client] -->|POST /order| API[FastAPI Order Service]
    API -->|check idempotency + lock stock row| Primary[(PostgreSQL Primary)]
    Primary -.streaming replication.-> Replica[(PostgreSQL Replica)]
    API -->|cache-aside + stampede lock| Redis[(Redis Cache)]
    API -->|publish order event| Kafka[Kafka: orders topic]
    Kafka -->|consume + idempotent processing| Worker[Fulfillment Worker]
    Worker -->|mark order completed| Primary

    style API fill:#2563eb,color:#fff
    style Primary fill:#059669,color:#fff
    style Replica fill:#059669,color:#fff
    style Redis fill:#dc2626,color:#fff
    style Kafka fill:#7c3aed,color:#fff
    style Worker fill:#ea580c,color:#fff
```

**Flow:** A client places an order → the API checks for duplicates and locks the relevant stock row → the order is committed to PostgreSQL → the cache is invalidated → an event is published to Kafka → a separate fulfillment worker picks it up independently and marks it complete. Every arrow above is a point where the system has been deliberately broken and tested for correct recovery.

---

## Verified engineering guarantees

Every claim below has been tested, not just implemented — see `Testing` section for how to reproduce each one.

| Guarantee | How it's enforced | How it was verified |
|---|---|---|
| **No duplicate orders** | Idempotency key + `INSERT ... ON CONFLICT` (DB-level atomicity) | 10 concurrent identical requests → exactly 1 order created |
| **No overselling** | `SELECT ... FOR UPDATE` row locking | 10 concurrent requests for 1 unit of stock → exactly 1 succeeds |
| **No cache stampede** | Redis distributed lock (`SET NX EX`) on cache miss | 15 concurrent requests on cold cache → only 1 reaches PostgreSQL |
| **No lost orders on worker crash** | Kafka manual offset commit (`enable.auto.commit: False`) | Worker killed mid-processing → order reprocessed correctly on restart |
| **No duplicate fulfillment on replay** | Consumer-side idempotency check before processing | Kafka offset manually reset (19 messages replayed) → 0 duplicate side effects |
| **Data survives primary DB crash** | PostgreSQL streaming replication (primary → replica) | Data inserted on primary appears on replica with zero manual steps |
| **Clean failure under chaos** | Transactional integrity + connection error handling | 50 concurrent orders fired, primary DB killed mid-test → orders before crash succeed, orders during/after fail cleanly (no corruption), system self-recovers after DB restart |

---

## Tech stack

| Layer | Technology |
|---|---|
| API | Python, FastAPI, Uvicorn |
| Database | PostgreSQL 16 (primary-replica streaming replication) |
| Caching | Redis 7 |
| Messaging | Apache Kafka + Zookeeper (Confluent images) |
| Infra | Docker, Docker Compose |
| Testing | Custom Python scripts using `threading` + `requests` |

---




## Project structure

\`\`\`
FORGE/
├── main.py                    # FastAPI app — order service, product endpoint
├── worker.py                  # Kafka consumer — fulfillment worker
├── docker-compose.yml         # PostgreSQL (primary+replica), Redis, Kafka, Zookeeper
├── init-replication.sh        # Grants replication permissions to primary on first boot
├── test_race_condition.py     # Verifies idempotency under concurrent load
├── test_stampede.py           # Verifies cache stampede protection
├── chaos_test.py              # Fires concurrent load + simulates DB crash mid-test
├── .env                       # Local secrets (not committed)
└── requirements.txt           # Python dependencies
\`\`\`
---

## How to run this on your own machine

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with WSL2 enabled, on Windows)
- Python 3.10+

### Step 1 — Clone the repo
```bash
git clone https://github.com/Arjunpaan/FORGE.git
cd FORGE
```

### Step 2 — Start the infrastructure (PostgreSQL, Redis, Kafka)
```bash
docker-compose up -d
```
This pulls and starts 5 containers: `forge_postgres` (primary), `forge_postgres_replica`, `forge_redis`, `forge_zookeeper`, `forge_kafka`. First run takes a few minutes (image downloads).

Verify everything is running:
```bash
docker ps
```
You should see all 5 containers with status `Up`.

### Step 3 — Set up the database
```bash
docker exec -it forge_postgres psql -U forge_user -d forge_db
```
Then run:
```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    idempotency_key VARCHAR(255) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    stock INTEGER NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

INSERT INTO products (name, stock, price) VALUES
('Laptop', 5, 50000.00),
('Mouse', 100, 500.00),
('Keyboard', 10, 1500.00);
```
Exit with `\q`.

### Step 4 — Create the Kafka topic
```bash
docker exec -it forge_kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Step 5 — Set up the Python environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

### Step 6 — Create your `.env` file

DATABASE_URL=postgresql://forge_user:forge_password@localhost:5432/forge_db


### Step 7 — Run the API
```bash
uvicorn main:app --reload
```
API is now live at `http://127.0.0.1:8000`. Interactive docs at `http://127.0.0.1:8000/docs`.

### Step 8 — Run the fulfillment worker (separate terminal, same venv)
```bash
python worker.py
```

You now have the full system running: place an order via `/docs` or Postman, and watch it flow through caching, Kafka, and fulfillment.

---

## Testing the guarantees yourself

```bash
# Idempotency + race condition safety (10 concurrent identical orders)
python test_race_condition.py

# Cache stampede protection (15 concurrent cold-cache requests)
python test_stampede.py

# Full chaos test — fires 50 concurrent orders; run `docker stop forge_postgres`
# in a separate terminal within the first ~3 seconds to simulate a live crash
python chaos_test.py
```

To verify replication independently:
```bash
docker exec -it forge_postgres_replica psql -U forge_user -d forge_db
SELECT * FROM products;   # should match the primary, with zero manual sync
```

---

## Known limitations (honest scope)

- **Failover is not automatic.** The replica stays perfectly in sync, but promoting it to primary after a crash is a manual step here — production systems use tools like Patroni or pg_auto_failover for automated failover. This project proves the replication layer works correctly; it doesn't implement failover orchestration.
- **Single-node Kafka, single-shard PostgreSQL.** Horizontal sharding across multiple physical database nodes is out of scope.
- **Load testing is at demonstration scale** (50 concurrent requests), not production scale (thousands+ per second).

---

## Future work

- [ ] Automated failover using Patroni or pg_auto_failover
- [ ] Prometheus + Grafana for real-time metrics (p50/p95/p99 latency, throughput, error rate)
- [ ] React dashboard for live order flow and system health visualization
- [ ] Horizontal Kafka partitioning for parallel order processing across multiple workers
- [ ] Rate limiting (token bucket) at the API layer
- [ ] Multi-region simulation (separate Docker networks acting as independent regions)

---

## Why this project exists

Most student projects demonstrate that something *works*. This one is built to demonstrate that it works *correctly under failure* — which is the actual, harder problem that separates a working demo from production-grade engineering. Every guarantee listed above was deliberately attacked (killed mid-request, hammered with concurrent load, replayed from a reset offset) before being called "done."




# FORGE — Distributed Order Processing System

FORGE is a backend system for processing e-commerce orders reliably under real failure conditions — including database outages, worker crashes, cache stampedes, and concurrent request load.

It implements the core reliability patterns used in production order-processing systems: idempotent request handling, row-level locking for inventory safety, cache-aside caching with stampede protection, event-driven fulfillment via Kafka, and PostgreSQL streaming replication. Each pattern is implemented and then verified under simulated failure — not just written and assumed to work.

An order placed once is fulfilled once, regardless of retries, concurrent load, or infrastructure failure mid-request.

---

## Demo

<!-- PASTE VIDEO LINK / EMBED HERE -->
**[Demo video goes here]**

The video walks through the full system live: placing orders through the dashboard, triggering a simulated outage mid-load, watching requests fail cleanly, and watching the system recover on its own — followed by the Kafka worker crash test and PostgreSQL replication check running in the terminal.

---

## What this project actually demonstrates

Every claim below was deliberately tested by breaking the system on purpose, not just implemented and assumed to work.

| Problem | How FORGE solves it | How it was verified |
|---|---|---|
| Duplicate orders from client retries | Idempotency key + `INSERT ... ON CONFLICT` (atomic at the DB level, not the app level) | 10 identical concurrent requests → exactly 1 order created, 9 return the same result |
| Overselling under concurrent demand | `SELECT ... FOR UPDATE` row locking during stock check | 10 concurrent requests for 1 unit of stock → exactly 1 succeeds, 9 see a clean "out of stock" |
| Cache stampede on a cold key | Redis distributed lock (`SET NX EX`) so only one request repopulates the cache | 15 concurrent requests on an expired cache key → only 1 reaches PostgreSQL, the rest wait and read from cache |
| Losing orders when a worker crashes | Kafka with manual offset commits (`enable.auto.commit: False`) | Worker killed mid-processing → same order redelivered and completed correctly on restart |
| Double-fulfilling an order on message replay | Consumer checks order status before doing any work | Kafka offsets manually reset, 19 messages replayed → 0 duplicate side effects, all skipped cleanly |
| Losing data if the primary database dies | PostgreSQL streaming replication (primary → replica) | Data written to the primary appears on the replica with zero manual steps |
| Silent corruption under real failure | Transactional writes + explicit error handling, no partial state | 50 concurrent orders fired while the primary was killed mid-test → orders before the crash succeeded, everything after failed cleanly (no corruption), and the system recovered on its own once the database came back |

---

## Architecture

```mermaid
graph TD
    subgraph Client
        U[User / Dashboard]
    end

    subgraph API["FastAPI Order Service"]
        A1[POST /order]
        A2[GET /product/:name]
        A3[GET /orders, /stats, /products]
        A4[Chaos endpoints for demo]
    end

    subgraph Cache["Redis"]
        R1[(Cache-aside store)]
        R2[Distributed lock<br/>stampede protection]
    end

    subgraph DB["PostgreSQL"]
        P1[(Primary)]
        P2[(Replica)]
    end

    subgraph Stream["Kafka"]
        K1[orders topic]
    end

    subgraph Worker["Fulfillment Worker"]
        W1[Consumer group]
        W2[Idempotency check]
    end

    U -->|places order| A1
    U -->|browses catalog| A2
    U -->|views dashboard data| A3

    A1 -->|lock stock row, FOR UPDATE| P1
    A1 -->|invalidate on write| R1
    A1 -->|publish order event| K1

    A2 -->|check cache first| R1
    A2 -.cold cache.-> R2
    R2 -.only one winner queries DB.-> P1

    P1 -.WAL streaming.-> P2

    K1 -->|consume, manual commit| W1
    W1 --> W2
    W2 -->|mark completed, or skip if already done| P1

    style API fill:#1e3a8a,color:#ffffff
    style Cache fill:#7f1d1d,color:#ffffff
    style DB fill:#064e3b,color:#ffffff
    style Stream fill:#4c1d95,color:#ffffff
    style Worker fill:#7c2d12,color:#ffffff

    style U fill:#1c1f28,color:#ffffff,stroke:#475569
    style A1 fill:#1c1f28,color:#ffffff,stroke:#475569
    style A2 fill:#1c1f28,color:#ffffff,stroke:#475569
    style A3 fill:#1c1f28,color:#ffffff,stroke:#475569
    style A4 fill:#1c1f28,color:#ffffff,stroke:#475569
    style R1 fill:#1c1f28,color:#ffffff,stroke:#475569
    style R2 fill:#1c1f28,color:#ffffff,stroke:#475569
    style P1 fill:#1c1f28,color:#ffffff,stroke:#475569
    style P2 fill:#1c1f28,color:#ffffff,stroke:#475569
    style K1 fill:#1c1f28,color:#ffffff,stroke:#475569
    style W1 fill:#1c1f28,color:#ffffff,stroke:#475569
    style W2 fill:#1c1f28,color:#ffffff,stroke:#475569


```

**How a request actually flows through this:**

1. A client (or the dashboard) sends `POST /order` with a product, quantity, and a unique idempotency key.
2. The API locks the relevant product row (`SELECT ... FOR UPDATE`) so no other request can read or modify it until this one finishes — this is what prevents overselling.
3. If the idempotency key already exists, PostgreSQL's own `UNIQUE` constraint rejects the insert, and the API returns the original result instead of creating a duplicate.
4. On success, the order is committed, the product's Redis cache entry is invalidated, and an event is published to Kafka's `orders` topic.
5. A separate fulfillment worker — a completely independent process — consumes that event, checks whether the order is already marked complete (in case this message is a replay), and only then does the actual fulfillment work.
6. The worker commits its Kafka offset **manually**, and only after the database update succeeds — so a crash between "processed" and "committed" results in the message being redelivered, not lost.
7. Meanwhile, the primary database continuously streams its write-ahead log to a replica, so the data survives even if the primary goes down.

Every arrow in that diagram is a place where the system was deliberately broken during testing — see the table above for what was actually attacked and what the result was.

---

## Interactive dashboard

Beyond the API, this project includes a live dashboard (served directly by FastAPI, no separate frontend server needed) that turns the guarantees above into something you can actually click through:

- **Store catalog** — 50 real products across 8 categories, live stock levels, one-click ordering
- **Recent orders feed** — updates automatically as orders are placed and fulfilled
- **Trigger Chaos (10s)** — flips the backend into a simulated-outage state; every order placed during that window fails with a clean `503`, and the dashboard shows it recovering on its own once the window ends
- **Simulate Stampede** — fires 15 concurrent order requests at the same product from the browser, so the cache/lock behavior is visible without touching a terminal
- **Activity log** — a real-time console of every action, success or failure, timestamped

This was built specifically so the system's failure-handling isn't just something you have to take on faith from a README — you can trigger it yourself and watch it happen.

---

## Tech stack

| Layer | Technology | Why |
|---|---|---|
| API | Python, FastAPI, Uvicorn | Async-first, fast to iterate on, strong typing via Pydantic |
| Database | PostgreSQL 16, primary-replica streaming replication | ACID guarantees and row-level locking are what make idempotency and stock-safety possible in the first place |
| Caching | Redis 7 | Cache-aside pattern plus atomic `SET NX` for distributed locking |
| Messaging | Apache Kafka + Zookeeper (Confluent images) | Decouples order placement from fulfillment; durable log means nothing is lost if a consumer is down |
| Infra | Docker, Docker Compose | Every service — including a second PostgreSQL node acting as a replica — spins up with one command |
| Frontend | Vanilla HTML/CSS/JS, served by FastAPI | No build step, no separate server; the dashboard is just another route |

---

## Project structure

```
FORGE/
├── main.py                  → FastAPI app: order endpoint, product/catalog endpoints,
│                               stats endpoints, chaos-simulation endpoints, dashboard route
├── worker.py                 → Kafka consumer: fulfillment worker with manual offset
│                               commits and idempotent processing
├── static/
│   └── dashboard.html        → Live dashboard (stats, catalog, orders feed, chaos controls)
├── docker-compose.yml        → PostgreSQL (primary + replica), Redis, Kafka, Zookeeper
├── init-replication.sh       → Grants replication permissions to the primary on first boot
├── test_race_condition.py    → Fires concurrent identical/competing requests to verify
│                               idempotency and stock-locking under load
├── test_stampede.py          → Fires concurrent requests at a cold cache key to verify
│                               stampede protection
├── chaos_test.py             → Fires concurrent load while the primary DB is killed
│                               mid-test, to verify clean failure and recovery
├── requirements.txt          → Python dependencies
└── .env                      → Local secrets (DATABASE_URL) — not committed
```

---

## Running this yourself

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — with WSL2 enabled if you're on Windows
- Python 3.10+

### 1 — Clone and enter the project
```bash
git clone https://github.com/Arjunpaan/FORGE.git
cd FORGE
```

### 2 — Start the infrastructure
```bash
docker-compose up -d
```
This brings up five containers: `forge_postgres` (primary), `forge_postgres_replica`, `forge_redis`, `forge_zookeeper`, and `forge_kafka`. The first run takes a few minutes while images download.

Confirm everything is healthy:
```bash
docker ps
```
All five should show status `Up`.

### 3 — Set up the database schema and seed data
```bash
docker exec -it forge_postgres psql -U forge_user -d forge_db
```
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
    price NUMERIC(10, 2) NOT NULL,
    category VARCHAR(100) DEFAULT 'General',
    image_url TEXT
);
```
Exit with `\q`. (See `main.py` for the full 50-product seed data used in the demo, or insert your own catalog.)

### 4 — Create the Kafka topic
```bash
docker exec -it forge_kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 5 — Set up the Python environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

### 6 — Configure your environment
Create a `.env` file in the project root:
```
DATABASE_URL=postgresql://forge_user:forge_password@localhost:5432/forge_db
```

### 7 — Run the API
```bash
uvicorn main:app --reload
```
The API is now live at `http://127.0.0.1:8000`, interactive docs at `http://127.0.0.1:8000/docs`, and the dashboard at `http://127.0.0.1:8000/dashboard`.

### 8 — Run the fulfillment worker
In a separate terminal (same virtual environment):
```bash
python worker.py
```

You now have the complete system running — place an order through the dashboard or `/docs`, and watch it move through caching, Kafka, and fulfillment in real time.

---

## Reproducing the tests yourself

```bash
# Idempotency + stock-locking under concurrent load (10 identical/competing requests)
python test_race_condition.py

# Cache stampede protection (15 concurrent requests on a cold cache key)
python test_stampede.py

# Full chaos test — fires 50 concurrent orders; run
# `docker stop forge_postgres` in a separate terminal within the first ~3 seconds
# to simulate a live crash mid-load, then `docker start forge_postgres` to recover
python chaos_test.py
```

To verify replication independently:
```bash
docker exec -it forge_postgres_replica psql -U forge_user -d forge_db
SELECT * FROM products;   -- should match the primary, with zero manual syncing
```

Or use the dashboard's built-in **Trigger Chaos** and **Simulate Stampede** buttons to see the same behavior without touching a terminal at all.

---

## Known limitations — the honest scope

- **Failover isn't automated.** The replica stays continuously in sync, but promoting it to primary after a crash is a manual step here. Production setups use something like Patroni or pg_auto_failover to automate that — this project proves the replication layer itself is correct, not a full HA orchestration system.
- **Single-broker Kafka, single-shard PostgreSQL.** Horizontal sharding across multiple physical database nodes, and multi-broker Kafka partitioning, are both out of scope for this project.
- **Load testing is at demonstration scale.** 10–50 concurrent requests is enough to reliably trigger and prove out race conditions; it isn't a substitute for real production-scale load testing (thousands of requests/sec).
- **The chaos-mode toggle used in the dashboard is an application-level fault injection**, not a real infrastructure failure — it exists so the failure/recovery behavior can be demonstrated safely and repeatably in a live demo, in addition to the real `docker stop` crash tests in `chaos_test.py`.

---

## Future work

- [ ] Automated failover using Patroni or pg_auto_failover
- [ ] Prometheus + Grafana for real request-level metrics (p50/p95/p99 latency, throughput, error rate)
- [ ] Horizontal Kafka partitioning so multiple workers can process orders in parallel
- [ ] Token-bucket rate limiting at the API layer
- [ ] Multi-region simulation using separate Docker networks acting as independent regions
- [ ] A lightweight hosted "preview mode" of the dashboard using mock data, for anyone who wants to see the UI without running the full stack locally

---

## Why the scope stops where it does

Kafka and PostgreSQL replication genuinely can't be hosted meaningfully on free-tier platforms — they're memory- and state-heavy in a way that serverless/static hosting isn't built for. Rather than fake a "live" version that quietly drops the distributed-systems parts that are the actual point of this project, the demo video shows the complete system — Kafka, replication, chaos testing, and all — running exactly as designed.

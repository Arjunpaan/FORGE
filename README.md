# FORGE

**A distributed order-processing engine built to stay correct when things break.**

FORGE processes e-commerce orders under real failure conditions — database outages, worker crashes, cache stampedes, and concurrent request storms — and guarantees that an order placed once is fulfilled exactly once, regardless of retries, load, or infrastructure failure mid-request.

It was built as an end-to-end distributed systems project spanning four concerns that are usually solved separately but have to work together in any real order-processing backend: **concurrency-safe transaction handling** (row-level locking, atomic idempotency at the database layer), **caching under load** (cache-aside with distributed-lock stampede protection), **event-driven decoupling** (Kafka producer/consumer with manual offset management), and **data durability** (PostgreSQL streaming replication). Every guarantee below was verified by deliberately attacking it — killing processes mid-request, replaying messages, hammering endpoints with concurrent load — not just implemented and assumed to work.

---

## Table of Contents

- [Why This Project Exists](#why-this-project-exists)
- [What FORGE Does](#what-forge-does)
- [Architecture](#architecture)
- [Key Engineering Decisions](#key-engineering-decisions)
- [Results](#results)
- [Interactive Dashboard](#interactive-dashboard)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup — Running FORGE Locally](#setup--running-forge-locally)
- [Reproducing the Tests Yourself](#reproducing-the-tests-yourself)
- [Limitations](#limitations)
- [Future Work](#future-work)

---

## Why This Project Exists

Placing an order sounds like a simple write to a database. It isn't, the moment more than one thing can happen at once.

Two customers can hit "buy" on the last unit of a product in the same millisecond. A customer's app can retry a request because a response was slow, without the customer ever knowing — and now the same order might get created twice, or charged twice. A background worker fulfilling an order can crash halfway through its job. A cache can go cold at the exact moment a thousand people load the same product page. The database itself can simply die.

None of these are exotic scenarios — they are the normal operating conditions of any system with real concurrent traffic, and they are exactly the class of bug that doesn't show up in a demo where requests are sent one at a time. A system that "works" under a single test request and a system that is *correct under concurrency and failure* are different systems, even if their code looks similar.

FORGE is a deliberately-scoped implementation of the second kind. It doesn't try to be a full e-commerce platform — there's no payment gateway, no user accounts, no shipping logic. It exists to answer one narrower question properly: **given an order request, how do you guarantee correct, non-duplicated, non-lost processing, even when the infrastructure underneath it is actively failing?** — and then to prove the answer by breaking the system on purpose and watching it hold.

---

## What FORGE Does

**1. Idempotent order creation, enforced at the database layer.** Every order request carries a client-generated idempotency key. Rather than checking "does this key exist?" and then inserting — which has a race condition baked into the two-step logic itself — FORGE attempts the insert directly and lets PostgreSQL's `UNIQUE` constraint reject duplicates atomically. If the insert fails on a conflict, the original order's result is looked up and returned instead. This means the guarantee holds even under genuinely concurrent identical requests, not just sequential ones.

**2. Overselling prevention via row-level locking.** Before decrementing stock, the relevant product row is locked with `SELECT ... FOR UPDATE`, so no other request can read or modify that row until the current transaction finishes. Ten concurrent requests competing for one unit of stock result in exactly one success and nine clean rejections — not nine successes and negative inventory.

**3. Cache-aside caching with stampede protection.** Product lookups check Redis first and fall back to PostgreSQL on a miss. The failure mode this avoids is a **cache stampede**: if a popular key expires and hundreds of requests arrive in the same instant, all of them would otherwise hit the database simultaneously. FORGE acquires a short-lived Redis lock (`SET NX EX`) before querying on a cache miss — the request that gets the lock populates the cache; everyone else waits briefly and reads the now-warm cache instead of independently querying the database.

**4. Event-driven fulfillment via Kafka, decoupled from order placement.** Placing an order and fulfilling it are different operations with different latency and failure profiles — placement should be near-instant; fulfillment can take longer and shouldn't block the customer's response. On a successful order, FORGE publishes an event to a Kafka topic and returns immediately. A separate, independently-running fulfillment worker consumes that topic and does the actual completion work.

**5. Manual offset commits for real at-least-once delivery.** The worker disables Kafka's automatic offset commits and only commits an offset after its database update succeeds. If the worker is killed between processing a message and committing it, Kafka redelivers that message on restart — no order is silently dropped because a worker happened to die at the wrong moment.

**6. Consumer-side idempotency to prevent duplicate fulfillment.** At-least-once delivery means messages can be redelivered — including ones that were already fully processed before a crash. The worker checks an order's current status before doing any fulfillment work; if it's already marked complete, the message is acknowledged and skipped. This was verified directly: manually resetting the consumer group's offset and replaying 19 already-processed messages produced zero duplicate side effects.

**7. PostgreSQL streaming replication for data durability.** A primary database continuously streams its write-ahead log to a replica. Data written to the primary appears on the replica automatically, with no manual sync step — verified by writing to the primary and querying the replica directly.

**8. A chaos-testing harness, not just unit tests.** Beyond testing each guarantee in isolation, `chaos_test.py` fires concurrent load at the running system while the primary database is killed mid-test with `docker stop`. The expected — and observed — result: requests before the crash succeed, requests during and after the crash fail cleanly with no data corruption, and the system recovers on its own once the database is restarted, with no code changes or manual intervention required.

**9. A live, interactive dashboard.** A dashboard (served directly by the FastAPI app, no separate frontend server) presents a real product catalog, lets you place orders with one click, and includes two demo-specific controls: a **chaos trigger** that puts the backend into a simulated-outage state for ten seconds so failure and recovery are visible on screen, and a **stampede simulator** that fires fifteen concurrent requests at one product from the browser. Every action is reflected in a real-time activity log.

---

## Architecture

### Request flow — placing an order

```mermaid
flowchart TD
    A["Client sends POST /order<br/>(product, quantity, idempotency_key)"] --> B["Lock product row<br/>SELECT ... FOR UPDATE"]
    B --> C{Enough stock?}
    C -->|No| D["Rollback, return 400"]
    C -->|Yes| E["Attempt INSERT with idempotency_key"]
    E --> F{Unique constraint<br/>violation?}
    F -->|Yes — duplicate| G["Rollback, look up original order,<br/>return the same result"]
    F -->|No — new order| H["Commit transaction"]
    H --> I["Invalidate Redis cache<br/>for this product"]
    I --> J["Publish event to Kafka<br/>orders topic"]
    J --> K["Return 200: order created"]

    style A fill:#12141a,stroke:#5eead4,color:#fff
    style H fill:#12141a,stroke:#4ade80,color:#fff
    style K fill:#12141a,stroke:#4ade80,color:#fff
    style D fill:#3a1a1a,stroke:#f87171,color:#eee
    style G fill:#12141a,stroke:#fbbf24,color:#eee
```

### Fulfillment — decoupled via Kafka

```mermaid
flowchart LR
    A["orders topic<br/>(Kafka)"] --> B["Worker polls for messages"]
    B --> C{"Order already<br/>marked completed?"}
    C -->|Yes, this is a replay| D["Skip fulfillment,<br/>commit offset anyway"]
    C -->|No, new work| E["Do fulfillment work"]
    E --> F["Update order status<br/>to completed"]
    F --> G["Manually commit<br/>Kafka offset"]

    style A fill:#12141a,stroke:#a78bfa,color:#fff
    style G fill:#12141a,stroke:#4ade80,color:#fff
    style D fill:#12141a,stroke:#fbbf24,color:#eee
```

*If the worker crashes between E and G, the offset was never committed — Kafka redelivers the same message, and the check at C prevents it from being fulfilled twice.*

### Caching — stampede protection on a cold key

```mermaid
flowchart TD
    A["N concurrent requests<br/>for the same product"] --> B{Cache hit?}
    B -->|Yes| C["Serve from Redis<br/>immediately"]
    B -->|No — cold cache| D["Try to acquire Redis lock<br/>SET NX EX"]
    D --> E{Lock acquired?}
    E -->|Yes — one winner| F["Query PostgreSQL"]
    F --> G["Populate cache,<br/>release lock"]
    E -->|No — everyone else| H["Wait briefly,<br/>then read from cache"]
    G --> C
    H --> C

    style A fill:#12141a,stroke:#5eead4,color:#fff
    style F fill:#3a1a1a,stroke:#f87171,color:#eee
    style C fill:#12141a,stroke:#4ade80,color:#fff
```

*Without the lock at D, every one of the N requests would query PostgreSQL simultaneously the instant the cache goes cold. With it, only one does.*

### Infrastructure topology

```mermaid
flowchart TD
    subgraph client["Client"]
        U["Browser / Dashboard"]
    end

    subgraph api["FastAPI Order Service"]
        A1["/order"]
        A2["/product/:name"]
        A3["/orders, /stats, /products"]
        A4["/chaos/* (demo controls)"]
    end

    subgraph cache["Redis"]
        R1["Product cache"]
        R2["Distributed locks"]
    end

    subgraph db["PostgreSQL"]
        P1[("Primary")]
        P2[("Replica")]
    end

    subgraph stream["Kafka"]
        K1["orders topic"]
    end

    subgraph worker["Fulfillment Worker"]
        W1["Consumer + idempotency check"]
    end

    U --> A1
    U --> A2
    U --> A3
    A1 --> P1
    A1 --> R1
    A1 --> K1
    A2 --> R1
    R2 -.-> R1
    K1 --> W1
    W1 --> P1
    P1 -.WAL streaming.-> P2

    style api fill:#1e3a8a,color:#fff,stroke:#93c5fd
    style cache fill:#7f1d1d,color:#fff,stroke:#fca5a5
    style db fill:#064e3b,color:#fff,stroke:#6ee7b7
    style stream fill:#4c1d95,color:#fff,stroke:#c4b5fd
    style worker fill:#7c2d12,color:#fff,stroke:#fdba74
    style client fill:#27272a,color:#fff,stroke:#a1a1aa
```

---

## Key Engineering Decisions

**Insert-then-catch instead of check-then-insert, for idempotency.** The naive approach — query for an existing idempotency key, and if none exists, insert — has a race window between the check and the insert. Two concurrent identical requests can both pass the check before either has inserted, producing two rows for one logical order. FORGE instead attempts the insert directly and relies on PostgreSQL's `UNIQUE` constraint to reject the second one atomically at the database level, which is the only place this can genuinely be made race-free without external locking.

**Row locking scoped to the transaction, not the request.** `SELECT ... FOR UPDATE` holds its lock only until the enclosing transaction commits or rolls back, not for the duration of the HTTP request. This keeps the lock window as short as possible — long enough to make the stock check and decrement atomic, short enough not to serialize unrelated traffic on the same product any longer than necessary.

**A distributed lock for cache repopulation, not just a cache.** A cache alone doesn't prevent a stampede — it prevents *steady-state* redundant queries, but does nothing at the moment a key goes cold under concurrent load. The `SET NX EX` lock specifically targets that transition moment, at the cost of a small amount of added latency for the requests that have to wait.

**Manual Kafka offset commits over the default auto-commit.** Auto-commit acknowledges a message on a timer, independent of whether the consumer's work actually succeeded — which can silently lose messages if a crash happens between the auto-commit and the real completion of work. Committing manually, only after the database write succeeds, means the "did we actually finish this?" question and the "can Kafka forget this message?" question are tied to the same event.

**Idempotency checked on the consumer side too, not just the producer side.** At-least-once delivery is a deliberate trade-off Kafka makes in favor of never silently losing messages — the cost is that messages can be redelivered. Rather than trying to force exactly-once semantics out of the broker, FORGE accepts at-least-once delivery and makes the consumer's *processing* idempotent instead, which is the standard, more robust way to get effectively-once behavior in distributed messaging systems.

**A real primary-replica setup instead of a single database with backups.** Backups protect against data loss after the fact; they don't keep a system available *during* a primary outage, and restoring from one takes real time. Streaming replication was set up specifically so the durability claim ("data survives a primary crash") could be tested directly — write to the primary, kill it, read from the replica — rather than asserted.

**Application-level chaos injection as a *complement* to real infrastructure kills, not a replacement.** The dashboard's "Trigger Chaos" button flips an in-process flag rather than actually stopping a container — this makes the failure/recovery behavior demonstrable safely and repeatably in a live setting (including over screen share) without needing terminal access. The real test of the underlying infrastructure is still `chaos_test.py`, which performs an actual `docker stop` against the primary mid-load.

---

## Results

Every result below reflects an actual run of the corresponding test script, not a theoretical claim.

| Guarantee | Enforced by | Verified by |
|---|---|---|
| No duplicate orders under concurrent retries | Idempotency key + atomic `INSERT ... ON CONFLICT` handling | 10 concurrent identical requests → exactly 1 order created, 9 return the original result |
| No overselling under concurrent demand | `SELECT ... FOR UPDATE` row locking during the stock check | 10 concurrent requests for 1 unit of stock → exactly 1 succeeds, 9 receive a clean "insufficient stock" error |
| No cache stampede on a cold key | Redis distributed lock (`SET NX EX`) around cache repopulation | 15 concurrent requests on an expired cache key → only 1 reaches PostgreSQL; the rest wait and read from cache |
| No lost orders when a worker crashes mid-task | Kafka manual offset commits (`enable.auto.commit: False`) | Worker killed mid-processing → the same message is redelivered and correctly completed on restart |
| No duplicate fulfillment on message replay | Consumer checks order status before doing any work | Consumer group offset manually reset, 19 already-processed messages replayed → 0 duplicate side effects |
| Data survives a primary database crash | PostgreSQL streaming replication (primary → replica) | Data written to the primary appears on the replica automatically, with zero manual sync steps |
| Clean failure — not silent corruption — under real infrastructure failure | Transactional writes, explicit error handling, no partial commits | 50 concurrent orders fired while the primary was killed mid-test → pre-crash orders succeeded, everything during/after failed cleanly, system self-recovered once the database was restarted |

---

## Interactive Dashboard

Rather than asking a reader to take the table above on faith, the project includes a live dashboard — served by FastAPI itself, no separate frontend build or server — that makes the system's behavior directly clickable:

- **Store catalog** — 50 products with real images across 8 categories, live stock counts, one-click ordering
- **Recent orders feed** — updates automatically as orders are placed and move through fulfillment
- **Trigger Chaos (10s)** — puts the backend into a simulated-outage state; every order placed during the window fails with a clean `503`, with a live countdown banner, and the dashboard visibly recovers once the window ends — no page reload needed
- **Simulate Stampede** — fires 15 concurrent order requests at the same product directly from the browser, so the locking behavior described above is visible without opening a terminal
- **Activity log** — a real-time, timestamped console of every action taken and its outcome, success or failure

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| API | Python, FastAPI, Uvicorn | Async-first request handling; Pydantic gives request validation for free |
| Database | PostgreSQL 16 — primary/replica streaming replication | Row-level locking (`FOR UPDATE`) and atomic `UNIQUE` constraints are what make the idempotency and stock-safety guarantees possible at all |
| Caching | Redis 7 | Cache-aside reads, plus atomic `SET NX` used directly as a distributed lock for stampede protection |
| Messaging | Apache Kafka + Zookeeper (Confluent images) | A durable, replayable log is what allows fulfillment to be decoupled from order placement without risking lost work |
| Infra | Docker, Docker Compose | The full topology — including a second PostgreSQL node acting as a live replica — comes up with a single command |
| Frontend | Vanilla HTML/CSS/JS | Served as a normal FastAPI route; no build step or separate server needed for the dashboard |

---

## Project Structure

```
FORGE/
├── main.py                     # FastAPI app: order endpoint, product/catalog endpoints,
│                                #   stats endpoints, chaos-simulation endpoints, dashboard route
├── worker.py                   # Kafka consumer — fulfillment worker with manual offset
│                                #   commits and consumer-side idempotency
├── static/
│   └── dashboard.html          # Live dashboard: catalog, live orders feed, chaos + stampede controls
├── docker-compose.yml          # PostgreSQL (primary + replica), Redis, Kafka, Zookeeper
├── init-replication.sh         # Grants replication permissions to the primary on first boot
├── test_race_condition.py      # Fires concurrent identical/competing requests — verifies
│                                #   idempotency and stock-locking under load
├── test_stampede.py            # Fires concurrent requests at a cold cache key — verifies
│                                #   stampede protection
├── chaos_test.py                # Fires concurrent load while killing the primary DB mid-test —
│                                #   verifies clean failure and self-recovery
├── requirements.txt             # Python dependencies
└── .env                         # Local secrets (DATABASE_URL) — not committed
```

---

## Setup — Running FORGE Locally

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — with WSL2 enabled if on Windows
- Python 3.10+

### 1. Clone the repository

```bash
git clone https://github.com/Arjunpaan/FORGE.git
cd FORGE
```

### 2. Start the infrastructure

```bash
docker-compose up -d
```

This starts five containers: `forge_postgres` (primary), `forge_postgres_replica`, `forge_redis`, `forge_zookeeper`, and `forge_kafka`. The first run takes a few minutes while images download.

```bash
docker ps
```

All five should show status `Up`.

### 3. Set up the database schema

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

### 4. Create the Kafka topic

```bash
docker exec -it forge_kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 5. Set up the Python environment

```bash
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

### 6. Configure your environment

Create a `.env` file in the project root:

```
DATABASE_URL=postgresql://forge_user:forge_password@localhost:5432/forge_db
```

### 7. Run the API

```bash
uvicorn main:app --reload
```

The API is live at `http://127.0.0.1:8000`, interactive docs at `http://127.0.0.1:8000/docs`, and the dashboard at `http://127.0.0.1:8000/dashboard`.

### 8. Run the fulfillment worker

In a separate terminal, same virtual environment:

```bash
python worker.py
```

The complete system is now running — place an order through the dashboard or `/docs`, and watch it move through caching, Kafka, and fulfillment in real time.

---

## Reproducing the Tests Yourself

```bash
# Idempotency + stock-locking under concurrent load
python test_race_condition.py

# Cache stampede protection
python test_stampede.py

# Full chaos test — fires 50 concurrent orders; run
# `docker stop forge_postgres` in a separate terminal within the first ~3 seconds
# to simulate a live crash mid-load, then `docker start forge_postgres` to recover
python chaos_test.py
```

To verify replication independently:

```bash
docker exec -it forge_postgres_replica psql -U forge_user -d forge_db
SELECT * FROM products;   -- matches the primary, with zero manual syncing
```

Or use the dashboard's built-in **Trigger Chaos** and **Simulate Stampede** buttons to see the same behavior without touching a terminal at all.

---

## Limitations

Stated plainly, not glossed over:

- **Failover is not automated.** The replica stays continuously synced with the primary, but promoting it to primary after a real crash is a manual step here. Production systems use something like Patroni or pg_auto_failover to automate that — this project proves the replication layer itself is correct, not a complete HA orchestration system.
- **Single-broker Kafka, single-shard PostgreSQL.** Horizontal sharding across multiple physical database nodes, and multi-broker Kafka partitioning for parallel consumption, are both out of scope here.
- **Load testing is at demonstration scale.** 10–50 concurrent requests is enough to reliably trigger and prove out race conditions and stampedes; it is not a substitute for production-scale load testing at thousands of requests per second.
- **The dashboard's "Trigger Chaos" is application-level fault injection**, not a real infrastructure failure — it exists to make the failure/recovery behavior demonstrable safely and repeatably in a live setting, in addition to (not instead of) the real `docker stop` crash tests in `chaos_test.py`.
- **The live dashboard only reflects the full system when running locally.** Kafka and PostgreSQL replication are memory- and state-heavy in a way free-tier hosting platforms aren't built for, so no persistently-hosted version of the complete stack exists; the recorded demo shows the full system, including Kafka and replication, running as designed.

---

## Future Work

- Automated failover using Patroni or pg_auto_failover
- Prometheus + Grafana for real request-level metrics (p50/p95/p99 latency, throughput, error rate)
- Horizontal Kafka partitioning so multiple workers can process orders in parallel
- Token-bucket rate limiting at the API layer
- Multi-region simulation using separate Docker networks acting as independent regions
- A lightweight hosted preview of the dashboard using mock data, for anyone who wants to see the UI without running the full stack locally

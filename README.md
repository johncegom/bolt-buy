# Bolt Buy

A high-performance, concurrent purchase processing system built in Go. 

> [!IMPORTANT]
> **No AI Disclaimer**: This project is built entirely without the use of AI/machine learning models. It is a pure, hands-on software engineering project designed to study, implement, and benchmark foundational distributed system concepts.

## The Problem Statement

During high-traffic events (such as flash sales, ticket releases, or limited-run product drops), transactional systems face major stability and consistency challenges:
* **Database Saturation (Thundering Herd):** A massive spike of concurrent reads and writes hitting a relational database simultaneously can exhaust connection pools, increase latency, and cause system outages.
* **Inventory Overselling (Race Conditions):** Multiple clients purchasing the last remaining items at the exact same millisecond can bypass simple database checks, leading to negative inventory and oversold stock.
* **Duplicate Purchases & Retries:** Unstable client connections can lead to network retries, causing a user to be charged multiple times or generate duplicate orders for a single action.
* **Cache-Database Drift:** If a transaction fails or rolls back in the database after the inventory cache was already updated, the cache and database drift out of sync.

**Bolt Buy** solves these problems by coordinating an in-memory cache and a relational database using atomic scripts, transactional rollbacks, and idempotency locks to guarantee consistency at high throughput.

## Systems Concepts Taught & Learned

This project demonstrates how to build resilient, consistent, and fast transaction systems by coordinating a fast in-memory cache (**Redis**) and a persistent relational database (**PostgreSQL**). The key system concepts covered include:

### 1. Atomic Cache Operations with Redis Lua Scripting
To prevent race conditions and over-allocation under concurrent load, inventory is decremented directly in the cache. This is achieved using a **Lua script** execution (`luaDeductStock`), which guarantees atomicity by executing the read-and-decrement check as a single uninterrupted transaction inside Redis.

### 2. Distributed Cache-Aside Consistency
* **Pre-Deduction in Cache:** Requests check and decrement Redis stock first to protect the database from query spikes (hotkey protection).
* **Distributed Rollbacks:** If the PostgreSQL database update, duplicate check, or transaction commit fails, a rollback mechanism increments the stock back in Redis and removes the idempotency token to prevent cache-database drift.

### 3. Request Idempotency
To prevent double-purchasing due to network retries, client-side idempotency keys are used:
* **In-Flight Blocking:** A key status of `PROCESSING` is set using `SETNX` with a TTL to prevent concurrent requests with the same key from entering critical sections.
* **Success Caching:** Once committed, the key transitions to `SUCCESS` for 24 hours, returning early without executing database writes.

### 4. Database Transaction Isolation & Constraints
Relies on PostgreSQL transaction limits (`tx.Begin()` / `tx.Commit()`):
* Checks user-specific purchase constraints (e.g., one purchase per user) within the transaction block.
* Modifies PostgreSQL state and records the order atomically, relying on PostgreSQL's ACID guarantees to enforce correct business rules.

### 5. Concurrency & Parallel Benchmarking
The test suite utilizes Go’s built-in testing packages to perform:
* **Concurrent Load Testing:** Simulates dozens of users purchasing the same product concurrently to verify that inventory bounds and order limits are strictly held.
* **Parallel Benchmarks:** Measures the throughput (ops/sec) under parallel lock-free caching conditions.

---

## Infrastructure Setup

To spin up the PostgreSQL and Redis containers, run:

```bash
docker-compose up -d
```

* **PostgreSQL:** Port `6378`
* **Redis:** Port `6379`

## Running Tests & Benchmarks

Run the tests to verify concurrency behavior, idempotency guarantees, and cache drift protections:

```bash
go test -v -run .
```

To run parallel performance benchmarks:

```bash
go test -bench=BenchmarkPurchase_Parallel -benchtime=10s
```

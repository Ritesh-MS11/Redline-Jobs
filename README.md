# Redline Job Queue System

A from-scratch distributed job queue built with **Node.js**, **Express**, and **Redis (ioredis)** — no BullMQ, Bull, or any ready-made queue library.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│  Clients                                                            │
│  POST /jobs  →  GET /jobs/:id  →  POST /jobs/:id/retry             │
└─────────────┬──────────────────────────────────────────────────────┘
              │ HTTP
┌─────────────▼──────────────────────────────────────────────────────┐
│  Express API  (src/api/server.js)                                   │
│  • Enqueue jobs atomically                                          │
│  • Poll job status                                                  │
│  • Manual retry of dead jobs                                        │
│  • Serve dashboard (dashboard/index.html)                           │
└─────────────┬──────────────────────────────────────────────────────┘
              │ ioredis
┌─────────────▼──────────────────────────────────────────────────────┐
│  Redis                                                              │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────────┐ │
│  │ queue:pending  │  │ queue:processing│  │ queue:delayed (ZSET) │ │
│  │  (List / FIFO) │  │  (List)         │  │  scored by run-at ms │ │
│  └────────────────┘  └─────────────────┘  └──────────────────────┘ │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────────┐ │
│  │ queue:dead     │  │  job:<id> hash  │  │ idempotency:<key>    │ │
│  │  (List)        │  │  (all metadata) │  │  (String, TTL 24h)   │ │
│  └────────────────┘  └─────────────────┘  └──────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  worker:<id>:heartbeat (Hash: activeJobs[], lastSeen)          │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────┬──────────────────────────────────────────────────────┘
              │ ioredis (BLMOVE)
┌─────────────▼──────────────────────────────────────────────────────┐
│  Worker Pool  (src/worker/worker.js)  — N instances                 │
│  • BLMOVE pending → processing  (atomic, crash-safe)               │
│  • Handler registry (sendEmail, resizeImage, flaky)                 │
│  • Scheduler loop: promote delayed → pending  (Lua, atomic)         │
│  • Reaper: reclaim stuck processing jobs (heartbeat-based)          │
│  • Graceful SIGTERM/SIGINT shutdown                                  │
└────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Local (no Docker)

```bash
# 1 — Start Redis
redis-server

# 2 — Install dependencies
npm install

# 3 — Start API
npm run start:api

# 4 — Start workers (in separate terminals)
npm run start:worker
npm run start:worker
npm run start:worker

# 5 — Open dashboard
open http://localhost:3000/dashboard
```

### Docker Compose

```bash
docker-compose up --build
# API: http://localhost:3000
# Dashboard: http://localhost:3000/dashboard
```

---

## API Reference

| Method | URL | Body | Description |
|--------|-----|------|-------------|
| `POST` | `/jobs` | `{ type, payload, maxAttempts?, idempotencyKey? }` | Enqueue a job |
| `GET`  | `/jobs/:id` | — | Get job status |
| `GET`  | `/queue/stats` | — | Queue counts |
| `POST` | `/jobs/:id/retry` | — | Re-enqueue a dead job |

### Example

```bash
# Enqueue
curl -X POST http://localhost:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"sendEmail","payload":{"to":"user@example.com","subject":"Hi"}}'

# Poll status
curl http://localhost:3000/jobs/<jobId>

# Queue stats
curl http://localhost:3000/queue/stats
```

---

## Running Tests

```bash
# Requires Redis running on localhost:6379
npm test
```

---

## Chaos Test

```bash
# Spawns 3 workers, enqueues 1000 jobs, kills 1 worker, verifies 0 lost jobs
npm run chaos
```

---

## Benchmark

```bash
npm run benchmark            # enqueue throughput only
npm run benchmark:wait       # + waits for all jobs to complete
# or custom:
node scripts/benchmark.js --jobs 5000 --concurrency 100 --wait
```

---

## Design Decisions & Trade-offs

### 1. BLMOVE for atomic dequeue
`BLMOVE queue:pending queue:processing LEFT RIGHT <timeout>` moves a job ID between lists in a single atomic operation. If the worker crashes after the BLMOVE but before finishing, the job stays in `queue:processing` and the **Reaper** will eventually recover it. This guarantees **at-least-once delivery**.

**Trade-off:** At-least-once means a job *can* be executed more than once (e.g., after a crash + reap). Handlers should be idempotent where possible.

### 2. Exponential backoff via Sorted Set
Delayed jobs live in `queue:delayed` scored by their `run-at` Unix timestamp (ms). The scheduler loop does `ZRANGEBYSCORE queue:delayed -inf <now>` via a Lua script and atomically moves due jobs back to `queue:pending`. This avoids polling individual timers and scales to millions of delayed jobs with O(log N) Redis operations.

### 3. Lua scripts for atomicity
Several operations (promote delayed, move to dead, idempotency check + enqueue) must be atomic to prevent races when multiple workers run simultaneously. Redis executes Lua scripts as a single atomic transaction, eliminating the need for distributed locks.

### 4. Worker heartbeats for the Reaper
Each worker periodically writes `{activeJobs: [...jobIds], lastSeen: <now>}` to `worker:<id>:heartbeat`. The Reaper scans `queue:processing`, checks whether each job is claimed by a live worker (heartbeat fresher than `VISIBILITY_TIMEOUT_MS`), and re-queues or dead-letters abandoned jobs.

**Trade-off:** `VISIBILITY_TIMEOUT_MS` must be longer than the longest legitimate job. Too short → false reaps; too long → slow recovery after crashes.

### 5. Dead-letter queue
After `maxAttempts` failures, a job is moved to `queue:dead` (a list). Dead jobs are never automatically retried — they require an explicit `POST /jobs/:id/retry` call. This prevents poison-pill jobs from consuming retry budget forever.

### 6. Idempotency keys
An optional `idempotencyKey` prevents duplicate enqueues (e.g., when the HTTP client retries a timed-out POST). The key is stored as a Redis `STRING` with a 24-hour TTL. The Lua enqueue script checks for the key's existence before pushing to `queue:pending`.

### 7. Separate blocking connection
`BLMOVE` with a timeout blocks the entire ioredis connection for the duration of the wait. We use a *dedicated* connection for blocking operations and a separate one for all non-blocking work (hash reads/writes, Lua scripts, etc.). This prevents head-of-line blocking.

### 8. Concurrency within a single worker
Each worker runs up to `WORKER_CONCURRENCY` (default 5) jobs simultaneously using async/await without additional threads. This is Node.js's native I/O concurrency model — perfect for I/O-bound jobs (HTTP calls, DB queries). CPU-bound jobs should use `worker_threads` instead.

---

## Folder Structure

```
job-queue-system/
├── src/
│   ├── api/
│   │   ├── server.js       Express app + graceful shutdown
│   │   └── routes.js       POST/GET /jobs, GET /queue/stats
│   ├── worker/
│   │   ├── index.js        Process entry-point
│   │   └── worker.js       Dequeue loop, scheduler, reaper, heartbeat
│   ├── queue/
│   │   ├── keys.js         Redis key constants
│   │   ├── client.js       ioredis factory
│   │   ├── enqueue.js      Enqueue + idempotency Lua script
│   │   ├── dequeue.js      BLMOVE + acknowledge
│   │   ├── retry.js        Backoff, delayed set, dead-letter, promote
│   │   └── stats.js        Queue length counters, recent jobs
│   ├── handlers/
│   │   └── index.js        sendEmail, resizeImage, flaky handler registry
│   └── logger.js           Winston structured JSON logger
├── dashboard/
│   └── index.html          Single-page real-time dashboard
├── tests/
│   └── queue.test.js       Jest integration tests
├── scripts/
│   ├── chaos.js            1000-job crash-recovery test
│   └── benchmark.js        Enqueue + E2E throughput benchmark
├── docker-compose.yml      Redis + API + 3 workers
├── Dockerfile
└── package.json
```


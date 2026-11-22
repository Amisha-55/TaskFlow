# TaskFlow

**Fault-tolerant distributed job scheduling and background execution platform**

TaskFlow is a backend-first job processing system built with FastAPI, PostgreSQL, and Redis Streams. It accepts jobs through a REST API, durably records their publication intent, dispatches them to background workers, and tracks execution outcomes. Its primary engineering focus is correctness under partial failure: concurrent publishers, worker crashes, retries, expired leases, and stale ownership.

> **Project status:** The backend and its core reliability mechanisms are implemented. A React/TypeScript dashboard, end-to-end deployment validation, CI, and performance benchmarks are planned. This README describes the documented implementation, not unverified production guarantees.

## Table of Contents

1. [Features](#features)
2. [Technology Stack](#technology-stack)
3. [System Architecture](#system-architecture)
4. [Job Lifecycle](#job-lifecycle)
5. [Reliability and Delivery Semantics](#reliability-and-delivery-semantics)
6. [Getting Started](#getting-started)
7. [API Reference](#api-reference)
8. [Testing](#testing)
9. [Repository Structure](#repository-structure)
10. [Current Limitations and Roadmap](#current-limitations-and-roadmap)

## Features

- **Asynchronous job execution:** Submit jobs through FastAPI and process them using independent workers.
- **Durable dispatch:** Store each job and its `job.created` outbox event in the same PostgreSQL transaction.
- **Concurrent outbox processing:** Use PostgreSQL `FOR UPDATE SKIP LOCKED` to coordinate relay processes.
- **Redis Streams messaging:** Deliver versioned job messages through a consumer group.
- **Retry handling:** Track attempts and schedule eligible retries using exponential backoff, capped at 30 seconds.
- **Worker recovery:** Use expiring leases, heartbeat renewal, and recovery logic for abandoned work.
- **Stale-worker protection:** Require current claim ownership before a worker can finalize database state.
- **Job inspection:** List, filter, and retrieve jobs with status, timestamps, results, and error information.

## Technology Stack

| Layer | Technologies |
| --- | --- |
| API | Python, FastAPI, Pydantic |
| Persistence | PostgreSQL, SQLAlchemy, Alembic |
| Messaging | Redis Streams, consumer groups |
| Execution | Python worker processes and registered task handlers |
| Infrastructure | Docker, Docker Compose |
| Testing | pytest |
| Planned UI | React, TypeScript, Vite |

## System Architecture

```text
                    Client / REST API consumer
                               |
                               v
                         FastAPI API
                               |
                  Single database transaction
                               |
                               v
                  +-------------------------+
                  |       PostgreSQL        |
                  | jobs + outbox_events    |
                  +-------------------------+
                               |
                               v
                      Outbox relay(s)
                  FOR UPDATE SKIP LOCKED
                               |
                               v
                    Redis Streams queue
                     taskflow:jobs
                               |
                      Consumer group
                     taskflow:workers
                               |
                               v
                       Worker process
                               |
                    Atomic job claim
                               |
                               v
                       Task handler
                        add / echo
                               |
                               v
                    PostgreSQL job state
                  result / error / attempts
```

**PostgreSQL is the source of truth** for job state and ownership. Redis Streams transports work notifications; a queued message alone does not establish that a worker owns a job.

## Job Lifecycle

1. A client submits a job to `POST /api/v1/jobs`.
2. The API validates the request and inserts both the job and a `job.created` outbox event in one database transaction.
3. The outbox relay publishes the event to the Redis stream `taskflow:jobs`.
4. A worker receives the message and atomically attempts to claim the corresponding pending job.
5. The worker executes the registered handler outside the claim transaction.
6. On success, the worker persists the result and marks the job `succeeded`. On failure, it records the error and either schedules a retry or marks the job `failed` when attempts are exhausted.
7. Clients can inspect the current job state through the read API.

A newly submitted job normally begins in `pending`; submission and execution are separate operations.

### Job States

| State | Meaning |
| --- | --- |
| `pending` | Awaiting execution or eligible processing |
| `running` | Claimed by a worker |
| `succeeded` | Completed with a persisted result |
| `failed` | Reached terminal failure |
| `cancelled` | Defined in the model; a complete cancellation workflow is not yet documented |

## Reliability and Delivery Semantics

### Transactional Outbox

A database commit and Redis publication cannot ordinarily be committed atomically together. TaskFlow therefore commits the job and its publication intent in PostgreSQL, then uses a relay to publish the event. If Redis is temporarily unavailable, the outbox record remains available for a later publication attempt.

### Concurrent Relays

Relay processes select eligible outbox rows with `FOR UPDATE SKIP LOCKED`. This helps multiple relays work on different records without blocking on rows already locked by another relay transaction.

### Leases, Heartbeats, and Fencing

Workers use a **60-second lease** and a **15-second heartbeat renewal interval**. Recovery logic can reconsider jobs whose leases expire. Claim tokens prevent a worker holding an obsolete claim from finalizing job state after ownership has changed.

### Delivery Guarantee

TaskFlow follows **at-least-once processing semantics**, not exactly-once execution. Duplicate publication or re-execution can occur around crash and acknowledgement boundaries. Claim-token checks protect database finalization, but they cannot reverse external side effects. Handlers that send emails, charge payments, or call external services should implement idempotency or use external idempotency keys.

## Getting Started

### Prerequisites

- Python and `pip`
- Docker and Docker Compose
- PostgreSQL and Redis services configured through the included Compose file

The documented local setup exposes PostgreSQL on host port `5433` and Redis on `6379`. Check `docker-compose.yml` and `.env.example` for the current configuration before starting.

### 1. Clone the Repository

```bash
git clone https://github.com/Amisha-55/TaskFlow.git
cd TaskFlow
```

### 2. Create a Virtual Environment

**Windows (Command Prompt):**

```bat
python -m venv .venv
.venv\Scripts\activate.bat
pip install -r requirements.txt
```

**macOS / Linux:**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Use `.env.example` as the configuration reference. Copy it to `.env`, then supply the database and Redis settings required by `backend/app/config.py`.

**Windows:**

```bat
copy .env.example .env
```

**macOS / Linux:**

```bash
cp .env.example .env
```

Do not commit `.env` or real credentials.

### 4. Start Infrastructure and Apply Migrations

```bash
docker compose up -d
alembic upgrade head
```

### 5. Start the API

```bash
uvicorn backend.app.main:app --reload
```

API documentation: **http://127.0.0.1:8000/docs**

### 6. Start Background Processing

Run the worker in another terminal with the same environment configuration:

```bash
python -m backend.app.worker.main
```

**Important:** The architecture also requires the outbox relay to run for newly created jobs to reach Redis. The documentation confirms the relay implementation at `backend/app/queue/outbox_relay.py`, but does not establish a standalone relay launch command. Check the current worker entry point and relay module for how the relay is started; do not assume starting the API alone publishes queued work.

## API Reference

| Method | Endpoint | Description |
| --- | --- | --- |
| `POST` | `/api/v1/jobs` | Create a job and its outbox event |
| `GET` | `/api/v1/jobs` | List jobs; supports status filtering and `limit` / `offset` pagination |
| `GET` | `/api/v1/jobs/{job_id}` | Retrieve a job by UUID |

### Example: Create a Job

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"task_type":"add","payload":{"a":12,"b":30},"priority":5,"max_attempts":3}'
```

The documented `add` handler accepts numeric `a` and `b` values. A successful execution of this example produced a result of `42`. Initial API responses return a pending job; poll the detail endpoint to inspect execution status.

### Example: List Jobs

```bash
curl "http://127.0.0.1:8000/api/v1/jobs?limit=20&offset=0"
```

```bash
curl "http://127.0.0.1:8000/api/v1/jobs?status=succeeded"
```

Other registered example handlers include `echo`.

## Testing

Run the backend test suite from the repository root:

```bash
python -m pytest -q
```

To inspect the complete test inventory:

```bash
python -m pytest --collect-only -q
python -m pytest -v
```

**Latest documented checkpoint:** `59 passed, 1 skipped, 1 warning in 6.27s`.

The suite covers request validation, job/outbox persistence, relay and publisher behavior, Redis message contracts, worker execution, retries, lease renewal, ownership checks, and PostgreSQL row-locking behavior. Some integration tests require running infrastructure. The cause of the skipped test and warning should be checked against the current test output.

Passing tests are not evidence of production-scale throughput, exhaustive crash safety, or exactly-once side effects.

## Repository Structure

```text
TaskFlow/
├── backend/
│   ├── app/
│   │   ├── api/           # Job REST endpoints
│   │   ├── models/        # Job and outbox persistence models
│   │   ├── queue/         # Redis messages, publisher, outbox relay
│   │   ├── schemas/       # Request and response validation
│   │   ├── worker/        # Consumer, executor, handlers, lease, recovery
│   │   ├── config.py
│   │   ├── database.py
│   │   └── main.py
│   └── migrations/        # Alembic migrations
├── tests/                 # Backend and integration tests
├── .env.example
├── .gitignore
├── alembic.ini
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Current Limitations and Roadmap

The backend reliability mechanisms are implemented, but the following work is still planned or requires verification:

- [ ] Complete the React/TypeScript dashboard and connect job creation, filtering, and detail views.
- [ ] Validate the full UI-to-API-to-worker execution path.
- [ ] Inspect the skipped test and warning; re-run the full suite.
- [ ] Add CI and improve structured logging, metrics, and health checks.
- [ ] Run controlled worker-crash and Redis-outage recovery experiments.
- [ ] Benchmark enqueue latency, throughput, and multi-worker behavior before making performance claims.
- [ ] Review cancellation, external-handler idempotency, security, and deployment configuration.

---

**Engineering scope:** TaskFlow demonstrates durable job dispatch, asynchronous execution, and recovery-oriented worker coordination. It is a learning and portfolio project; production readiness has not been established.

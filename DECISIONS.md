# Architecture & Debugging Decisions

## 1. Root-Cause Diagnosis

- The original async handler ran 120,000 SHA-256 iterations on the server event loop, delaying health probes and other requests during bursts.
- Each insert and delivery update opened its own SQLite connection. Concurrent writes competed for SQLite's single writer. The handler checked only for the presence of the idempotency header, so retries added writes and deliveries.
- The handler created an HTTP client per request and awaited downstream before responding. The mock waits 250 ms and sometimes returns a first-attempt 503. That exception escaped the handler after the event was already committed.

## 2. Concurrency & Offloading Strategy

The webhook handler redacts content and submits the pending row to a bounded admission queue. The admission writer groups up to 64 submissions into one SQLite insert after a short collection window. Each request waits for SQLite to commit its batch before it can receive HTTP 202. The database itself is the delivery queue and retains work across restarts.

Each server process starts six delivery workers and a prewarmed four-process fingerprint pool. The process pool runs the Python hashing loop outside the event loop and outside its GIL. Request-scoped background tasks would still need persistent queue recovery after a crash. Delivery workers share one HTTPX client with six active and six keep-alive connections, explicit connect, read, write and pool timeouts, and a 10-second wall-clock limit per attempt.

## 3. Idempotency & SQLite Concurrency

SQLite enforces uniqueness on `(tenant_id, idempotency_key)`. A batched `INSERT ... ON CONFLICT DO NOTHING` makes the first accepted payload authoritative. A SHA-256 digest of the validated event ID and original content distinguishes conflicting payloads even when both redact to the same text. Identical requests return 202 and reuse the original row. Different payloads under the same tenant and key return 409. The raw content is discarded after redaction and digest computation. The digest is a comparison value and provides no secrecy for low-entropy inputs.

One SQLite connection per server process serializes statements. WAL, full synchronous commits, and short writes reduce contention while retaining durability. After a 100 ms SQLite busy timeout, recognized `SQLITE_BUSY` and `SQLITE_LOCKED` errors during admission return HTTP 503 with `Retry-After`. Other database errors remain server errors.

Startup migrates the schema transactionally and records SQLite schema version 2. Existing data is preserved. Legacy pending rows are eligible for delivery. Rows without an original idempotency key cannot be matched to new submissions. Rows from an intermediate schema that have a key but lack the original payload digest retain ownership of that key and return 409 on reuse because payload equality cannot be verified.

## 4. Failure Modes & Production Trade-offs

- Workers claim eligible pending rows atomically, using a 30-second lease and a claim attempt number. They try to renew every 10 seconds while processing and update by primary key and claim attempt. Expired claims become eligible again after a restart. A worker that loses its lease stops processing as soon as it detects the loss.
- If downstream is unavailable for 60 seconds, accepted events remain durable and pending. Network failures, timeouts, HTTP 408, 429 and 5xx persist a retry schedule with exponential backoff and jitter capped at 30 seconds. Other HTTP failures mark the row failed.
- After confirmed downstream success, the worker retries the SQLite status write within the same lease without posting again. A process crash or lease loss before the status commit, or a lost response after remote acceptance, can still cause redelivery. Exactly-once remote effects require receiver-side deduplication.
- Four Uvicorn processes would start six delivery workers and four fingerprint subprocesses each. CPU oversubscription, downstream concurrency and SQLite's single-writer capacity would limit scaling. SQLite leases coordinate claims across server processes sharing the same database.

## 5. Verification

The burst test sends all 60 POSTs concurrently over real loopback HTTP using one pre-established connection per sender, with a common start timestamp before dispatch. That measures the request burst including dispatch and response reads, while excluding initial connection setup. An independent thread and HTTP client sample health during the burst. The test waits for both downstream acceptance and committed SQLite delivery status before checking totals. The mock and latency limits are unchanged. The fixture checks both ports before launching and signals only its own servers to shut down gracefully so the fingerprint pool can close.

On Windows with Python 3.15.2, the full suite ran with `.\.venv\Scripts\python.exe -m pytest -v -s -p no:cacheprovider`. All 22 tests passed on three consecutive runs after the delivery concurrency and teardown changes. The final run measured 68.2 ms ingestion p95 and 5.6 ms maximum health latency. Downstream accepted 30 events, all unique, and SQLite had 30 rows, all delivered. The measured latency applies to this machine and the pre-established-connection workload.

import asyncio
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import httpx
import pytest

from app import database, main


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    path = tmp_path / "events.db"
    monkeypatch.setenv("WEBHOOK_DB_PATH", str(path))
    return path


@pytest.fixture
async def db(db_path):
    async with database.open_database() as connection:
        yield connection


@pytest.fixture
async def client(db, monkeypatch):
    writer = database.AdmissionWriter(db)
    monkeypatch.setattr(main.app.state, "admission", writer, raising=False)
    task = asyncio.create_task(writer.run())
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app, raise_app_exceptions=False),
            base_url="http://ingress",
        ) as client:
            yield client
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def submit(client, *, event_id="event", tenant_id="tenant", content="one@example.com", key="key"):
    return await client.post(
        "/webhooks/ingest",
        json={"event_id": event_id, "tenant_id": tenant_id, "content": content},
        headers={"X-Idempotency-Key": key} if key else {},
    )


def committed_rows(path, columns):
    with closing(sqlite3.connect(path)) as connection:
        return connection.execute(f"SELECT {columns} FROM events ORDER BY id").fetchall()


async def test_202_means_pending_row_is_committed_and_redacted(client, db_path):
    assert (await submit(client)).status_code == 202
    assert committed_rows(db_path, "cleaned_text, status, fingerprint") == [
        ("[REDACTED]", "pending", "")
    ]


async def test_missing_key_is_rejected(client, db_path):
    assert (await submit(client, key=None)).status_code == 400
    assert committed_rows(db_path, "id") == []


@pytest.mark.parametrize(
    ("changes", "expected", "rows"),
    [
        ({}, 202, 1),
        ({"content": "two@example.com"}, 409, 1),
        ({"event_id": "other"}, 409, 1),
        ({"tenant_id": "other"}, 202, 2),
    ],
)
async def test_idempotency(client, db_path, changes, expected, rows):
    assert (await submit(client)).status_code == 202
    assert (await submit(client, **changes)).status_code == expected
    assert len(committed_rows(db_path, "id")) == rows


async def test_conflicting_concurrent_writes_across_connections(db, db_path):
    async with database.open_database() as other:
        outcomes = await asyncio.gather(
            database.enqueue_event(db, "event", "tenant", "redacted", "key", "first"),
            database.enqueue_event(other, "event", "tenant", "redacted", "key", "second"),
        )
    assert sorted(outcomes) == [False, True]
    assert len(committed_rows(db_path, "id")) == 1


async def test_busy_writer_returns_retryable_503(client, db_path):
    with closing(sqlite3.connect(db_path)) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        response = await submit(client)
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "1"
    assert committed_rows(db_path, "id") == []
    assert (await submit(client)).status_code == 202


async def test_other_sqlite_error_is_not_reported_as_contention(client, monkeypatch):
    async def enqueue(*args):
        raise sqlite3.OperationalError("no such table")

    monkeypatch.setattr(database, "enqueue_batch", enqueue)
    assert (await submit(client)).status_code == 500


async def test_migration_preserves_original_rows_and_recovers_pending(db_path):
    with closing(sqlite3.connect(db_path)) as original:
        original.executescript("""
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL, cleaned_text TEXT NOT NULL, fingerprint TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO events (event_id, tenant_id, cleaned_text, fingerprint, status)
            VALUES ('old', 'tenant', 'redacted', 'legacy-fingerprint', 'pending'),
                   ('done', 'tenant', 'redacted', 'legacy-fingerprint', 'delivered');
        """)
    before = committed_rows(db_path, "*")
    async with database.open_database() as first:
        row = await database.claim_event(first)
        assert row["event_id"] == "old"
        assert row["fingerprint"] == "legacy-fingerprint"
    async with database.open_database() as second:
        assert await database.claim_event(second) is None
        assert (await second.execute_fetchall("PRAGMA user_version"))[0][0] == database.SCHEMA_VERSION
    assert [row[:7] for row in committed_rows(db_path, "*")] == before


async def test_lease_renewal_restart_reclaim_and_stale_update(db, db_path, monkeypatch):
    now = 100.0
    monkeypatch.setattr(database, "time", SimpleNamespace(time=lambda: now))
    await database.enqueue_event(db, "shared", "tenant", "redacted", "first", "digest")
    await database.enqueue_event(db, "shared", "tenant", "redacted", "second", "digest")
    async with database.open_database() as other:
        first, second = await asyncio.gather(database.claim_event(db), database.claim_event(other))
        assert first["id"] != second["id"]
        assert await database.finish_attempt(other, second, "hash", "delivered")
        now = 120.0
        assert await database.renew_lease(db, first) == 150.0
        now = 131.0
        assert await database.claim_event(other) is None
    now = 151.0
    async with database.open_database() as reopened:
        recovered = await database.claim_event(reopened)
        assert recovered["id"] == first["id"]
        assert recovered["attempts"] == first["attempts"] + 1
        assert not await database.renew_lease(db, first)
        assert not await database.finish_attempt(db, first, "hash", "delivered")
        assert await database.finish_attempt(reopened, recovered, "hash", "delivered")
    assert committed_rows(db_path, "status") == [("delivered",), ("delivered",)]


async def queued_event(db):
    await database.enqueue_event(db, "event", "tenant", "redacted", "key", "digest")
    event = dict(await database.claim_event(db))
    event["fingerprint"] = "hash"
    return event


@pytest.mark.parametrize(
    ("outcome", "status"),
    [(200, "delivered"), (503, "pending"), (429, "pending"),
     (408, "pending"), (400, "failed"), ("timeout", "pending")],
)
async def test_delivery_outcome_is_committed(db, db_path, outcome, status):
    event = await queued_event(db)

    def respond(request):
        if outcome == "timeout":
            raise httpx.ReadTimeout("downstream timeout", request=request)
        return httpx.Response(outcome)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await main.process_claim(db, client, None, event)
    [(saved_status, fingerprint, available_at)] = committed_rows(
        db_path, "status, fingerprint, available_at"
    )
    assert (saved_status, fingerprint) == (status, "hash")
    if status == "pending":
        assert await database.claim_event(db) is None
        assert 0 < available_at - main.time.time() <= 0.5


async def test_failed_delivery_commit_does_not_repeat_successful_post(db, db_path, monkeypatch):
    event = await queued_event(db)
    posts = writes = 0
    finish = main.finish_attempt

    async def fail_first_write(*args):
        nonlocal writes
        writes += 1
        if writes == 1:
            raise sqlite3.OperationalError("database is locked")
        return await finish(*args)

    def respond(request):
        nonlocal posts
        posts += 1
        return httpx.Response(200)

    monkeypatch.setattr(main, "finish_attempt", fail_first_write)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await main.process_claim(db, client, None, event)
    assert (posts, writes) == (1, 2)
    assert committed_rows(db_path, "status") == [("delivered",)]


@pytest.mark.parametrize(("attempt", "limit"), [(1, 0.5), (3, 2), (100, 30)])
def test_retry_delay_is_jittered_and_capped(attempt, limit, monkeypatch):
    bounds = []

    def midpoint(low, high):
        bounds.append((low, high))
        return (low + high) / 2

    monkeypatch.setattr(main.random, "uniform", midpoint)
    assert main.retry_delay(attempt) == 0.75 * limit
    assert bounds == [(limit / 2, limit)]

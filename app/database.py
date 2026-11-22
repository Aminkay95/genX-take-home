import asyncio
import os
import sqlite3
import time
from contextlib import asynccontextmanager

import aiosqlite

DB_PATH = "webhooks.db"
LEASE_SECONDS = 30
SCHEMA_VERSION = 2


def is_contention(error: aiosqlite.OperationalError) -> bool:
    code = getattr(error, "sqlite_errorcode", 0)
    return code & 0xFF in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)


@asynccontextmanager
async def open_database():
    path = os.getenv("WEBHOOK_DB_PATH", DB_PATH)
    async with aiosqlite.connect(path, timeout=0.1, isolation_level=None) as db:
        db.row_factory = aiosqlite.Row
        await db.execute_fetchall("PRAGMA journal_mode=WAL")
        await db.execute_fetchall("PRAGMA synchronous=FULL")
        await db.execute("BEGIN IMMEDIATE")
        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    cleaned_text TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            columns = {row["name"] for row in await db.execute_fetchall("PRAGMA table_info(events)")}
            version = (await db.execute_fetchall("PRAGMA user_version"))[0][0]
            if version > SCHEMA_VERSION:
                raise RuntimeError(f"Unsupported webhook database schema version {version}")
            for name, definition in (
                ("idempotency_key", "TEXT"),
                ("available_at", "REAL NOT NULL DEFAULT 0"),
                ("attempts", "INTEGER NOT NULL DEFAULT 0"),
                ("payload_digest", "TEXT"),
            ):
                if name not in columns:
                    await db.execute(f"ALTER TABLE events ADD COLUMN {name} {definition}")
            await db.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS events_tenant_idempotency
                ON events (tenant_id, idempotency_key)
            """)
            await db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            await db.execute("""
                CREATE INDEX IF NOT EXISTS events_pending
                ON events (available_at, id) WHERE status = 'pending'
            """)
            await db.commit()
        except BaseException:
            await db.rollback()
            raise
        yield db


async def enqueue_event(db, event_id, tenant_id, cleaned_text, idempotency_key, payload_digest):
    [accepted] = await enqueue_batch(db, [(event_id, tenant_id, cleaned_text, idempotency_key, payload_digest)])
    return accepted


async def enqueue_batch(db, submissions):
    placeholders = ", ".join("(?, ?, ?, '', ?, ?)" for _ in submissions)
    await db.execute_fetchall(f"""
        INSERT INTO events (event_id, tenant_id, cleaned_text, fingerprint,
                            idempotency_key, payload_digest)
        VALUES {placeholders}
        ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
    """, tuple(value for submission in submissions for value in submission))

    # Key ownership and the original digest are immutable after insertion.
    keys = [(tenant, key) for _, tenant, _, key, _ in submissions]
    key_placeholders = ", ".join("(?, ?)" for _ in keys)
    rows = await db.execute_fetchall(f"""
        SELECT tenant_id, idempotency_key, payload_digest FROM events
        WHERE (tenant_id, idempotency_key) IN (VALUES {key_placeholders})
    """, tuple(value for key in keys for value in key))
    digests = {(tenant, key): digest for tenant, key, digest in rows}
    return [
        digests[tenant, key] == digest
        for _, tenant, _, key, digest in submissions
    ]


class AdmissionWriter:
    """Group concurrent admissions into short, durable SQLite writes."""

    def __init__(self, db):
        self.db = db
        self.queue = asyncio.Queue(maxsize=256)

    async def submit(self, *submission):
        result = asyncio.get_running_loop().create_future()
        await self.queue.put((submission, result))
        return await result

    async def run(self):
        while True:
            batch = [await self.queue.get()]
            await asyncio.sleep(0.002)
            while len(batch) < 64 and not self.queue.empty():
                batch.append(self.queue.get_nowait())
            submissions, results = zip(*batch, strict=True)
            try:
                accepted = await enqueue_batch(self.db, submissions)
            except Exception as error:
                for result in results:
                    if not result.done():
                        result.set_exception(error)
            else:
                for result, outcome in zip(results, accepted, strict=True):
                    if not result.done():
                        result.set_result(outcome)


async def claim_event(db):
    now = time.time()
    rows = await db.execute_fetchall("""
        UPDATE events
        SET available_at = ?, attempts = attempts + 1
        WHERE id = (
            SELECT id FROM events
            WHERE status = 'pending' AND available_at <= ?
            ORDER BY available_at, id LIMIT 1
        )
        RETURNING id, event_id, cleaned_text, fingerprint, attempts, available_at
    """, (now + LEASE_SECONDS, now))
    return next(iter(rows), None)


async def renew_lease(db, event):
    now = time.time()
    rows = await db.execute_fetchall("""
        UPDATE events SET available_at = ?
        WHERE id = ? AND attempts = ? AND status = 'pending' AND available_at > ?
        RETURNING available_at
    """, (now + LEASE_SECONDS, event["id"], event["attempts"], now))
    return rows[0][0] if rows else None


async def finish_attempt(db, event, fingerprint, status, delay=0):
    now = time.time()
    rows = await db.execute_fetchall("""
        UPDATE events SET fingerprint = ?, status = ?, available_at = ?
        WHERE id = ? AND attempts = ? AND status = 'pending' AND available_at > ?
        RETURNING id
    """, (fingerprint, status, now + delay, event["id"], event["attempts"], now))
    return bool(rows)

import asyncio
import hashlib
import json
import logging
import random
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
from multiprocessing import get_context

import aiosqlite
import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from app.database import (
    AdmissionWriter, LEASE_SECONDS, claim_event, finish_attempt, is_contention,
    open_database, renew_lease,
)
from app.downstream import forward_event
from app.processor import fingerprint_text, scrub_text

logger = logging.getLogger(__name__)
DELIVERY_WORKERS = 6
FINGERPRINT_WORKERS = 4


def retry_delay(attempt):
    ceiling = min(0.5 * 2 ** min(attempt - 1, 6), 30)
    return random.uniform(ceiling / 2, ceiling)


async def maintain_lease(db, event):
    deadline = event["available_at"]
    while True:
        await asyncio.sleep(min(LEASE_SECONDS / 3, max(0, deadline - time.time())))
        while time.time() < deadline:
            try:
                renewed = await renew_lease(db, event)
            except aiosqlite.OperationalError:
                await asyncio.sleep(0.05)
                continue
            if renewed is None:
                return
            deadline = renewed
            break
        else:
            return


async def process_event(db, client, pool, event):
    fingerprint = event["fingerprint"] or await asyncio.get_running_loop().run_in_executor(
        pool, fingerprint_text, event["cleaned_text"]
    )
    status = "delivered"
    delay = 0
    try:
        async with asyncio.timeout(10):
            await forward_event(client, event["event_id"], event["cleaned_text"], fingerprint)
    except httpx.HTTPStatusError as error:
        code = error.response.status_code
        if code in (408, 429) or code >= 500:
            status, delay = "pending", retry_delay(event["attempts"])
        else:
            status = "failed"
            logger.warning("Delivery rejected for row %s with HTTP %s", event["id"], code)
    except (httpx.RequestError, TimeoutError):
        status, delay = "pending", retry_delay(event["attempts"])

    # Keep the outcome in memory while retrying the commit under the same lease.
    while True:
        try:
            await finish_attempt(db, event, fingerprint, status, delay)
            return
        except aiosqlite.OperationalError:
            logger.warning("Could not persist delivery outcome for row %s", event["id"])
            await asyncio.sleep(0.1)


async def process_claim(db, client, pool, event):
    delivery = asyncio.create_task(process_event(db, client, pool, event))
    heartbeat = asyncio.create_task(maintain_lease(db, event))
    try:
        done, _ = await asyncio.wait((delivery, heartbeat), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        delivery.cancel()
        heartbeat.cancel()
        await asyncio.gather(delivery, heartbeat, return_exceptions=True)


async def deliver_events(db, client, pool):
    while True:
        try:
            event = await claim_event(db)
            if event is None:
                await asyncio.sleep(0.05)
                continue

            await process_claim(db, client, pool, event)
        except aiosqlite.OperationalError:
            logger.exception("Could not access the delivery queue")
            await asyncio.sleep(0.25)


@asynccontextmanager
async def lifespan(app: FastAPI):
    timeout = httpx.Timeout(connect=2, read=5, write=5, pool=2)
    limits = httpx.Limits(max_connections=DELIVERY_WORKERS,
                          max_keepalive_connections=DELIVERY_WORKERS)
    async with open_database() as db, httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        admission = AdmissionWriter(db)
        app.state.admission = admission
        pool = ProcessPoolExecutor(max_workers=FINGERPRINT_WORKERS, mp_context=get_context("spawn"))
        workers = []
        try:
            loop = asyncio.get_running_loop()
            await asyncio.gather(*(
                loop.run_in_executor(pool, fingerprint_text, "")
                for _ in range(FINGERPRINT_WORKERS)
            ))
            workers = [asyncio.create_task(admission.run())]
            workers.extend(
                asyncio.create_task(deliver_events(db, client, pool))
                for _ in range(DELIVERY_WORKERS)
            )
            yield
        finally:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            await asyncio.to_thread(pool.shutdown, cancel_futures=True)


app = FastAPI(title="Webhook Ingestion Service", lifespan=lifespan)


class WebhookPayload(BaseModel):
    event_id: str
    tenant_id: str
    content: str


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhooks/ingest", status_code=202)
async def ingest_webhook(
    payload: WebhookPayload,
    request: Request,
    x_idempotency_key: str | None = Header(default=None),
):
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing X-Idempotency-Key header")

    try:
        encoded = json.dumps([payload.event_id, payload.content]).encode("ascii")
        accepted = await request.app.state.admission.submit(
            payload.event_id, payload.tenant_id,
            scrub_text(payload.content), x_idempotency_key, hashlib.sha256(encoded).hexdigest(),
        )
    except aiosqlite.OperationalError as error:
        if not is_contention(error):
            raise
        logger.exception("Could not persist webhook")
        raise HTTPException(status_code=503, detail="Webhook storage busy",
                            headers={"Retry-After": "1"}) from error
    if not accepted:
        raise HTTPException(status_code=409, detail="Idempotency key already used for another payload")
    return {"status": "accepted"}

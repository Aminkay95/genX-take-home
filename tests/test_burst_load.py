import asyncio
import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, closing
from http.client import HTTPConnection
import httpx
import pytest

INGEST_PORT = 8901
DOWNSTREAM_PORT = 9001
INGEST_URL = f"http://127.0.0.1:{INGEST_PORT}"
DOWNSTREAM_URL = f"http://127.0.0.1:{DOWNSTREAM_PORT}"

SERVER_RUNNER = """
import os
import sys
import threading
import time
from pathlib import Path
import uvicorn

if __name__ == "__main__":
    sys.path.insert(0, os.getcwd())
    server = uvicorn.Server(uvicorn.Config(
        sys.argv[1], host="127.0.0.1", port=int(sys.argv[2]), log_level="warning",
    ))

    def watch_parent():
        stop = Path(sys.argv[3])
        while not stop.exists():
            time.sleep(0.05)
        server.should_exit = True

    threading.Thread(target=watch_parent, daemon=True).start()
    server.run()
"""


async def _wait_for_server(url: str, process, timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"Service for {url} exited with code {process.returncode}")
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.1)
    raise RuntimeError(f"Server at {url} did not start within {timeout}s")


@pytest.fixture
async def running_services(tmp_path):
    for port in (INGEST_PORT, DOWNSTREAM_PORT):
        with socket.socket() as probe:
            if sys.platform == "win32":
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError as error:
                pytest.exit(f"Test port {port} is unavailable. No service was started.", returncode=2)

    db_file = tmp_path / "test_webhooks.db"
    runner = tmp_path / "serve.py"
    runner.write_text(SERVER_RUNNER, encoding="utf-8")
    stop = tmp_path / "stop"
    env = os.environ.copy()
    env["WEBHOOK_DB_PATH"] = str(db_file)
    env["DOWNSTREAM_URL"] = f"{DOWNSTREAM_URL}/events"

    processes = []
    try:
        for application, port in (
            ("mock_downstream:app", DOWNSTREAM_PORT), ("app.main:app", INGEST_PORT),
        ):
            processes.append(subprocess.Popen(
                [sys.executable, str(runner), application, str(port), str(stop)],
                env=env,
            ))
        downstream_proc, ingest_proc = processes
        await _wait_for_server(f"{DOWNSTREAM_URL}/stats", downstream_proc)
        await _wait_for_server(f"{INGEST_URL}/health", ingest_proc)
        yield db_file
    finally:
        stop.touch()
        for process in processes:
            try:
                await asyncio.to_thread(process.wait, timeout=10)
            except subprocess.TimeoutExpired:
                process.terminate()
                await asyncio.to_thread(process.wait, timeout=5)


@pytest.mark.asyncio
async def test_burst_traffic_idempotency_and_event_loop_health(running_services):
    db_file = running_services
    unique_count = 30

    # Build 60 requests: 30 unique events, each duplicated with the same idempotency key
    requests_to_send = []
    for i in range(unique_count):
        event_id = f"evt_{i:03d}"
        idem_key = f"idem_{i:03d}"
        payload = {
            "event_id": event_id,
            "tenant_id": f"tenant_{i % 3}",
            "content": f"Customer user{i}@example.com SSN 123-45-6789 submitted ticket #{i}",
        }
        requests_to_send.append((idem_key, payload))
        requests_to_send.append((idem_key, payload))  # Concurrent retry duplicate

    health_latencies_ms: list[float] = []
    ingest_latencies_ms: list[float] = []
    stop_health_probe = threading.Event()
    probe_ready = threading.Event()

    def probe_health():
        with httpx.Client(timeout=5, trust_env=False) as probe_client:
            probe_client.get(f"{INGEST_URL}/health").raise_for_status()
            probe_ready.set()
            while not stop_health_probe.is_set():
                t0 = time.perf_counter()
                resp = probe_client.get(f"{INGEST_URL}/health")
                health_latencies_ms.append((time.perf_counter() - t0) * 1000)
                assert resp.status_code == 200
                stop_health_probe.wait(0.05)

    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(httpx.AsyncClient(timeout=15, trust_env=False))
        (await client.post(f"{DOWNSTREAM_URL}/reset")).raise_for_status()
        loop = asyncio.get_running_loop()
        senders = [
            stack.enter_context(closing(HTTPConnection("127.0.0.1", INGEST_PORT, timeout=15)))
            for _ in requests_to_send
        ]
        pool = stack.enter_context(ThreadPoolExecutor(max_workers=len(requests_to_send)))

        def warm_up(sender):
            sender.request("GET", "/health")
            response = sender.getresponse()
            response.read()
            assert response.status == 200
            sender.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        await asyncio.gather(*(loop.run_in_executor(pool, warm_up, sender) for sender in senders))

        def send_webhook(sender, idem_key, payload, started_at):
            sender.request("POST", "/webhooks/ingest", json.dumps(payload), {
                "X-Idempotency-Key": idem_key, "Content-Type": "application/json",
            })
            response = sender.getresponse()
            response.read()
            ingest_latencies_ms.append((time.perf_counter() - started_at) * 1000)
            return response.status

        probe_task = asyncio.create_task(asyncio.to_thread(probe_health))
        try:
            assert await asyncio.to_thread(probe_ready.wait, 6), "Health probe did not start"
            started_at = time.perf_counter()
            status_codes = await asyncio.gather(
                *(loop.run_in_executor(pool, send_webhook, sender, key, body, started_at)
                  for sender, (key, body) in zip(senders, requests_to_send, strict=True))
            )
        finally:
            stop_health_probe.set()
            await probe_task

        # 1. All 60 webhook requests must succeed quickly (200 or 202)
        ingest_latencies_ms.sort()
        p95_ingest = ingest_latencies_ms[int(len(ingest_latencies_ms) * 0.95) - 1]
        max_health = max(health_latencies_ms)

        # 3. Allow background delivery & retries to settle
        deadline = time.monotonic() + 6.0
        stats = {}
        while time.monotonic() < deadline:
            stats_resp = await client.get(f"{DOWNSTREAM_URL}/stats")
            stats = stats_resp.json()
            with closing(sqlite3.connect(db_file)) as conn:
                rows = conn.execute("SELECT event_id, cleaned_text, status FROM events").fetchall()
            if (stats["unique_events"] == unique_count
                    and len(rows) == unique_count and all(row[2] == "delivered" for row in rows)):
                break
            await asyncio.sleep(0.2)

        print(f"Ingest p95: {p95_ingest:.1f} ms | Max health: {max_health:.1f} ms | "
              f"Downstream accepted: {stats['total_accepted']} ({stats['unique_events']} unique) | "
              f"DB rows: {len(rows)} | Status: {dict(Counter(row[2] for row in rows))}")
        assert all(code in (200, 202) for code in status_codes), f"Failed responses: {status_codes}"
        assert p95_ingest < 250.0, f"Ingest p95 latency too high: {p95_ingest:.1f}ms (target < 250ms)"
        assert max_health < 50.0, f"Event loop starved! Max /health latency: {max_health:.1f}ms (target < 50ms)"
        assert stats["unique_events"] == unique_count, (
            f"Expected {unique_count} delivered events, got {stats['unique_events']}"
        )
        assert stats["total_accepted"] == unique_count, (
            f"Duplicate deliveries detected! Total accepted: {stats['total_accepted']} vs unique: {stats['unique_events']}"
        )

        assert len(rows) == unique_count, f"Expected {unique_count} DB rows, found {len(rows)}"
        for event_id, cleaned_text, status in rows:
            assert "123-45-6789" not in cleaned_text, f"PII leak in {event_id}"
            assert "@example.com" not in cleaned_text, f"Email leak in {event_id}"
            assert status == "delivered", f"Event {event_id} stuck in status '{status}'"

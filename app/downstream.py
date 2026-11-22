import os

import httpx

DOWNSTREAM_URL = "http://127.0.0.1:9001/events"


async def forward_event(client: httpx.AsyncClient, event_id: str, cleaned_text: str, fingerprint: str):
    response = await client.post(
        os.getenv("DOWNSTREAM_URL", DOWNSTREAM_URL),
        json={"event_id": event_id, "text": cleaned_text, "fingerprint": fingerprint},
    )
    response.raise_for_status()

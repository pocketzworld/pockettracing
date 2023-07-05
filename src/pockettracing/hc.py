"""Honeycomb utilities."""
from asyncio import sleep
from typing import Final, NoReturn

from aiohttp import ClientSession

from . import Span, Tracer

HONEYCOMB_URL: Final = "https://api.honeycomb.io"


async def send_to_honeycomb(
    tracer: Tracer, http_client: ClientSession, api_key: str, dataset: str
) -> NoReturn:
    """Continually send traces to Honeycomb, until cancelled."""
    url = f"{HONEYCOMB_URL}/1/batch/{dataset}"
    buffer: list[Span] = []

    def add_to_buffer(spans: list[Span]) -> None:
        if len(buffer) < 1000:
            buffer.extend(spans)

    tracer.receivers.append(add_to_buffer)

    while True:
        while buffer:
            buf = buffer
            buffer = []
            # HC expects a string value under "time"
            payload = dumps([{"data": e, "time": str(e.pop("time"))} for e in buf])  # type: ignore
            resp = await http_client.post(
                url, data=payload, headers={"X-Honeycomb-Team": api_key}
            )
            resp.raise_for_status()
        await sleep(5)

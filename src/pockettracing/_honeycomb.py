from __future__ import annotations

from asyncio import sleep
from typing import NoReturn, Final
from aiohttp import ClientSession
from orjson import dumps

from . import Tracer

HONEYCOMB_URL: Final = "https://api.honeycomb.io"


async def send_to_honeycomb(
    tracer: Tracer, http_client: ClientSession, api_key: str, dataset: str
) -> NoReturn:
    url = f"{HONEYCOMB_URL}/1/batch/{dataset}"
    while True:
        while buf := tracer.drain_buffer():
            # HC expects a string value under "time"
            payload = dumps([{"data": e, "time": str(e.pop("time"))} for e in buf])
            resp = await http_client.post(
                url, data=payload, headers={"X-Honeycomb-Team": api_key}
            )
            resp.raise_for_status()
        await sleep(5)

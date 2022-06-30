"""The beginnings of a simple tracing module."""

from __future__ import annotations

from asyncio import sleep
from contextlib import contextmanager
from contextvars import ContextVar
from itertools import count
from random import random
from secrets import token_hex
from time import perf_counter, time
from typing import Any, Final, Iterator, Mapping, NoReturn

from aiohttp import ClientSession
from attrs import Factory, define
from orjson import dumps

HONEYCOMB_URL: Final = "https://api.honeycomb.io"

_trace_cnt = count().__next__
_span_cnt = count().__next__


@define
class Tracer:
    """A tracer for generating traces to be sent to a tracing service."""

    metadata: dict[str, str] = {}  # Should contain service.name.
    trace_prefix: str = Factory(lambda: token_hex(8))
    buffer_len: int = 1000
    trace_chance: float | None = None
    _buffer: list[dict] = Factory(list)
    _active_trace_and_span: ContextVar[tuple[str, str] | None] = Factory(
        lambda: ContextVar("active", default=None)
    )

    @contextmanager
    def trace(self, name: str, **kwargs: str) -> Iterator[dict[str, str]]:
        if self.trace_chance is not None and random() > self.trace_chance:
            yield {}
            return
        trace_id = f"{self.trace_prefix}:{_trace_cnt()}"
        span_id = f"{self.trace_prefix}:span:{_span_cnt()}"
        start = time()
        duration_start = perf_counter()
        trace_metadata = kwargs
        self._active_trace_and_span.set((trace_id, span_id))
        try:
            yield trace_metadata
        except BaseException as exc:
            trace_metadata["error"] = repr(exc)
            raise
        finally:
            duration = perf_counter() - duration_start
            self._append_to_buffer(
                self.metadata
                | {
                    "name": name,
                    "time": start,
                    "duration_ms": duration * 1000,
                    "trace.trace_id": trace_id,
                    "trace.span_id": span_id,
                }
                | trace_metadata
            )

    @contextmanager
    def span(
        self, name: str, parent: tuple[str, str] | None = None, **kwargs: str
    ) -> Iterator[dict[str, str]]:
        parent = parent or self._active_trace_and_span.get()
        span_metadata: dict[str, str] = kwargs
        if parent is None:
            yield span_metadata
            return

        trace_id, parent_span_id = parent
        start = time()
        duration_start = perf_counter()
        span_id = f"{self.trace_prefix}:span:{_span_cnt()}"
        token = self._active_trace_and_span.set((trace_id, span_id))
        try:
            yield span_metadata
        except BaseException as exc:
            span_metadata["error"] = repr(exc)
            raise
        finally:
            duration = perf_counter() - duration_start
            self._append_to_buffer(
                self.metadata
                | {
                    "name": name,
                    "time": start,
                    "duration_ms": duration * 1000,
                    "trace.trace_id": trace_id,
                    "trace.span_id": span_id,
                    "trace.parent_id": parent_span_id,
                }
                | span_metadata
            )
            self._active_trace_and_span.reset(token)

    @contextmanager
    def span_from_dict(
        self, name: str, parent_dict: Mapping[str, str], **kwargs: str
    ) -> Iterator[dict[str, str]]:
        """Start a child span, if possible."""
        if "_trace" not in parent_dict:
            yield {}
            return
        trace_id, parent_id = parent_dict["_trace"].split(",")
        with self.span(name, (trace_id, parent_id), **kwargs) as span_metadata:
            yield span_metadata

    def span_to_dict(self) -> dict[str, str]:
        parent = self._active_trace_and_span.get()
        return {} if parent is None else {"_trace": ",".join(parent)}

    def drain_buffer(self) -> list[dict[str, str | float]]:
        buf = self._buffer
        self._buffer = []
        return buf

    def _append_to_buffer(self, val: dict[str, Any]) -> None:
        if len(self._buffer) <= self.buffer_len:
            self._buffer.append(val)


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

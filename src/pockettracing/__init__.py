"""The beginnings of a simple tracing module."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from itertools import count
from random import random
from secrets import token_hex
from time import perf_counter, time
from typing import Iterator, Mapping, Protocol, Dict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

_ELEMENT: TypeAlias = Dict[str, Union[str, float]]

_trace_cnt = count().__next__
_span_cnt = count().__next__


class BufferProtocol(Protocol):
    size_limit: int | None

    def append(self, item: _ELEMENT) -> None:
        ...

    def drain(self) -> list[_ELEMENT]:
        ...


class _ListBuffer:
    _data: list[_ELEMENT]
    size_limit: int | None

    def __init__(self, size_limit: int | None = None):
        self.size_limit = size_limit
        self._data = []

    def append(self, item: _ELEMENT):
        if self.size_limit is None or len(self._data) <= self.size_limit:
            self._data.append(item)

    def drain(self) -> list[_ELEMENT]:
        res = self._data
        self._data = []
        return res


class Tracer:
    """A tracer for generating traces to be sent to a tracing service."""

    metadata: dict[str, str]
    trace_prefix: str
    trace_chance: float | None
    _buffer: BufferProtocol
    _active_trace_and_span: ContextVar[tuple[str, str] | None]

    def __init__(
        self,
        metadata: dict[str, str] | None,
        trace_prefix: str | None = None,
        buffer_len: int = 1000,
        trace_chance: float | None = None,
    ):
        self.metadata = {} if metadata is None else metadata
        self.trace_prefix = token_hex(8) if trace_prefix is None else trace_prefix
        self._buffer = _ListBuffer(buffer_len)
        self.trace_chance = trace_chance
        self._active_trace_and_span = ContextVar("active", default=None)

    @property
    def buffer_len(self) -> int | None:
        return self._buffer.size_limit

    @buffer_len.setter
    def buffer_len(self, value: int | None):
        self._buffer.size_limit = value

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
            data: _ELEMENT = {
                "name": name,
                "time": start,
                "duration_ms": duration * 1000,
                "trace.trace_id": trace_id,
                "trace.span_id": span_id,
            }
            self._buffer.append(self.metadata | data | trace_metadata)

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
            data: _ELEMENT = {
                "name": name,
                "time": start,
                "duration_ms": duration * 1000,
                "trace.trace_id": trace_id,
                "trace.span_id": span_id,
                "trace.parent_id": parent_span_id,
            }
            self._buffer.append(self.metadata | data | span_metadata)
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

    def drain_buffer(self) -> list[_ELEMENT]:
        return self._buffer.drain()

    def span_to_dict(self) -> dict[str, str]:
        parent = self._active_trace_and_span.get()
        return {} if parent is None else {"_trace": ",".join(parent)}

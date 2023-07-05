"""The beginnings of a simple tracing module."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from itertools import count
from random import random
from secrets import token_hex
from time import perf_counter, time
from typing import Callable, Iterator, Mapping, NotRequired, TypedDict

from attrs import Factory, define
from rich import print as rich_print
from rich.columns import Columns
from rich.console import Group
from rich.text import Text
from rich.tree import Tree

_trace_cnt = count().__next__
_span_cnt = count().__next__

Span = TypedDict(
    "Span",
    {
        "name": str,
        "time": float,
        "duration_ms": float,
        "service.name": str,
        "trace.trace_id": str,
        "trace.span_id": str,
        "trace.parent_id": NotRequired[str],
    },
)


@define
class Tracer:
    """A tracer for generating traces to be sent to a tracing service."""

    metadata: dict[str, str] = {}  # Should contain service.name.
    receivers: list[Callable[[list[Span]], None]] = []
    trace_prefix: str = Factory(lambda: token_hex(8))
    trace_chance: float | None = None
    _active_trace_and_span: ContextVar[tuple[str, str, list[Span]] | None] = Factory(
        lambda: ContextVar("active", default=None)
    )

    @contextmanager
    def trace(self, name: str, **kwargs: str) -> Iterator[dict[str, str]]:
        """Start a trace and a span, trace_chance permitting.

        Return a dictionary that can be used to add metadata.
        """
        if self.trace_chance is not None and random() > self.trace_chance:
            yield {}
            return
        trace_id = f"{self.trace_prefix}:{_trace_cnt()}"
        span_id = f"{self.trace_prefix}:span:{_span_cnt()}"
        start = time()
        duration_start = perf_counter()
        trace_metadata = kwargs
        child_spans: list[Span] = []
        token = self._active_trace_and_span.set((trace_id, span_id, child_spans))
        try:
            yield trace_metadata
        except BaseException as exc:
            trace_metadata["error"] = repr(exc)
            raise
        finally:
            self._active_trace_and_span.reset(token)
            duration = perf_counter() - duration_start
            child_spans.append(
                self.metadata  # type: ignore
                | {
                    "name": name,
                    "time": start,
                    "duration_ms": duration * 1000,
                    "trace.trace_id": trace_id,
                    "trace.span_id": span_id,
                }
                | trace_metadata
            )
            self._emit(child_spans)

    @contextmanager
    def span(
        self,
        name: str,
        parent: tuple[str, str, list[Span]] | None = None,
        **kwargs: str,
    ) -> Iterator[dict[str, str]]:
        """Start a new span, if there is a trace active."""
        parent = parent or self._active_trace_and_span.get()
        span_metadata: dict[str, str] = kwargs
        if parent is None:
            yield span_metadata
            return

        trace_id, parent_span_id, children = parent
        start = time()
        duration_start = perf_counter()
        span_id = f"{self.trace_prefix}:span:{_span_cnt()}"
        token = self._active_trace_and_span.set((trace_id, span_id, children))
        try:
            yield span_metadata
        except BaseException as exc:
            span_metadata["error"] = repr(exc)
            raise
        finally:
            self._active_trace_and_span.reset(token)
            duration = perf_counter() - duration_start
            children.append(
                self.metadata  # type: ignore
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

    @contextmanager
    def span_from_dict(
        self, name: str, parent_dict: Mapping[str, str], **kwargs: str
    ) -> Iterator[dict[str, str]]:
        """Start a child span, if possible."""
        if "_trace" not in parent_dict:
            yield {}
            return
        trace_id, parent_id = parent_dict["_trace"].split(",")
        children: list[Span] = []
        try:
            with self.span(
                name, (trace_id, parent_id, children), **kwargs
            ) as span_metadata:
                yield span_metadata
        finally:
            self._emit(children)

    def span_to_dict(self) -> dict[str, str]:
        parent = self._active_trace_and_span.get()
        return {} if parent is None else {"_trace": f"{parent[0]},{parent[1]}"}

    def _emit(self, spans: list[Span]) -> None:
        """When a unit of work is finished, notify all receivers."""
        for receiver in self.receivers:
            receiver(spans)


def encode_trace(spans: list[Span]) -> None:
    """Encode a trace for logs and eventual storage in OpenSearch."""
    from orjson import dumps

    last_span = spans[-1]
    if "trace.parent_id" not in last_span:
        # We only send the Trace Group metadata if we're ending
        # the trace here.
        # If it was started somewhere else, it will be set there.
        tg = {
            "traceGroup": last_span["name"],
            "traceGroupFields": {
                "endTime": datetime.fromtimestamp(
                    last_span["time"] + (last_span["duration_ms"] / 1000)
                ).isoformat(timespec="microseconds"),
                "durationInNanos": int(last_span["duration_ms"] * 1_000_000),
            },
        }
    else:
        tg = {}

    for span in spans:
        span_dict = {
            "logger": "trace",
            "traceId": span["trace.trace_id"],
            "spanId": span["trace.span_id"],
            "name": span["name"],
            "startTime": datetime.fromtimestamp(span["time"]).isoformat(
                timespec="microseconds"
            ),
            "endTime": (
                datetime.fromtimestamp(
                    span["time"] + (span["duration_ms"] / 1000)
                ).isoformat(timespec="microseconds")
            ),
            "serviceName": span["service.name"],
            "durationInNanos": int(span["duration_ms"] * 1_000_000),
            "parentSpanId": span.get("trace.parent_id", ""),
        }

        metadata = {
            k: str(v)
            for k, v in span.items()
            if k not in Span.__required_keys__ | Span.__optional_keys__
        }
        print(dumps(span_dict | metadata | tg).decode(), flush=True)


def print_trace(spans: list[Span]) -> None:
    """Format a trace with Rich and print it out in the terminal."""

    start = min(s["time"] for s in spans)
    end = max(s["time"] + (s["duration_ms"] / 1000) for s in spans)

    trace_span_ids = {s["trace.span_id"] for s in spans}

    root_span = [
        s
        for s in spans
        if ("trace.parent_id" not in s or s["trace.parent_id"] not in trace_span_ids)
    ][0]
    tree = Tree(t := Text(root_span["name"], style="bold white"))
    t.set_length(30)
    data = _process_children(root_span, spans, start, end, tree)

    width = 120
    lines = []
    durations = []  # In seconds
    metadata_strings = []
    for _, start, stop, dur, metadata in data:
        prefix = int(start * width) * " "
        body = int((stop - start) * width) * "━"
        suffix = int((1.0 - stop) * width) * " "
        line = Text(prefix + body + suffix)
        line.set_length(width)
        lines.append(line)
        durations.append(dur)
        metadata_strings.append(
            " ".join(f"{k}=[magenta]{v}[/]" for k, v in metadata.items())
        )

    g = Columns(
        [
            tree,
            Group(f"[bold sea_green2]{lines[0]}[/]", *[line for line in lines[1:]]),
            Group(*[f"[dim]{dur * 1000:4.0f} [italic]ms[/][/]" for dur in durations]),
            Group(*metadata_strings),
        ]
    )
    rich_print(g)


def _process_children(
    parent: Span, spans: list[Span], start: float, end: float, tree: Tree
) -> list[tuple[str, float, float, float, dict[str, str]]]:
    total_duration = end - start
    span_duration = parent["duration_ms"] / 1000
    start_pct = (parent["time"] - start) / total_duration
    res = [
        (
            parent["name"],
            start_pct,
            start_pct + (span_duration / total_duration),
            span_duration,
            {
                k: str(v)
                for k, v in parent.items()
                if k not in Span.__required_keys__ | Span.__optional_keys__
            },
        )
    ]
    children = [s for s in spans if s.get("trace.parent_id") == parent["trace.span_id"]]
    for child in children:
        child_tree = tree.add(child["name"])
        child_data = _process_children(child, spans, start, end, child_tree)
        res.extend(child_data)
    return res

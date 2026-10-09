"""nigiri — streaming source adapters that yield micro-batched DataFrames."""

from __future__ import annotations

import json
import queue
import socketserver
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Generator

import pandas as pd

_SOURCES = ("kafka", "kinesis", "websocket", "webhook")


def _to_dataframe(
    rows: list[Any], schema: dict[str, dict] | None
) -> pd.DataFrame:
    from . import wasabi

    if schema is not None:
        return wasabi.normalize(wasabi.flatten(rows), schema)
    return pd.DataFrame(rows)


def _drain_queue(
    q: "queue.Queue[Any]",
    *,
    batch_size: int,
    timeout_ms: int,
    max_batches: int | None,
    schema: dict[str, dict] | None,
    stop_event: threading.Event,
) -> Generator[pd.DataFrame, None, None]:
    """Shared batching loop: pulls deserialized rows off a queue into DataFrames."""

    batches_yielded = 0

    while max_batches is None or batches_yielded < max_batches:
        rows: list[Any] = []
        deadline = time.monotonic() + timeout_ms / 1000

        while len(rows) < batch_size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                rows.append(q.get(timeout=remaining))
            except queue.Empty:
                break

        if rows:
            yield _to_dataframe(rows, schema)
            batches_yielded += 1
        elif stop_event.is_set() and q.empty():
            break


class _WebhookServer(ThreadingHTTPServer):
    """ThreadingHTTPServer without the reverse-DNS lookup in ``server_bind``.

    ``HTTPServer.server_bind`` calls ``socket.getfqdn()`` after binding but
    before listening. Where reverse DNS is slow (common on macOS), that
    delays startup by seconds, and connections in the meantime are refused.
    The name is only used for display, so the bind host is used instead.
    """

    daemon_threads = True

    def server_bind(self) -> None:
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = str(host)
        self.server_port = port


def _make_webhook_handler(path: str, q: "queue.Queue[Any]", deserializer: Callable[[bytes], Any]):
    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 (stdlib method name)
            if self.path != path:
                self.send_response(404)
                self.end_headers()
                return

            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            q.put(deserializer(body))
            self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            pass  # silence default stderr request logging

    return _Handler


def _stream_webhook(
    config: dict[str, Any],
    *,
    batch_size: int,
    timeout_ms: int,
    schema: dict[str, dict] | None,
    deserializer: Callable[[bytes], Any] | None,
    max_batches: int | None,
) -> Generator[pd.DataFrame, None, None]:
    q: "queue.Queue[Any]" = queue.Queue()
    stop_event = threading.Event()
    deserialize = deserializer or (lambda body: json.loads(body))

    path = config.get("path", "/ingest")
    handler = _make_webhook_handler(path, q, deserialize)
    server = _WebhookServer((config.get("host", "0.0.0.0"), config.get("port", 8080)), handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    try:
        yield from _drain_queue(
            q,
            batch_size=batch_size,
            timeout_ms=timeout_ms,
            max_batches=max_batches,
            schema=schema,
            stop_event=stop_event,
        )
    finally:
        stop_event.set()
        server.shutdown()
        server_thread.join(timeout=5)


def _stream_websocket(
    config: dict[str, Any],
    *,
    batch_size: int,
    timeout_ms: int,
    schema: dict[str, dict] | None,
    deserializer: Callable[[bytes | str], Any] | None,
    max_batches: int | None,
) -> Generator[pd.DataFrame, None, None]:
    try:
        import asyncio

        import websockets
    except ImportError as e:
        raise ImportError(
            "WebSocket support requires websockets. "
            "Install it with: pip install sushitruck[websocket]"
        ) from e

    q: "queue.Queue[Any]" = queue.Queue()
    stop_event = threading.Event()
    deserialize = deserializer or (lambda msg: json.loads(msg))

    async def _consume() -> None:
        async with websockets.connect(config["uri"], additional_headers=config.get("headers", {})) as ws:
            async for message in ws:
                q.put(deserialize(message))

    def _run_loop() -> None:
        try:
            asyncio.run(_consume())
        finally:
            stop_event.set()

    consumer_thread = threading.Thread(target=_run_loop, daemon=True)
    consumer_thread.start()

    try:
        yield from _drain_queue(
            q,
            batch_size=batch_size,
            timeout_ms=timeout_ms,
            max_batches=max_batches,
            schema=schema,
            stop_event=stop_event,
        )
    finally:
        stop_event.set()
        consumer_thread.join(timeout=5)


def _stream_kafka(
    config: dict[str, Any],
    *,
    batch_size: int,
    timeout_ms: int,
    schema: dict[str, dict] | None,
    deserializer: Callable[[bytes], Any] | None,
    max_batches: int | None,
) -> Generator[pd.DataFrame, None, None]:
    try:
        from confluent_kafka import Consumer
    except ImportError as e:
        raise ImportError(
            "Kafka support requires confluent-kafka. "
            "Install it with: pip install sushitruck[kafka]"
        ) from e

    deserialize = deserializer or (lambda value: json.loads(value))
    topic = config["topic"]
    consumer_config = {k: v for k, v in config.items() if k != "topic"}

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    try:
        batches_yielded = 0
        while max_batches is None or batches_yielded < max_batches:
            rows: list[Any] = []
            deadline = time.monotonic() + timeout_ms / 1000

            while len(rows) < batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                message = consumer.poll(timeout=remaining)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(f"Kafka consumer error: {message.error()}")
                rows.append(deserialize(message.value()))

            if rows:
                yield _to_dataframe(rows, schema)
                batches_yielded += 1
    finally:
        consumer.close()


def _stream_kinesis(
    config: dict[str, Any],
    *,
    batch_size: int,
    timeout_ms: int,
    schema: dict[str, dict] | None,
    deserializer: Callable[[bytes], Any] | None,
    max_batches: int | None,
) -> Generator[pd.DataFrame, None, None]:
    try:
        import boto3
    except ImportError as e:
        raise ImportError(
            "Kinesis support requires boto3. Install it with: pip install sushitruck[kinesis]"
        ) from e

    deserialize = deserializer or (lambda data: json.loads(data))
    client = boto3.client("kinesis", region_name=config.get("region_name"))

    shard_iterator = client.get_shard_iterator(
        StreamName=config["stream_name"],
        ShardId=config["shard_id"],
        ShardIteratorType=config.get("iterator_type", "TRIM_HORIZON"),
    )["ShardIterator"]

    batches_yielded = 0
    while max_batches is None or batches_yielded < max_batches:
        rows: list[Any] = []
        deadline = time.monotonic() + timeout_ms / 1000

        while len(rows) < batch_size and time.monotonic() < deadline and shard_iterator:
            response = client.get_records(ShardIterator=shard_iterator, Limit=batch_size - len(rows))
            shard_iterator = response.get("NextShardIterator")
            records = response["Records"]

            if not records:
                time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
                continue

            rows.extend(deserialize(record["Data"]) for record in records)

        if rows:
            yield _to_dataframe(rows, schema)
            batches_yielded += 1

        if not shard_iterator:
            break


_STREAMERS: dict[str, Callable[..., Generator[pd.DataFrame, None, None]]] = {
    "webhook": _stream_webhook,
    "websocket": _stream_websocket,
    "kafka": _stream_kafka,
    "kinesis": _stream_kinesis,
}


def stream(
    source: str,
    *,
    config: dict[str, Any],
    batch_size: int = 500,
    timeout_ms: int = 1000,
    schema: dict[str, dict] | None = None,
    deserializer: Callable[[Any], Any] | None = None,
    max_batches: int | None = None,
) -> Generator[pd.DataFrame, None, None]:
    """
    Consume a continuous streaming source, yielding micro-batched DataFrames.

    Parameters
    ----------
    source
        ``"kafka"``, ``"kinesis"``, ``"websocket"``, or ``"webhook"``.
    config
        Source-specific connection config — see module docs for the shape
        expected by each source.
    batch_size
        Maximum rows per yielded DataFrame.
    timeout_ms
        Maximum time to wait for a full batch before yielding a partial one.
        Never stalls indefinitely waiting for a full batch.
    schema
        Optional :mod:`sushitruck.wasabi` schema applied to each batch via
        ``wasabi.flatten`` + ``wasabi.normalize``.
    deserializer
        Converts a raw message (bytes or str, depending on source) into a
        record (typically a dict). Defaults to ``json.loads``.
    max_batches
        Stop after yielding this many batches. ``None`` runs forever (or
        until the underlying source closes, for ``websocket``/``kinesis``).

    Returns
    -------
    Generator[pd.DataFrame, None, None]
        A generator of batches — never loads the whole stream into memory.

    Raises
    ------
    ValueError
        If ``source`` is not one of the supported source names.
    """

    if source not in _SOURCES:
        raise ValueError(f"Unsupported source: {source!r}. Supported sources: {_SOURCES}")

    return _STREAMERS[source](
        config,
        batch_size=batch_size,
        timeout_ms=timeout_ms,
        schema=schema,
        deserializer=deserializer,
        max_batches=max_batches,
    )

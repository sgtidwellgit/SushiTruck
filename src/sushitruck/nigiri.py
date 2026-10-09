"""nigiri — streaming source adapters that yield micro-batched DataFrames."""

from __future__ import annotations

import codecs
import json
import queue
import socket
import socketserver
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Generator, Iterable, Iterator

import pandas as pd

_SOURCES = ("kafka", "kinesis", "websocket", "webhook", "sse")

# HTTP statuses worth reconnecting after; any other 4xx is a caller error.
_SSE_RETRYABLE_4XX = (408, 429)


def _to_dataframe(rows: list[Any], normalizer: Any) -> pd.DataFrame:
    if normalizer is not None:
        from . import wasabi

        return normalizer(wasabi.flatten(rows))
    return pd.DataFrame(rows)


def _drain_queue(
    q: "queue.Queue[Any]",
    *,
    batch_size: int,
    timeout_ms: int,
    max_batches: int | None,
    normalizer: Any,
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
            yield _to_dataframe(rows, normalizer)
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
    normalizer: Any,
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
            normalizer=normalizer,
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
    normalizer: Any,
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
            normalizer=normalizer,
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
    normalizer: Any,
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
                yield _to_dataframe(rows, normalizer)
                batches_yielded += 1
    finally:
        consumer.close()


def _list_kinesis_shards(client: Any, stream_name: str) -> list[str]:
    """Return every shard id in the stream, following ``NextToken`` pages."""

    shard_ids: list[str] = []
    response = client.list_shards(StreamName=stream_name)
    while True:
        shard_ids.extend(shard["ShardId"] for shard in response.get("Shards", []))
        token = response.get("NextToken")
        if not token:
            return shard_ids
        # The API rejects StreamName alongside NextToken.
        response = client.list_shards(NextToken=token)


def _stream_kinesis(
    config: dict[str, Any],
    *,
    batch_size: int,
    timeout_ms: int,
    normalizer: Any,
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
    stream_name = config["stream_name"]
    iterator_type = config.get("iterator_type", "TRIM_HORIZON")

    def open_shard(shard_id: str, shard_iterator_type: str) -> str | None:
        return client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType=shard_iterator_type,
        )["ShardIterator"]

    # shard id -> current iterator. A single configured shard keeps the
    # one-shard behavior; otherwise every shard in the stream is read.
    if config.get("shard_id"):
        shard_ids = [config["shard_id"]]
    else:
        shard_ids = _list_kinesis_shards(client, stream_name)
    iterators: dict[str, str | None] = {sid: open_shard(sid, iterator_type) for sid in shard_ids}
    seen = set(iterators)
    follow_children = not config.get("shard_id")

    batches_yielded = 0
    while iterators and (max_batches is None or batches_yielded < max_batches):
        rows: list[Any] = []
        deadline = time.monotonic() + timeout_ms / 1000

        while len(rows) < batch_size and iterators and time.monotonic() < deadline:
            got_records = False
            # Round-robin: one GetRecords call per shard per pass.
            for shard_id in list(iterators):
                if len(rows) >= batch_size:
                    break
                response = client.get_records(
                    ShardIterator=iterators[shard_id], Limit=batch_size - len(rows)
                )
                records = response["Records"]
                if records:
                    got_records = True
                    rows.extend(deserialize(record["Data"]) for record in records)

                next_iterator = response.get("NextShardIterator")
                if next_iterator:
                    iterators[shard_id] = next_iterator
                    continue

                # The shard is closed and fully read. After a reshard, its
                # children carry on from where it ended.
                del iterators[shard_id]
                if follow_children:
                    for child in response.get("ChildShards") or []:
                        child_id = child["ShardId"]
                        if child_id not in seen:
                            seen.add(child_id)
                            iterators[child_id] = open_shard(child_id, "TRIM_HORIZON")

            if not got_records:
                time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))

        if rows:
            yield _to_dataframe(rows, normalizer)
            batches_yielded += 1


def _iter_sse_lines(chunks: Iterable[str]) -> Iterator[str]:
    """Split decoded text chunks into lines ending in CRLF, LF, or CR.

    Handles a CRLF pair split across two chunks, which naive ``splitlines``
    on each chunk would turn into a spurious blank line (and a blank line
    dispatches an event in SSE).
    """

    buf = ""
    for chunk in chunks:
        buf += chunk
        while True:
            positions = [i for i in (buf.find("\r"), buf.find("\n")) if i != -1]
            if not positions:
                break
            i = min(positions)
            if buf[i] == "\r":
                if i + 1 == len(buf):
                    break  # wait: the next chunk may start with "\n"
                end = i + 2 if buf[i + 1] == "\n" else i + 1
            else:
                end = i + 1
            yield buf[:i]
            buf = buf[end:]
    if buf:
        yield buf.rstrip("\r")


def _parse_sse(lines: Iterable[str]) -> Iterator[tuple[str, Any]]:
    """Parse the text/event-stream format (WHATWG HTML spec, "server-sent events").

    Yields ``("event", {"event", "data", "id"})`` for each dispatched event
    and ``("retry", milliseconds)`` when the server sets a reconnect delay.
    """

    event_type = ""
    data_lines: list[str] = []
    last_id: str | None = None

    for line in lines:
        if line == "":
            if data_lines:
                yield "event", {
                    "event": event_type or "message",
                    "data": "\n".join(data_lines),
                    "id": last_id,
                }
            event_type = ""
            data_lines = []
            continue
        if line.startswith(":"):
            continue  # comment / keep-alive

        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]

        if field == "data":
            data_lines.append(value)
        elif field == "event":
            event_type = value
        elif field == "id":
            if "\0" not in value:
                last_id = value
        elif field == "retry":
            if value.isdigit():
                yield "retry", int(value)
    # An event without its terminating blank line is incomplete and dropped.


def _iter_available(response: Any) -> Iterator[bytes]:
    """Yield response bytes as soon as they arrive.

    ``iter_content(chunk_size=None)`` only does this for chunked transfer
    encoding; a stream delimited by connection close would be buffered
    until the server hangs up, which for a live feed is never.
    """

    read1 = getattr(response.raw, "read1", None)
    if read1 is None:  # urllib3 without read1(): one byte at a time is slow but live
        yield from response.iter_content(chunk_size=1)
        return
    while True:
        data = read1(65536)
        if not data:
            return
        yield data


def _interrupt_read(response: Any) -> None:
    """Unblock a thread waiting in a read on ``response`` by shutting down its socket.

    Closing the response from another thread would wait for the blocked read
    to finish (the buffered reader holds a lock), so a quiet stream could
    take until its next event to stop. On Linux and macOS, shutting down the
    socket makes the pending read return immediately. Windows does not wake
    a blocked ``recv`` this way, and neither path may exist on other urllib3
    versions; in those cases the daemon reader thread exits at its next
    event or read timeout. The caller is never kept waiting: ``stream``
    only joins the thread for up to a second, and no events are delivered
    after the generator is closed.
    """

    raw = response.raw
    getters = (
        lambda: raw._fp.fp.raw._sock,   # http.client response -> socket file -> socket
        lambda: raw._connection.sock,   # urllib3 connection
    )
    for get in getters:
        try:
            sock = get()
        except AttributeError:
            continue
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            return


def _stream_sse(
    config: dict[str, Any],
    *,
    batch_size: int,
    timeout_ms: int,
    normalizer: Any,
    deserializer: Callable[[str], Any] | None,
    max_batches: int | None,
) -> Generator[pd.DataFrame, None, None]:
    import requests
    import urllib3

    url = config["url"]
    headers = dict(config.get("headers") or {})
    params = config.get("params")
    wanted = set(config["events"]) if config.get("events") else None
    reconnect = config.get("reconnect", True)
    max_retries = config.get("max_retries")
    read_timeout = config.get("read_timeout", 60)
    retry_ms = [int(config.get("retry_ms", 3000))]
    deserialize = deserializer or (lambda data: json.loads(data))

    q: "queue.Queue[Any]" = queue.Queue()
    stop_event = threading.Event()
    errors: list[BaseException] = []
    session = requests.Session()
    current: dict[str, Any] = {"response": None}

    def consume() -> None:
        last_id: str | None = None
        failures = 0
        try:
            while not stop_event.is_set():
                request_headers = {
                    "Accept": "text/event-stream",
                    "Cache-Control": "no-cache",
                    # Compression can hold events back in the server's buffer.
                    "Accept-Encoding": "identity",
                    **headers,
                }
                if last_id is not None:
                    request_headers["Last-Event-ID"] = last_id

                try:
                    response = session.get(
                        url,
                        params=params,
                        headers=request_headers,
                        stream=True,
                        timeout=(10, read_timeout),
                    )
                    current["response"] = response
                    try:
                        if response.status_code == 204:
                            return  # the server asks the client to stop reconnecting
                        status = response.status_code
                        if 400 <= status < 500 and status not in _SSE_RETRYABLE_4XX:
                            response.raise_for_status()  # caller error: surface it
                        if status >= 400:
                            raise requests.HTTPError(f"{status} from {url}", response=response)
                        failures = 0

                        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
                        chunks = (decoder.decode(c) for c in _iter_available(response))
                        for kind, value in _parse_sse(_iter_sse_lines(chunks)):
                            if stop_event.is_set():
                                return
                            if kind == "retry":
                                retry_ms[0] = value
                                continue
                            if value["id"] is not None:
                                last_id = value["id"]
                            if wanted is not None and value["event"] not in wanted:
                                continue
                            q.put(deserialize(value["data"]))
                    finally:
                        response.close()
                except requests.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else 0
                    if 400 <= status < 500 and status not in _SSE_RETRYABLE_4XX:
                        raise
                except (requests.ConnectionError, requests.Timeout, urllib3.exceptions.HTTPError):
                    pass  # dropped or silent connection: reconnect below

                if stop_event.is_set() or not reconnect:
                    return
                failures += 1
                if max_retries is not None and failures > max_retries:
                    raise ConnectionError(
                        f"SSE stream {url} failed {failures} times in a row; giving up."
                    )
                stop_event.wait(retry_ms[0] / 1000)
        except BaseException as exc:  # noqa: BLE001 - re-raised in the caller's thread
            if not stop_event.is_set():
                errors.append(exc)
        finally:
            stop_event.set()
            session.close()

    consumer_thread = threading.Thread(target=consume, daemon=True)
    consumer_thread.start()

    try:
        yield from _drain_queue(
            q,
            batch_size=batch_size,
            timeout_ms=timeout_ms,
            max_batches=max_batches,
            normalizer=normalizer,
            stop_event=stop_event,
        )
        if errors:
            raise errors[0]
    finally:
        stop_event.set()
        response = current["response"]
        if response is not None:
            _interrupt_read(response)  # the reader thread then closes it
        consumer_thread.join(timeout=1)


_STREAMERS: dict[str, Callable[..., Generator[pd.DataFrame, None, None]]] = {
    "webhook": _stream_webhook,
    "websocket": _stream_websocket,
    "kafka": _stream_kafka,
    "kinesis": _stream_kinesis,
    "sse": _stream_sse,
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
        ``"kafka"``, ``"kinesis"``, ``"websocket"``, ``"webhook"``, or
        ``"sse"`` (server-sent events).
    config
        Source-specific connection config:

        - ``"webhook"``: ``host`` (default ``"0.0.0.0"``), ``port`` (default
          ``8080``), ``path`` (default ``"/ingest"``).
        - ``"websocket"``: ``uri`` (required), ``headers``.
        - ``"kafka"``: ``topic`` (required) plus any ``confluent_kafka``
          consumer setting (``bootstrap.servers``, ``group.id``, ...).
        - ``"kinesis"``: ``stream_name`` (required), ``region_name``,
          ``iterator_type`` (default ``"TRIM_HORIZON"``), and optionally
          ``shard_id``. Without ``shard_id``, every shard is read and the
          child shards of a reshard are followed; with it, only that shard.
        - ``"sse"``: ``url`` (required), ``headers``, ``params``,
          ``events`` (event types to keep; default all), ``reconnect``
          (default ``True``), ``retry_ms`` (reconnect delay, default
          ``3000``; the server's ``retry:`` field overrides it),
          ``max_retries`` (consecutive failed reconnects before raising;
          default unlimited), ``read_timeout`` (seconds of silence before
          reconnecting, default ``60``; ``None`` waits forever).
    batch_size
        Maximum rows per yielded DataFrame.
    timeout_ms
        Maximum time to wait for a full batch before yielding a partial one.
        Never stalls indefinitely waiting for a full batch.
    schema
        Optional :mod:`sushitruck.wasabi` schema applied to each batch via
        ``wasabi.flatten`` + ``wasabi.normalize``. A warning about columns
        missing from the schema is issued once per stream, not per batch.
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

    from . import wasabi

    normalizer = wasabi._BatchNormalizer(schema) if schema is not None else None
    return _STREAMERS[source](
        config,
        batch_size=batch_size,
        timeout_ms=timeout_ms,
        normalizer=normalizer,
        deserializer=deserializer,
        max_batches=max_batches,
    )

import asyncio
import json
import socket
import threading
import time

import pandas as pd
import pytest
import requests

from sushitruck import nigiri


def _post(url: str, payload: dict) -> requests.Response:
    """POST, retrying while the listener is still starting (connection refused)."""

    deadline = time.monotonic() + 10
    while True:
        try:
            return requests.post(url, json=payload, timeout=5)
        except requests.ConnectionError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def test_stream_rejects_unknown_source():
    with pytest.raises(ValueError):
        nigiri.stream("carrier-pigeon", config={})


def test_webhook_stream_yields_posted_batches():
    port = _free_port()
    config = {"host": "127.0.0.1", "port": port, "path": "/ingest"}

    # A generous timeout so both messages land in one batch even on a slow machine.
    gen = nigiri.stream("webhook", config=config, batch_size=2, timeout_ms=5000, max_batches=1)

    def post_messages():
        for i in range(2):
            _post(f"http://127.0.0.1:{port}/ingest", {"i": i})

    threading.Thread(target=post_messages, daemon=True).start()

    batch = next(gen)
    assert len(batch) == 2
    assert sorted(batch["i"].tolist()) == [0, 1]

    gen.close()


def test_webhook_stream_yields_partial_batch_on_timeout():
    port = _free_port()
    config = {"host": "127.0.0.1", "port": port, "path": "/ingest"}

    gen = nigiri.stream("webhook", config=config, batch_size=100, timeout_ms=200, max_batches=1)

    def post_one():
        _post(f"http://127.0.0.1:{port}/ingest", {"i": 1})

    threading.Thread(target=post_one, daemon=True).start()

    batch = next(gen)
    assert len(batch) == 1

    gen.close()


def test_websocket_stream_yields_pushed_messages():
    websockets = pytest.importorskip("websockets")
    from websockets.asyncio.server import serve

    port = _free_port()
    messages = [json.dumps({"i": 0}), json.dumps({"i": 1})]

    async def handler(ws):
        for message in messages:
            await ws.send(message)

    async def run_server_and_collect():
        async with serve(handler, "127.0.0.1", port):
            config = {"uri": f"ws://127.0.0.1:{port}"}
            gen = nigiri.stream("websocket", config=config, batch_size=2, timeout_ms=1000, max_batches=1)
            loop = asyncio.get_event_loop()
            batch = await loop.run_in_executor(None, next, gen)
            gen.close()
            return batch

    batch = asyncio.run(run_server_and_collect())
    assert len(batch) == 2
    assert sorted(batch["i"].tolist()) == [0, 1]


def test_kafka_stream_raises_import_error_without_extra(no_kafka):
    with pytest.raises(ImportError, match=r"sushitruck\[kafka\]"):
        next(nigiri.stream("kafka", config={"topic": "t"}, max_batches=1))


def test_kinesis_stream_raises_import_error_without_extra(no_boto3):
    with pytest.raises(ImportError, match=r"sushitruck\[kinesis\]"):
        next(
            nigiri.stream(
                "kinesis",
                config={"stream_name": "s", "shard_id": "shard-0"},
                max_batches=1,
            )
        )


# --- Kafka and Kinesis (in-memory fakes from conftest.py) ---


def test_kafka_stream_yields_batches_and_strips_topic_from_config(fake_kafka):
    fake_kafka.topics["trades"] = [fake_kafka.Message(json.dumps({"i": i}).encode()) for i in range(5)]
    config = {"bootstrap.servers": "localhost:9092", "group.id": "g", "topic": "trades"}

    batches = list(nigiri.stream("kafka", config=config, batch_size=2, timeout_ms=100, max_batches=3))

    assert [len(b) for b in batches] == [2, 2, 1]
    assert pd.concat(batches)["i"].tolist() == [0, 1, 2, 3, 4]
    assert "topic" not in fake_kafka.state["consumer_config"]
    assert fake_kafka.state["closed"] is True


def test_kafka_stream_custom_deserializer_and_schema(fake_kafka):
    fake_kafka.topics["t"] = [fake_kafka.Message(b"1.5"), fake_kafka.Message(b"2.5")]

    batch = next(
        nigiri.stream(
            "kafka",
            config={"topic": "t"},
            batch_size=2,
            timeout_ms=100,
            deserializer=lambda raw: {"price": raw.decode(), "meta": {"src": "k"}},
            schema={"price": {"dtype": float}, "meta_src": {"dtype": str, "rename": "source"}},
        )
    )

    assert batch["price"].tolist() == [1.5, 2.5]
    assert batch["source"].tolist() == ["k", "k"]


def test_kafka_stream_raises_on_message_error(fake_kafka):
    fake_kafka.topics["t"] = [fake_kafka.Message(b"", error="broker down")]

    with pytest.raises(RuntimeError, match="broker down"):
        next(nigiri.stream("kafka", config={"topic": "t"}, timeout_ms=100, max_batches=1))
    assert fake_kafka.state["closed"] is True


def test_kinesis_stream_reads_until_shard_closes(fake_boto3):
    fake_boto3.kinesis.records = [json.dumps({"i": i}).encode() for i in range(5)]
    config = {"stream_name": "events", "shard_id": "shardId-0", "region_name": "us-east-1"}

    batches = list(nigiri.stream("kinesis", config=config, batch_size=2, timeout_ms=200))

    assert [len(b) for b in batches] == [2, 2, 1]
    assert pd.concat(batches)["i"].tolist() == [0, 1, 2, 3, 4]


def test_kinesis_stream_respects_max_batches(fake_boto3):
    fake_boto3.kinesis.records = [json.dumps({"i": i}).encode() for i in range(10)]
    config = {"stream_name": "events", "shard_id": "shardId-0"}

    batches = list(nigiri.stream("kinesis", config=config, batch_size=3, timeout_ms=200, max_batches=2))
    assert [len(b) for b in batches] == [3, 3]


def test_websocket_stream_without_extra_raises_import_error(no_websockets):
    with pytest.raises(ImportError, match=r"sushitruck\[websocket\]"):
        next(nigiri.stream("websocket", config={"uri": "ws://localhost:1"}, max_batches=1))


def test_webhook_ignores_unknown_paths():
    port = _free_port()
    config = {"host": "127.0.0.1", "port": port, "path": "/ingest"}
    gen = nigiri.stream("webhook", config=config, batch_size=1, timeout_ms=500, max_batches=1)

    statuses = {}

    def post_messages():
        statuses["wrong"] = _post(f"http://127.0.0.1:{port}/elsewhere", {"i": 0}).status_code
        statuses["right"] = _post(f"http://127.0.0.1:{port}/ingest", {"i": 1}).status_code

    threading.Thread(target=post_messages, daemon=True).start()

    batch = next(gen)
    gen.close()

    assert batch["i"].tolist() == [1]
    assert statuses == {"wrong": 404, "right": 200}


# --- Kinesis: every shard, and resharding ---


def _kinesis_values(batches):
    return sorted(json.loads(json.dumps(v)) for v in pd.concat(batches)["v"].tolist())


def test_kinesis_reads_every_shard_when_no_shard_id_given(fake_boto3):
    kinesis = fake_boto3.kinesis
    kinesis.shards = {
        "shardId-0": [b'{"v": 1}', b'{"v": 2}'],
        "shardId-1": [b'{"v": 3}'],
        "shardId-2": [b'{"v": 4}', b'{"v": 5}', b'{"v": 6}'],
    }
    kinesis.closed = set(kinesis.shards)

    batches = list(nigiri.stream("kinesis", config={"stream_name": "events"}, batch_size=4, timeout_ms=500))

    assert _kinesis_values(batches) == [1, 2, 3, 4, 5, 6]
    # Shard listing followed every page, sending NextToken without StreamName.
    assert kinesis.list_calls[0] == {"StreamName": "events"}
    assert [c for c in kinesis.list_calls[1:]] == [{"NextToken": "1"}, {"NextToken": "2"}]


def test_kinesis_follows_child_shards_after_a_reshard(fake_boto3):
    kinesis = fake_boto3.kinesis
    kinesis.shards = {
        "parent": [b'{"v": 1}', b'{"v": 2}'],
        "child-a": [b'{"v": 3}'],
        "child-b": [b'{"v": 4}'],
    }
    kinesis.closed = set(kinesis.shards)
    kinesis.listed = ["parent"]  # the children appear only after reading starts
    kinesis.children = {"parent": ["child-a", "child-b"]}

    config = {"stream_name": "events", "iterator_type": "LATEST"}
    batches = list(nigiri.stream("kinesis", config=config, batch_size=10, timeout_ms=500))

    assert _kinesis_values(batches) == [1, 2, 3, 4]
    types = {r["ShardId"]: r["ShardIteratorType"] for r in kinesis.iterator_requests}
    # The configured position applies to the original shards; children are read from their start.
    assert types == {"parent": "LATEST", "child-a": "TRIM_HORIZON", "child-b": "TRIM_HORIZON"}


def test_kinesis_with_shard_id_reads_only_that_shard(fake_boto3):
    kinesis = fake_boto3.kinesis
    kinesis.shards = {"parent": [b'{"v": 1}'], "other": [b'{"v": 2}'], "child": [b'{"v": 3}']}
    kinesis.closed = set(kinesis.shards)
    kinesis.children = {"parent": ["child"]}

    config = {"stream_name": "events", "shard_id": "parent"}
    batches = list(nigiri.stream("kinesis", config=config, batch_size=10, timeout_ms=300))

    assert _kinesis_values(batches) == [1]
    assert kinesis.list_calls == []


def test_kinesis_open_shard_does_not_block_batches_from_others(fake_boto3):
    kinesis = fake_boto3.kinesis
    kinesis.shards = {"busy": [b'{"v": 1}', b'{"v": 2}'], "quiet": []}
    kinesis.closed = {"busy"}  # "quiet" stays open with no data

    config = {"stream_name": "events"}
    batch = next(nigiri.stream("kinesis", config=config, batch_size=2, timeout_ms=1000, max_batches=1))

    assert sorted(batch["v"].tolist()) == [1, 2]


# --- schema warnings: once per stream, not once per batch ---


def test_stream_warns_once_about_extra_columns(fake_kafka):
    import warnings

    fake_kafka.topics["t"] = [fake_kafka.Message(json.dumps({"a": i, "extra": i}).encode()) for i in range(6)]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        batches = list(
            nigiri.stream("kafka", config={"topic": "t"}, batch_size=2, timeout_ms=100,
                          max_batches=3, schema={"a": {"dtype": int}})
        )

    assert len(batches) == 3
    extra_warnings = [w for w in caught if "not in schema" in str(w.message)]
    assert len(extra_warnings) == 1


# --- server-sent events ---


class _SSEServer:
    """Serves a scripted list of responses, one per connection (the last one repeats)."""

    def __init__(self, responses):
        self.responses = responses
        self.requests = []
        server_self = self

        class Handler(nigiri.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                server_self.requests.append({"path": self.path, **dict(self.headers)})
                index = min(len(server_self.requests), len(server_self.responses)) - 1
                status, pieces, hold_open = (list(server_self.responses[index]) + [0])[:3]
                self.send_response(status)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                for piece in pieces:
                    self.wfile.write(piece)
                    self.wfile.flush()
                    time.sleep(0.02)
                time.sleep(hold_open)

            def log_message(self, *args):
                pass

        self.server = nigiri._WebhookServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/events"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def sse_server():
    servers = []

    def make(responses):
        server = _SSEServer(responses)
        servers.append(server)
        return server

    yield make
    for server in servers:
        server.close()


def test_sse_line_splitter_handles_crlf_split_across_chunks():
    chunks = ["a\r", "\nb\rc\n", "\r\n", "d"]
    assert list(nigiri._iter_sse_lines(chunks)) == ["a", "b", "c", "", "d"]


def test_sse_parser_follows_the_spec():
    lines = [
        ": comment",
        "event: edit",
        "id: 7",
        "data: first",
        "data:second",
        "",
        "data",          # a field with no colon has an empty value
        "",
        "id: bad\0id",   # ids containing NUL are ignored
        "retry: 250",
        "data: x",
        "",
        "data: incomplete",  # no blank line before EOF: dropped
    ]
    parsed = list(nigiri._parse_sse(lines))

    assert parsed == [
        ("event", {"event": "edit", "data": "first\nsecond", "id": "7"}),
        ("event", {"event": "message", "data": "", "id": "7"}),
        ("retry", 250),
        ("event", {"event": "message", "data": "x", "id": "7"}),
    ]


def test_sse_stream_parses_filters_and_ends_when_server_closes(sse_server):
    server = sse_server([(200, [
        b": keep-alive\r\n",
        b"id: 1\r\nevent: edit\r\ndata: {\"n\": 1}\r",   # CRLF split across writes
        b"\n\r\n",
        b"data: {\"n\":\r\ndata: 2}\r\n\r\n",           # multi-line data
        b"event: ping\ndata: {\"n\": 99}\n\n",            # filtered out below
        b"data: {\"n\": 3}\n\n",
    ])])

    config = {"url": server.url, "events": ["message", "edit"], "reconnect": False,
              "headers": {"Authorization": "Bearer t"}}
    batches = list(nigiri.stream("sse", config=config, batch_size=10, timeout_ms=300))

    assert pd.concat(batches)["n"].tolist() == [1, 2, 3]
    request = server.requests[0]
    assert request["Accept"] == "text/event-stream"
    assert request["Authorization"] == "Bearer t"
    assert "Last-Event-ID" not in request


def test_sse_reconnects_with_last_event_id_and_stops_on_204(sse_server):
    server = sse_server([
        (200, [b"retry: 10\nid: a\ndata: {\"n\": 1}\n\nid: b\ndata: {\"n\": 2}\n\n"]),
        (200, [b"id: c\ndata: {\"n\": 3}\n\n"]),
        (204, []),
    ])

    batches = list(nigiri.stream("sse", config={"url": server.url}, batch_size=10, timeout_ms=300))

    assert pd.concat(batches)["n"].tolist() == [1, 2, 3]
    assert [r.get("Last-Event-ID") for r in server.requests] == [None, "b", "c"]


def test_sse_raises_on_client_error_without_retrying(sse_server):
    server = sse_server([(401, [])])

    with pytest.raises(requests.HTTPError):
        list(nigiri.stream("sse", config={"url": server.url, "retry_ms": 10}, timeout_ms=200))
    assert len(server.requests) == 1


def test_sse_gives_up_after_max_retries(sse_server):
    server = sse_server([(503, [])])

    config = {"url": server.url, "retry_ms": 10, "max_retries": 2}
    with pytest.raises(ConnectionError, match="3 times"):
        list(nigiri.stream("sse", config=config, timeout_ms=200))
    assert len(server.requests) == 3


def test_sse_custom_deserializer_and_schema(sse_server):
    server = sse_server([(200, [b"data: hello\n\ndata: sushi\n\n"])])

    batches = list(nigiri.stream(
        "sse",
        config={"url": server.url, "reconnect": False},
        batch_size=10,
        timeout_ms=300,
        deserializer=lambda text: {"text": text, "meta": {"length": len(text)}},
        schema={"text": {"dtype": str}, "meta_length": {"dtype": int, "rename": "length"}},
    ))

    df = pd.concat(batches)
    assert df["text"].tolist() == ["hello", "sushi"]
    assert df["length"].tolist() == [5, 5]


def test_sse_closing_the_generator_stops_a_quiet_stream_promptly(sse_server):
    # One event, then the server keeps the connection open and silent.
    server = sse_server([(200, [b"data: {\"n\": 1}\n\n"], 5)])

    start = time.monotonic()
    gen = nigiri.stream("sse", config={"url": server.url}, batch_size=1, timeout_ms=2000)
    assert next(gen)["n"].tolist() == [1]
    gen.close()

    assert time.monotonic() - start < 3

import asyncio
import json
import socket
import threading
import time

import pandas as pd
import pytest
import requests

from sushitruck import nigiri


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

    gen = nigiri.stream("webhook", config=config, batch_size=2, timeout_ms=300, max_batches=1)

    def post_messages():
        time.sleep(0.2)
        for i in range(2):
            requests.post(f"http://127.0.0.1:{port}/ingest", json={"i": i})

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
        time.sleep(0.1)
        requests.post(f"http://127.0.0.1:{port}/ingest", json={"i": 1})

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
        time.sleep(0.2)
        statuses["wrong"] = requests.post(f"http://127.0.0.1:{port}/elsewhere", json={"i": 0}).status_code
        statuses["right"] = requests.post(f"http://127.0.0.1:{port}/ingest", json={"i": 1}).status_code

    threading.Thread(target=post_messages, daemon=True).start()

    batch = next(gen)
    gen.close()

    assert batch["i"].tolist() == [1]
    assert statuses == {"wrong": 404, "right": 200}

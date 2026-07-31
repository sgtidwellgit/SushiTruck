import asyncio
import json
import socket
import threading
import time

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


def test_kafka_stream_raises_import_error_without_extra():
    with pytest.raises(ImportError):
        next(nigiri.stream("kafka", config={"topic": "t"}, max_batches=1))


def test_kinesis_stream_raises_import_error_without_extra():
    with pytest.raises(ImportError):
        next(
            nigiri.stream(
                "kinesis",
                config={"stream_name": "s", "shard_id": "shard-0"},
                max_batches=1,
            )
        )

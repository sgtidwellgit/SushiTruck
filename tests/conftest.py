"""Shared fixtures: in-memory stand-ins for the optional cloud and queue SDKs.

Each fixture installs a fake module into ``sys.modules`` for the duration of
one test, so the ``[cloud]``, ``[kafka]``, and ``[kinesis]`` code paths run
without network access or the real SDKs installed. The ``no_*`` fixtures do
the opposite: they make the import fail even if the real SDK is installed,
so the friendly ``ImportError`` messages are always exercised.
"""

from __future__ import annotations

import io
import sys
import types
from typing import Any

import pytest


class StreamingBody:
    """A network-style response body: ``read(n)`` and ``close()`` only.

    No seeking and no line iteration, like a real S3 ``StreamingBody`` or a
    GCS blob reader, so a code path that needs the whole object up front
    fails here instead of passing by accident. ``bytes_read`` and
    ``max_read`` record how the body was consumed.
    """

    def __init__(self, data: bytes) -> None:
        self._buf = io.BytesIO(data)
        self.size = len(data)
        self.bytes_read = 0
        self.max_read = 0
        self.closed = False

    def read(self, amt: int | None = None) -> bytes:
        if amt is None or amt < 0:
            amt = self.size
        chunk = self._buf.read(amt)
        self.bytes_read += len(chunk)
        self.max_read = max(self.max_read, len(chunk))
        return chunk

    def close(self) -> None:
        self.closed = True


class FakeS3:
    """Just enough of a boto3 S3 client: get/put objects and paginated listing."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.client_kwargs: list[dict[str, Any]] = []
        self.bodies: list[StreamingBody] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        body = StreamingBody(self.objects[(Bucket, Key)])
        self.bodies.append(body)
        return {"Body": body}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> dict[str, Any]:  # noqa: N803
        self.objects[(Bucket, Key)] = Body
        return {}

    def get_paginator(self, name: str) -> "FakeS3":
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket: str, Prefix: str):  # noqa: N803
        keys = sorted(k for b, k in self.objects if b == Bucket and k.startswith(Prefix))
        # Two keys per page, so callers must walk every page.
        for i in range(0, len(keys), 2):
            yield {"Contents": [{"Key": k} for k in keys[i : i + 2]]}
        if not keys:
            yield {}


class FakeKinesis:
    """Just enough of a boto3 Kinesis client: several shards, resharding, read and write.

    ``shards`` maps shard id to its records. A shard listed in ``closed`` ends
    once fully read (no next iterator); any other shard stays open and
    returns empty reads when caught up. ``children`` maps a parent shard to
    child shards that are reported (via ``ChildShards``) when the parent
    closes, but are not returned by ``list_shards``, as after a reshard that
    happens mid-read.
    """

    def __init__(self) -> None:
        self.shards: dict[str, list[bytes]] = {}
        self.closed: set[str] = set()
        self.children: dict[str, list[str]] = {}
        self.listed: list[str] | None = None
        self.put_calls: list[dict[str, Any]] = []
        self.client_kwargs: list[dict[str, Any]] = []
        self.iterator_requests: list[dict[str, Any]] = []
        self.list_calls: list[dict[str, Any]] = []

    @property
    def records(self) -> list[bytes]:
        return self.shards.get("shardId-0", [])

    @records.setter
    def records(self, value: list[bytes]) -> None:
        # The single-shard form used by most tests: one closed shard.
        self.shards = {"shardId-0": value}
        self.closed = {"shardId-0"}

    def list_shards(self, **kwargs: Any) -> dict[str, Any]:
        self.list_calls.append(kwargs)
        if "NextToken" in kwargs and "StreamName" in kwargs:
            # The real API rejects this combination.
            raise ValueError("StreamName and NextToken cannot be combined")
        ids = self.listed if self.listed is not None else sorted(self.shards)
        start = int(kwargs.get("NextToken", 0))
        # One shard per page, so callers must follow NextToken.
        page = {"Shards": [{"ShardId": shard_id} for shard_id in ids[start : start + 1]]}
        if start + 1 < len(ids):
            page["NextToken"] = str(start + 1)
        return page

    def get_shard_iterator(self, **kwargs: Any) -> dict[str, Any]:
        self.iterator_requests.append(kwargs)
        return {"ShardIterator": f"{kwargs['ShardId']}:0"}

    def get_records(self, *, ShardIterator: str, Limit: int) -> dict[str, Any]:  # noqa: N803
        shard_id, _, position = ShardIterator.rpartition(":")
        start = int(position)
        records = self.shards[shard_id]
        chunk = records[start : start + Limit]
        end = start + len(chunk)

        response: dict[str, Any] = {"Records": [{"Data": data} for data in chunk]}
        if shard_id in self.closed and end >= len(records):
            # A closed shard returns no next iterator once fully read.
            response["NextShardIterator"] = None
            if shard_id in self.children:
                response["ChildShards"] = [{"ShardId": c} for c in self.children[shard_id]]
        else:
            response["NextShardIterator"] = f"{shard_id}:{end}"
        return response

    def put_record(self, *, StreamName: str, Data: bytes, PartitionKey: str) -> dict[str, Any]:  # noqa: N803
        self.put_calls.append({"StreamName": StreamName, "Data": Data, "PartitionKey": PartitionKey})
        return {}


@pytest.fixture
def fake_boto3(monkeypatch):
    """Install a fake ``boto3`` whose ``client("s3" | "kinesis")`` returns shared fakes."""

    s3 = FakeS3()
    kinesis = FakeKinesis()
    services = {"s3": s3, "kinesis": kinesis}

    module = types.ModuleType("boto3")

    def client(service: str, **kwargs: Any) -> Any:
        services[service].client_kwargs.append(kwargs)
        return services[service]

    module.client = client  # type: ignore[attr-defined]
    module.s3 = s3  # type: ignore[attr-defined]
    module.kinesis = kinesis  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "boto3", module)
    return module


class _FakeBlob:
    def __init__(self, store: dict[tuple[str, str], bytes], bucket: str, name: str) -> None:
        self._store = store
        self._bucket = bucket
        self.name = name

    def download_as_bytes(self) -> bytes:
        return self._store[(self._bucket, self.name)]

    def open(self, mode: str = "r") -> StreamingBody:
        assert mode == "rb"
        body = StreamingBody(self._store[(self._bucket, self.name)])
        FakeGCSClient.opened.append(body)
        return body

    def upload_from_string(self, payload: bytes) -> None:
        self._store[(self._bucket, self.name)] = payload


class _FakeBucket:
    def __init__(self, store: dict[tuple[str, str], bytes], name: str) -> None:
        self._store = store
        self._name = name

    def blob(self, key: str) -> _FakeBlob:
        return _FakeBlob(self._store, self._name, key)


class FakeGCSClient:
    store: dict[tuple[str, str], bytes] = {}
    init_kwargs: list[dict[str, Any]] = []
    opened: list[StreamingBody] = []

    def __init__(self, **kwargs: Any) -> None:
        type(self).init_kwargs.append(kwargs)

    def bucket(self, name: str) -> _FakeBucket:
        return _FakeBucket(self.store, name)

    def list_blobs(self, bucket: str, prefix: str = "") -> list[_FakeBlob]:
        return [
            _FakeBlob(self.store, b, k)
            for (b, k) in sorted(self.store)
            if b == bucket and k.startswith(prefix)
        ]


@pytest.fixture
def fake_gcs(monkeypatch):
    """Install a fake ``google.cloud.storage`` backed by an in-memory dict."""

    client_cls = type("Client", (FakeGCSClient,), {"store": {}, "init_kwargs": []})
    FakeGCSClient.opened = []
    client_cls.opened = FakeGCSClient.opened

    storage = types.ModuleType("google.cloud.storage")
    storage.Client = client_cls  # type: ignore[attr-defined]
    cloud = types.ModuleType("google.cloud")
    cloud.storage = storage  # type: ignore[attr-defined]
    google = types.ModuleType("google")
    google.cloud = cloud  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage)
    return client_cls


class FakeKafkaMessage:
    def __init__(self, value: bytes, error: Any = None) -> None:
        self._value = value
        self._error = error

    def value(self) -> bytes:
        return self._value

    def error(self) -> Any:
        return self._error


@pytest.fixture
def fake_kafka(monkeypatch):
    """Install a fake ``confluent_kafka`` with an in-memory topic per name."""

    topics: dict[str, list[FakeKafkaMessage]] = {}
    state: dict[str, Any] = {"consumer_config": None, "producer_config": None, "closed": False, "flushed": False}

    class Consumer:
        def __init__(self, config: dict[str, Any]) -> None:
            state["consumer_config"] = config
            self._topic: str | None = None

        def subscribe(self, names: list[str]) -> None:
            self._topic = names[0]

        def poll(self, timeout: float) -> FakeKafkaMessage | None:
            pending = topics.setdefault(self._topic or "", [])
            return pending.pop(0) if pending else None

        def close(self) -> None:
            state["closed"] = True

    class Producer:
        def __init__(self, config: dict[str, Any]) -> None:
            state["producer_config"] = config

        def produce(self, topic: str, value: bytes) -> None:
            topics.setdefault(topic, []).append(FakeKafkaMessage(value))

        def flush(self) -> None:
            state["flushed"] = True

    module = types.ModuleType("confluent_kafka")
    module.Consumer = Consumer  # type: ignore[attr-defined]
    module.Producer = Producer  # type: ignore[attr-defined]
    module.topics = topics  # type: ignore[attr-defined]
    module.state = state  # type: ignore[attr-defined]
    module.Message = FakeKafkaMessage  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    return module


def _block_import(monkeypatch, *names: str) -> None:
    # A None entry in sys.modules makes ``import name`` raise ImportError.
    for name in names:
        monkeypatch.setitem(sys.modules, name, None)


@pytest.fixture
def no_boto3(monkeypatch):
    _block_import(monkeypatch, "boto3")


@pytest.fixture
def no_gcs(monkeypatch):
    _block_import(monkeypatch, "google.cloud.storage", "google.cloud", "google")


@pytest.fixture
def no_kafka(monkeypatch):
    _block_import(monkeypatch, "confluent_kafka")


@pytest.fixture
def no_websockets(monkeypatch):
    _block_import(monkeypatch, "websockets")

import pandas as pd
import pytest

from sushitruck import tobiko


@pytest.fixture
def df():
    return pd.DataFrame({"price": [1.0, 2.0, 3.0], "date": ["2024-01-01", "2024-01-01", "2024-01-02"]})


def test_send_local_parquet(tmp_path, df):
    target = str(tmp_path / "out.parquet")
    result = tobiko.send(df, target, format="parquet")

    assert result.targets_written == [target]
    assert result.rows_written == 3
    assert (tmp_path / "out.parquet").exists()


def test_send_local_csv(tmp_path, df):
    target = str(tmp_path / "out.csv")
    tobiko.send(df, target, format="csv")

    written = pd.read_csv(target)
    assert len(written) == 3


def test_send_multiple_targets(tmp_path, df):
    t1 = str(tmp_path / "a.csv")
    t2 = str(tmp_path / "b.csv")
    result = tobiko.send(df, [t1, t2], format="csv")

    assert set(result.targets_written) == {t1, t2}


def test_send_partitioned_output(tmp_path, df):
    target = str(tmp_path / "data")
    result = tobiko.send(df, target, format="csv", partition_by="date")

    assert (tmp_path / "data" / "date=2024-01-01" / "part.csv").exists()
    assert (tmp_path / "data" / "date=2024-01-02" / "part.csv").exists()
    assert result.rows_written == 3


def test_send_append_csv(tmp_path, df):
    target = str(tmp_path / "out.csv")
    tobiko.send(df, target, format="csv")
    tobiko.send(df, target, format="csv", mode="append")

    written = pd.read_csv(target)
    assert len(written) == 6


def test_send_append_rejects_parquet(tmp_path, df):
    target = str(tmp_path / "out.parquet")
    tobiko.send(df, target, format="parquet")

    with pytest.raises(ValueError):
        tobiko.send(df, target, format="parquet", mode="append")


def test_send_rejects_bad_mode(tmp_path, df):
    with pytest.raises(ValueError):
        tobiko.send(df, str(tmp_path / "out.csv"), mode="bogus")


def test_send_rejects_bad_format(tmp_path, df):
    with pytest.raises(ValueError):
        tobiko.send(df, str(tmp_path / "out.xyz"), format="xyz")


def test_publish_uses_custom_serializer():
    df = pd.DataFrame({"a": [1, 2]})
    sent = []

    def fake_kafka_publish(rows, target, config):
        sent.extend(rows)
        return len(rows)

    import sushitruck.tobiko as tobiko_module

    original = tobiko_module._publish_kafka
    tobiko_module._publish_kafka = fake_kafka_publish
    try:
        count = tobiko.publish(df, "kafka://topic", config={}, serializer=lambda row: str(row).encode())
    finally:
        tobiko_module._publish_kafka = original

    assert count == 2
    assert sent == [str({"a": 1}).encode(), str({"a": 2}).encode()]


def test_publish_rejects_unsupported_target():
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError):
        tobiko.publish(df, "sqs://queue", config={})


# --- object store and queue backends (in-memory fakes from conftest.py) ---


def test_send_s3_parquet(fake_boto3, df):
    import io

    result = tobiko.send(
        df, "s3://bucket/out/prices.parquet", storage_options={"region_name": "us-east-1"}
    )

    payload = fake_boto3.s3.objects[("bucket", "out/prices.parquet")]
    assert pd.read_parquet(io.BytesIO(payload))["price"].tolist() == [1.0, 2.0, 3.0]
    assert result.targets_written == ["s3://bucket/out/prices.parquet"]
    assert result.bytes_written == len(payload)
    assert fake_boto3.s3.client_kwargs == [{"region_name": "us-east-1"}]


def test_send_s3_partitioned(fake_boto3, df):
    result = tobiko.send(df, "s3://bucket/out/", format="csv", partition_by="date")

    assert ("bucket", "out/date=2024-01-01/part.csv") in fake_boto3.s3.objects
    assert ("bucket", "out/date=2024-01-02/part.csv") in fake_boto3.s3.objects
    assert result.rows_written == 3


def test_send_gcs_jsonl(fake_gcs, df):
    tobiko.send(df, "gs://bucket/out.jsonl", format="jsonl")

    lines = fake_gcs.store[("bucket", "out.jsonl")].decode().strip().splitlines()
    assert len(lines) == 3


def test_send_local_json(tmp_path, df):
    target = tmp_path / "out.json"
    tobiko.send(df, str(target), format="json")

    assert len(pd.read_json(target)) == 3


def test_send_append_jsonl(tmp_path, df):
    target = str(tmp_path / "out.jsonl")
    tobiko.send(df, target, format="jsonl")
    tobiko.send(df, target, format="jsonl", mode="append")

    assert len(pd.read_json(target, lines=True)) == 6


def test_send_without_cloud_extra_raises_import_error(no_boto3, no_gcs, df):
    with pytest.raises(ImportError, match=r"sushitruck\[cloud\]"):
        tobiko.send(df, "s3://bucket/out.csv", format="csv")
    with pytest.raises(ImportError, match=r"sushitruck\[cloud\]"):
        tobiko.send(df, "gs://bucket/out.csv", format="csv")


def test_publish_kafka(fake_kafka):
    df = pd.DataFrame({"a": [1, 2, 3]})
    count = tobiko.publish(df, "kafka://trades", config={"bootstrap.servers": "localhost:9092"})

    assert count == 3
    sent = [m.value() for m in fake_kafka.topics["trades"]]
    assert sent == [b'{"a": 1}', b'{"a": 2}', b'{"a": 3}']
    assert fake_kafka.state["flushed"] is True
    assert fake_kafka.state["producer_config"] == {"bootstrap.servers": "localhost:9092"}


def test_publish_kinesis_uses_partition_key(fake_boto3):
    df = pd.DataFrame({"a": [1, 2]})
    count = tobiko.publish(
        df, "kinesis://events", config={"region_name": "us-east-1", "partition_key": "pk"}
    )

    assert count == 2
    calls = fake_boto3.kinesis.put_calls
    assert [c["StreamName"] for c in calls] == ["events", "events"]
    assert all(c["PartitionKey"] == "pk" for c in calls)
    # partition_key is a tobiko option, not a boto3 client kwarg.
    assert fake_boto3.kinesis.client_kwargs == [{"region_name": "us-east-1"}]


def test_publish_without_extras_raises_import_error(no_kafka, no_boto3):
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ImportError, match=r"sushitruck\[kafka\]"):
        tobiko.publish(df, "kafka://t", config={})
    with pytest.raises(ImportError, match=r"sushitruck\[kinesis\]"):
        tobiko.publish(df, "kinesis://s", config={})


def test_send_append_rejects_object_store_targets(fake_boto3, df):
    with pytest.raises(ValueError, match="local targets"):
        tobiko.send(df, "s3://bucket/out.csv", format="csv", mode="append")
    assert fake_boto3.s3.objects == {}

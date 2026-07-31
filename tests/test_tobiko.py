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

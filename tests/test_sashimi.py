import pandas as pd
import pytest

from sushitruck import sashimi


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / "prices.csv"
    path.write_text("price,symbol\n1.0,AAA\n2.0,BBB\n3.0,CCC\n")
    return path


@pytest.fixture
def jsonl_file(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text('{"a": 1}\n{"a": 2}\n{"a": 3}\n')
    return path


def test_read_csv_full(csv_file):
    df = sashimi.read(csv_file)
    assert list(df.columns) == ["price", "symbol"]
    assert len(df) == 3


def test_read_csv_chunked(csv_file):
    chunks = list(sashimi.read(csv_file, chunksize=2))
    assert len(chunks) == 2
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 1


def test_read_jsonl_chunked(jsonl_file):
    chunks = list(sashimi.read(jsonl_file, chunksize=2))
    assert sum(len(c) for c in chunks) == 3


def test_read_auto_detects_format_from_extension(csv_file):
    df = sashimi.read(str(csv_file))
    assert isinstance(df, pd.DataFrame)


def test_read_raises_on_unknown_extension(tmp_path):
    path = tmp_path / "mystery.xyz"
    path.write_text("data")
    with pytest.raises(ValueError):
        sashimi.read(path)


def test_read_rejects_chunksize_for_parquet(tmp_path):
    df = pd.DataFrame({"a": [1, 2]})
    path = tmp_path / "data.parquet"
    df.to_parquet(path)

    with pytest.raises(ValueError):
        sashimi.read(path, chunksize=1)


def test_read_applies_schema(csv_file):
    schema = {
        "price": {"dtype": float, "nullable": False},
        "symbol": {"dtype": str, "nullable": False},
    }
    df = sashimi.read(csv_file, schema=schema)
    assert df["price"].dtype == float


def test_read_kwargs_forwarded(csv_file):
    df = sashimi.read(csv_file, usecols=["price"])
    assert list(df.columns) == ["price"]


def test_list_objects_local(tmp_path):
    (tmp_path / "a.csv").write_text("x")
    (tmp_path / "b.csv").write_text("x")
    (tmp_path / "c.json").write_text("{}")

    results = sashimi.list_objects(str(tmp_path), pattern="*.csv")
    assert len(results) == 2
    assert all(r.endswith(".csv") for r in results)


def test_read_rejects_unsupported_storage(csv_file):
    with pytest.raises(ValueError):
        sashimi.read(csv_file, storage="ftp")


def test_list_objects_rejects_unsupported_storage(tmp_path):
    with pytest.raises(ValueError):
        sashimi.list_objects(str(tmp_path), storage="ftp")


# --- object store backends (in-memory fakes from conftest.py) ---


def test_read_s3_csv(fake_boto3):
    fake_boto3.s3.objects[("bucket", "data/prices.csv")] = b"price,symbol\n1.0,AAA\n2.0,BBB\n"

    df = sashimi.read(
        "s3://bucket/data/prices.csv", storage="s3", storage_options={"region_name": "us-east-1"}
    )

    assert df["symbol"].tolist() == ["AAA", "BBB"]
    assert fake_boto3.s3.client_kwargs == [{"region_name": "us-east-1"}]


def test_read_s3_parquet(fake_boto3):
    import io

    buf = io.BytesIO()
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(buf)
    fake_boto3.s3.objects[("bucket", "t.parquet")] = buf.getvalue()

    df = sashimi.read("s3://bucket/t.parquet", storage="s3")
    assert df["a"].tolist() == [1, 2, 3]


def test_read_s3_chunked_jsonl(fake_boto3):
    fake_boto3.s3.objects[("bucket", "e.jsonl")] = b'{"a": 1}\n{"a": 2}\n{"a": 3}\n'

    chunks = list(sashimi.read("s3://bucket/e.jsonl", storage="s3", chunksize=2))
    assert [len(c) for c in chunks] == [2, 1]


def test_read_gcs_json(fake_gcs):
    fake_gcs.store[("bucket", "dir/rows.json")] = b'[{"a": 1}, {"a": 2}]'

    df = sashimi.read("gs://bucket/dir/rows.json", storage="gcs", storage_options={"project": "p"})

    assert df["a"].tolist() == [1, 2]
    assert fake_gcs.init_kwargs == [{"project": "p"}]


def test_list_objects_s3_walks_all_pages_and_filters(fake_boto3):
    for key in ("data/a.csv", "data/b.csv", "data/c.json", "data/d.csv", "other/e.csv"):
        fake_boto3.s3.objects[("bucket", key)] = b"x"

    results = sashimi.list_objects("s3://bucket/data/", storage="s3", pattern="*.csv")

    assert results == ["s3://bucket/data/a.csv", "s3://bucket/data/b.csv", "s3://bucket/data/d.csv"]


def test_list_objects_s3_empty_prefix(fake_boto3):
    assert sashimi.list_objects("s3://bucket/none/", storage="s3") == []


def test_list_objects_gcs_filters(fake_gcs):
    for key in ("data/a.csv", "data/b.parquet", "elsewhere/c.csv"):
        fake_gcs.store[("bucket", key)] = b"x"

    results = sashimi.list_objects("gs://bucket/data/", storage="gcs", pattern="*.csv")
    assert results == ["gs://bucket/data/a.csv"]


def test_read_s3_without_extra_raises_helpful_import_error(no_boto3):
    with pytest.raises(ImportError, match=r"sushitruck\[cloud\]"):
        sashimi.read("s3://bucket/x.csv", storage="s3")


def test_read_gcs_without_extra_raises_helpful_import_error(no_gcs):
    with pytest.raises(ImportError, match=r"sushitruck\[cloud\]"):
        sashimi.read("gs://bucket/x.csv", storage="gcs")


def test_list_objects_without_extras_raises_import_error(no_boto3, no_gcs):
    with pytest.raises(ImportError):
        sashimi.list_objects("s3://bucket/", storage="s3")
    with pytest.raises(ImportError):
        sashimi.list_objects("gs://bucket/", storage="gcs")


def test_read_explicit_format_overrides_extension(tmp_path):
    path = tmp_path / "data.dat"
    path.write_text("a,b\n1,2\n")

    df = sashimi.read(path, format="csv")
    assert df.to_dict("records") == [{"a": 1, "b": 2}]


def test_chunked_read_applies_schema_per_chunk(csv_file):
    schema = {"price": {"dtype": float}, "symbol": {"dtype": str}}
    chunks = list(sashimi.read(csv_file, chunksize=2, schema=schema))

    assert [len(c) for c in chunks] == [2, 1]
    assert all(c["price"].dtype == float for c in chunks)


def test_read_closes_local_file(csv_file):
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error", ResourceWarning)
        sashimi.read(csv_file)
        list(sashimi.read(csv_file, chunksize=1))

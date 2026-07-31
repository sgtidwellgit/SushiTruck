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

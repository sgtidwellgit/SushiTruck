import pandas as pd
import pytest
import responses

from sushitruck import temaki


@pytest.fixture
def two_csv_files(tmp_path):
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text("x\n1\n2\n")
    b.write_text("x\n3\n4\n")
    return a, b


def test_run_merges_multiple_files(two_csv_files):
    a, b = two_csv_files
    job = temaki.TemakiJob().add_file(a).add_file(b)
    result = job.run()

    assert result.sources_processed == 2
    assert result.total_rows == 4
    assert sorted(result.df["x"].tolist()) == [1, 2, 3, 4]
    assert result.sources_failed == []


def test_run_parallel_workers(two_csv_files):
    a, b = two_csv_files
    job = temaki.TemakiJob(workers=2).add_file(a).add_file(b)
    result = job.run()

    assert result.sources_processed == 2
    assert result.total_rows == 4


def test_add_glob_matches_files(tmp_path, two_csv_files):
    job = temaki.TemakiJob().add_glob(str(tmp_path / "*.csv"))
    result = job.run()

    assert result.sources_processed == 2
    assert result.total_rows == 4


def test_run_applies_schema(two_csv_files):
    a, b = two_csv_files
    schema = {"x": {"dtype": float, "nullable": False}}
    job = temaki.TemakiJob(schema=schema).add_file(a).add_file(b)
    result = job.run()

    assert result.df["x"].dtype == float


def test_run_on_error_raise_propagates(tmp_path):
    job = temaki.TemakiJob(on_error="raise").add_file(tmp_path / "missing.csv")
    with pytest.raises(Exception):
        job.run()


def test_run_on_error_skip_records_failure(tmp_path, two_csv_files):
    a, _ = two_csv_files
    job = temaki.TemakiJob(on_error="skip").add_file(a).add_file(tmp_path / "missing.csv")
    result = job.run()

    assert result.sources_processed == 1
    assert len(result.sources_failed) == 1
    assert result.total_rows == 2


def test_run_on_error_warn_emits_warning(tmp_path, two_csv_files):
    a, _ = two_csv_files
    job = temaki.TemakiJob(on_error="warn").add_file(a).add_file(tmp_path / "missing.csv")

    with pytest.warns(UserWarning):
        result = job.run()

    assert len(result.sources_failed) == 1


def test_rejects_invalid_on_error():
    with pytest.raises(ValueError):
        temaki.TemakiJob(on_error="bogus")


def test_stream_yields_per_source(two_csv_files):
    a, b = two_csv_files
    job = temaki.TemakiJob().add_file(a).add_file(b)
    batches = list(job.stream())

    assert len(batches) == 2
    assert sum(len(b) for b in batches) == 4


def test_stream_yields_per_chunk(two_csv_files):
    a, _ = two_csv_files
    job = temaki.TemakiJob().add_file(a, chunksize=1)
    batches = list(job.stream())

    assert len(batches) == 2
    assert all(len(b) == 1 for b in batches)


@responses.activate
def test_add_api_forwards_pagination_options():
    from sushitruck import maki

    responses.add(responses.GET, "https://api.example.com/rows", json={"data": [{"x": 1}, {"x": 2}]})
    responses.add(responses.GET, "https://api.example.com/rows", json={"data": [{"x": 3}]})

    client = maki.MakiClient("https://api.example.com")
    job = temaki.TemakiJob().add_api(
        client, "/rows", paginate=True, results_key="data", pagination="offset", page_size=2
    )
    result = job.run()

    assert result.df["x"].tolist() == [1, 2, 3]
    assert responses.calls[1].request.params["offset"] == "2"


def test_add_glob_s3(fake_boto3):
    fake_boto3.s3.objects[("bucket", "prices/a.csv")] = b"x\n1\n"
    fake_boto3.s3.objects[("bucket", "prices/b.csv")] = b"x\n2\n"
    fake_boto3.s3.objects[("bucket", "prices/skip.json")] = b"[]"

    result = temaki.TemakiJob().add_glob("s3://bucket/prices/*.csv", storage="s3").run()

    assert result.sources_processed == 2
    assert sorted(result.df["x"].tolist()) == [1, 2]


def test_parallel_run_records_failures(tmp_path, two_csv_files):
    a, _ = two_csv_files
    job = temaki.TemakiJob(workers=2, on_error="skip").add_file(a).add_file(tmp_path / "missing.csv")
    result = job.run()

    assert result.sources_processed == 1
    assert len(result.sources_failed) == 1


def test_run_with_no_sources_returns_empty_frame():
    result = temaki.TemakiJob().run()

    assert result.df.empty
    assert result.sources_processed == 0


def test_stream_on_error_warn_continues(tmp_path, two_csv_files):
    a, _ = two_csv_files
    job = temaki.TemakiJob(on_error="warn").add_file(tmp_path / "missing.csv").add_file(a)

    with pytest.warns(UserWarning):
        batches = list(job.stream())

    assert len(batches) == 1


def test_stream_on_error_raise_propagates(tmp_path):
    job = temaki.TemakiJob().add_file(tmp_path / "missing.csv")
    with pytest.raises(Exception):
        list(job.stream())

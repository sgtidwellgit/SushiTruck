# sushitruck

**SushiTruck** is the streaming ingestion and API connector toolkit for the food truck fleet — the conveyor belt of the data pipeline.

> Just like conveyor belt sushi: data flows continuously past, and you consume exactly what you need, at your own pace.

```bash
pip install sushitruck
```

SushiTruck handles the *intake* side of a data pipeline: connecting to external APIs, streaming sources, and files/object stores, then normalizing everything into clean `pd.DataFrame`s ready for [ThaiTruck](https://pypi.org/project/thaitruck/) (cleaning/merging) and [RamenTruck](https://pypi.org/project/ramentruck/) (ML/AI).

```
External World → SushiTruck (ingest, connect, normalize) →
                 ThaiTruck (clean, merge, profile) →
                 RamenTruck (train, tune, explain)
```

## The Belt

| Module | Purpose |
|---|---|
| `maki` | REST API client — auth, pagination, rate limiting, retry |
| `sashimi` | Local file / S3 / GCS reader — CSV, JSON, JSONL, Parquet, chunked or full |
| `wasabi` | Nested JSON flattening + schema enforcement |
| `gari` | Rate limiting, retry with backoff, and circuit breaking |
| `temaki` | Batch ingestion coordinator — merges files, globs, and API calls into one DataFrame |
| `tobiko` | Output router — write to local/S3/GCS, or publish to Kafka/Kinesis |
| `nigiri` | Streaming source adapters — webhook, WebSocket, Kafka, Kinesis |

## Quick examples

**Pull data from a REST API:**

```python
from sushitruck import maki

client = maki.MakiClient(
    "https://api.example.com/v2",
    auth={"type": "bearer", "token": "my-token"},
    rate_limit=10.0,
    retries=3,
)

df = client.fetch("/transactions", paginate=True, results_key="data.transactions")
```

**Read a large local or S3 file in memory-safe chunks:**

```python
from sushitruck import sashimi

for chunk_df in sashimi.read("big_trades.csv", chunksize=50_000):
    process(chunk_df)
```

**Flatten and normalize a raw payload:**

```python
from sushitruck import wasabi

df = wasabi.flatten([{"price": "142.5", "meta": {"source": "bloomberg"}}])
schema = {
    "price": {"dtype": float, "nullable": False},
    "meta_source": {"dtype": str, "nullable": True, "rename": "source"},
}
clean = wasabi.normalize(df, schema)
```

**Coordinate a multi-source batch job:**

```python
from sushitruck import temaki

job = (
    temaki.TemakiJob(workers=4, on_error="warn")
    .add_glob("s3://my-bucket/prices/*.parquet", storage="s3")
    .add_api(client, "/supplemental", paginate=True, results_key="data")
    .add_file("local_overrides.csv")
)
result = job.run()
print(result.total_rows, result.sources_failed)
```

**Route the output:**

```python
from sushitruck import tobiko

tobiko.send(clean_df, "s3://my-bucket/processed/trades/", format="parquet", partition_by="date")
```

## Optional extras

The core install requires only `pandas`, `numpy`, and `requests`. Everything else is opt-in:

```bash
pip install sushitruck              # core: REST APIs, local files, webhook/websocket streaming
pip install sushitruck[kafka]       # + Kafka streaming
pip install sushitruck[kinesis]     # + AWS Kinesis streaming
pip install sushitruck[cloud]       # + S3 and GCS object store
pip install sushitruck[websocket]   # + WebSocket streaming
pip install sushitruck[all]         # everything
```

## Fleet

SushiTruck, ThaiTruck, and RamenTruck are fully independent packages — none imports another. They compose at the application layer through `pd.DataFrame`.

- **SushiTruck** — streaming ingestion & API connectors *(this package)*
- **[ThaiTruck](https://pypi.org/project/thaitruck/)** — batch DataFrame cleaning & processing
- **[RamenTruck](https://pypi.org/project/ramentruck/)** — ML/AI toolkit

See `PROJECT.md` for full module design details and the roadmap.

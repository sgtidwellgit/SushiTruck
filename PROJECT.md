# SushiTruck — Project Document

> **Current version:** 0.2.2 | **Python:** ≥ 3.9 (CI: 3.9–3.13) | **Status:** Beta — all modules implemented

---

## Table of Contents

1. [What SushiTruck Is](#what-sushitruck-is)
2. [The Philosophy](#the-philosophy)
3. [Current State](#current-state)
4. [Repository Layout](#repository-layout)
5. [Dependency & Extras Architecture](#dependency--extras-architecture)
6. [The Belt — All Planned Modules](#the-belt--all-planned-modules)
   - [`nigiri` — Streaming Source Adapters](#nigiri--streaming-source-adapters)
   - [`maki` — REST API Client](#maki--rest-api-client)
   - [`sashimi` — Raw File & Object Store Reader](#sashimi--raw-file--object-store-reader)
   - [`wasabi` — JSON Flattener & Schema Normalizer](#wasabi--json-flattener--schema-normalizer)
   - [`gari` — Rate Limiter & Retry](#gari--rate-limiter--retry)
   - [`temaki` — Batch Ingestion Coordinator](#temaki--batch-ingestion-coordinator)
   - [`tobiko` — Output Router](#tobiko--output-router)
7. [Design Principles](#design-principles)
8. [Inter-Op with the Fleet](#inter-op-with-the-fleet)
9. [The Food Truck Fleet](#the-food-truck-fleet)
10. [Build & Publish Plan](#build--publish-plan)

---

## What SushiTruck Is

SushiTruck is the **streaming ingestion and API connector toolkit** for the food truck fleet. Where ThaiTruck cleans and merges DataFrames and RamenTruck trains models on them, SushiTruck handles everything *before* that — connecting to external data sources, consuming live streams, calling APIs, reading large files, and normalizing raw payloads into clean DataFrames ready for the rest of the pipeline.

The fleet pipeline in full:

```
External World → SushiTruck (ingest, connect, normalize) →
                 ThaiTruck (clean, merge, profile) →
                 RamenTruck (train, tune, explain)
```

```bash
pip install sushitruck
```

SushiTruck targets three ingestion patterns:

1. **Streaming** — continuous data from Kafka, Kinesis, WebSocket, or webhook endpoints
2. **API pull** — REST APIs with pagination, authentication, rate limiting, and retry
3. **File / object store** — large local files, S3, or GCS objects read in memory-safe chunks

All three converge on the same output: a `pd.DataFrame` (or a generator of them) that slots directly into ThaiTruck's `orange_chicken` → `fried_rice` pipeline.

---

## The Philosophy

**Why conveyor belt sushi?** Conveyor belt sushi is continuous, self-pacing, and pull-based — plates flow past and you take exactly what you need, when you need it. That is the right mental model for data ingestion: data arrives continuously from the outside world; SushiTruck lets you consume it at your own cadence without drowning in it.

The guiding design values:

- **Generator-first** — streaming sources yield DataFrames lazily; don't load what you don't need
- **Batteries-included connectors** — auth, pagination, retry, and rate limiting are built-in, not left to the caller
- **Normalize at the boundary** — raw payloads get flattened and schema-checked before they leave SushiTruck; downstream consumers receive clean DataFrames
- **Optional extras, not mandatory bloat** — Kafka, Kinesis, and cloud SDKs are heavy; they are opt-in
- **Consistent with the fleet** — same `src/` layout, same no-mutation conventions, same typed result containers as ThaiTruck and RamenTruck

---

## Current State

| Item | Status |
|---|---|
| PyPI name `sushitruck` | Secured (2026-06-15) |
| Version | 0.2.2 |
| `src/sushitruck/__init__.py` | Exists — `__version__ = "0.2.2"`, exports all seven modules + `MakiClient`, `TemakiJob`, `TemakiResult`, `TobikoResult` |
| `pyproject.toml` | Exists — hatchling build, Python ≥ 3.9, MIT license, optional extras declared |
| `README.md` | Exists — install, usage examples, fleet context |
| All planned modules | Implemented: `gari`, `wasabi`, `maki`, `sashimi`, `tobiko`, `temaki`, `nigiri` |
| Tests | 129 passing tests, 96% line coverage (`pytest`); cloud and queue backends tested offline against in-memory stand-ins in `tests/conftest.py` |
| CI | `.github/workflows/tests.yml` — Python 3.9–3.13 on Linux, plus Windows and macOS; builds and checks distributions |
| `LICENSE` | MIT |
| Optional extras in `pyproject.toml` | Declared: `kafka`, `kinesis`, `cloud`, `websocket`, `all`, `dev` |
| `py.typed` marker | Present (PEP 561) |
| `CHANGELOG.md` | Present |

All seven modules are implemented and tested. `nigiri`'s WebSocket support ships behind the `[websocket]` extra (rather than core, as originally sketched below) for consistency with the "optional extras, not mandatory bloat" principle.

---

## Repository Layout

```
SushiTruck/
├── pyproject.toml              # build config, metadata, optional extras
├── README.md                   # user-facing install and usage guide
├── PROJECT.md                  # this file — comprehensive project state + roadmap
├── CHANGELOG.md                # per-version release notes
├── LICENSE                     # MIT
├── .github/workflows/tests.yml # CI: test matrix + package build check
├── src/
│   └── sushitruck/
│       ├── __init__.py         # public re-exports + __version__
│       ├── nigiri.py           # streaming source adapters (Kafka, Kinesis, WebSocket)
│       ├── maki.py             # REST API client with pagination, auth, retry
│       ├── sashimi.py          # raw file / object store reader (local, S3, GCS)
│       ├── wasabi.py           # JSON flattener + schema normalizer
│       ├── gari.py             # rate limiter, retry logic, circuit breaker
│       ├── temaki.py            # batch ingestion coordinator
│       ├── tobiko.py           # output router (local / S3 / GCS files, Kafka / Kinesis queues)
│       └── results.py          # TemakiResult, TobikoResult
└── tests/
    ├── conftest.py             # in-memory stand-ins for boto3, google-cloud-storage, confluent-kafka
    ├── test_nigiri.py
    ├── test_maki.py
    ├── test_sashimi.py
    ├── test_wasabi.py
    ├── test_gari.py
    ├── test_temaki.py
    └── test_tobiko.py
```

---

## Dependency & Extras Architecture

The core package should require **only `requests`, `pandas`, and `numpy`** — no Kafka clients, no AWS SDK, no cloud storage SDKs. Every heavy dependency is gated behind an optional extra.

```toml
[project]
dependencies = [
    "pandas>=1.5",
    "numpy>=1.23",
    "requests>=2.28",
]

[project.optional-dependencies]
dev        = ["pytest>=7", "pytest-cov", "responses>=0.23", "build", "twine"]
kafka      = ["confluent-kafka>=2.0"]
kinesis    = ["boto3>=1.26"]
cloud      = ["boto3>=1.26", "google-cloud-storage>=2.0"]
websocket  = ["websockets>=14.0"]   # connect(additional_headers=...) needs 14+
all        = ["sushitruck[kafka,kinesis,cloud,websocket]"]
```

Install patterns:

```bash
pip install sushitruck                    # core (REST APIs + local files)
pip install sushitruck[kafka]             # + Kafka streaming
pip install sushitruck[kinesis]           # + AWS Kinesis streaming
pip install sushitruck[cloud]             # + S3 and GCS object store
pip install sushitruck[websocket]         # + WebSocket streaming
pip install sushitruck[all]              # everything
```

**Import guard pattern** — every module that wraps an optional dependency must guard cleanly:

```python
# nigiri.py (Kafka path)
try:
    from confluent_kafka import Consumer
except ImportError as e:
    raise ImportError(
        "Kafka support requires confluent-kafka. "
        "Install it with: pip install sushitruck[kafka]"
    ) from e
```

---

## The Belt — All Planned Modules

---

### `nigiri` — Streaming Source Adapters

**File:** `src/sushitruck/nigiri.py`  
**Core:** webhook support  
**Optional extras:** `[websocket]` for WebSocket, `[kafka]` for Kafka, `[kinesis]` for Kinesis

Named for nigiri — one clean piece at a time, precisely placed. `nigiri` connects to a continuous streaming source and yields micro-batched DataFrames. The caller consumes them at their own pace with a `for` loop or `next()`.

**Planned sources:**

| Source | Transport | Extra Required |
|---|---|---|
| Apache Kafka | `confluent-kafka` consumer | `[kafka]` |
| AWS Kinesis | `boto3` shard reader | `[kinesis]` |
| WebSocket | `websockets` async client | `[websocket]` |
| HTTP webhook / SSE | stdlib `http.server` + `requests` | core |

**Planned signature:**

```python
def stream(
    source: str,                          # "kafka", "kinesis", "websocket", "webhook"
    *,
    config: dict,                         # source-specific connection config
    batch_size: int = 500,                # rows per yielded DataFrame
    timeout_ms: int = 1000,               # max wait before yielding a partial batch
    schema: dict | None = None,           # optional wasabi schema for normalization
    deserializer: callable | None = None, # custom message deserializer
    max_batches: int | None = None,       # stop after N batches (None = run forever)
) -> Generator[pd.DataFrame, None, None]
```

`stream()` is a generator — it never loads all data into memory. Each `yield` produces a DataFrame of up to `batch_size` rows. If `timeout_ms` elapses before the batch is full, a partial batch is yielded anyway (no stalling).

**Config examples:**

```python
# Kafka
config = {
    "bootstrap.servers": "localhost:9092",
    "group.id":           "sushitruck-consumer",
    "auto.offset.reset":  "earliest",
    "topic":              "trades",
}

# Kinesis
config = {
    "stream_name":  "market-events",
    "region_name":  "us-east-1",
    "shard_id":     "shardId-000000000000",
    "iterator_type": "TRIM_HORIZON",
}

# WebSocket
config = {
    "uri":     "wss://data.example.com/feed",
    "headers": {"Authorization": "Bearer <token>"},
}

# Webhook (starts a local listener)
config = {
    "host": "0.0.0.0",
    "port": 8080,
    "path": "/ingest",
}
```

**Example:**

```python
from sushitruck import nigiri

for batch_df in nigiri.stream("kafka", config=kafka_config, batch_size=1000):
    clean = orange_chicken(batch_df)
    process(clean)

# With a batch limit for testing
for batch_df in nigiri.stream("kafka", config=kafka_config, max_batches=10):
    print(batch_df.shape)
```

**Design notes:**

- The generator is the central abstraction — all sources expose the same `for batch_df in nigiri.stream(...)` interface regardless of backend
- `schema=` passes through to `wasabi` for automatic normalization of each batch before yielding
- `deserializer=` allows the caller to handle non-JSON messages (Avro, Protobuf, MessagePack, etc.)
- `max_batches=` is essential for testing and for finite consumption of a bounded stream

---

### `maki` — REST API Client

**File:** `src/sushitruck/maki.py`  
**Core dependency:** `requests`

Named for maki rolls — structured, neat, tightly wrapped. `maki` is SushiTruck's REST API client. It handles the boilerplate that every API integration needs: authentication, pagination, rate limiting, retry with exponential backoff, and response normalization. Each "roll" is a configured API client pointed at a specific endpoint.

**Planned signatures:**

```python
class MakiClient:
    def __init__(
        self,
        base_url: str,
        *,
        auth: dict | None = None,         # {"type": "bearer", "token": "..."} | {"type": "basic", ...} | {"type": "api_key", ...}
        headers: dict | None = None,
        timeout: int = 30,
        rate_limit: float | None = None,  # max requests per second
        retries: int = 3,
        retry_on: list[int] = [429, 500, 502, 503, 504],
        session: requests.Session | None = None,
    )

    def get(
        self,
        endpoint: str,
        *,
        params: dict | None = None,
        paginate: bool = False,
        page_param: str = "page",
        page_size_param: str = "per_page",
        page_size: int = 100,
        results_key: str | None = None,   # nested key to extract from response JSON
    ) -> dict | list[dict]

    def post(
        self,
        endpoint: str,
        *,
        body: dict | None = None,
        json: dict | None = None,
    ) -> dict

    def fetch(
        self,
        endpoint: str,
        *,
        params: dict | None = None,
        paginate: bool = False,
        results_key: str | None = None,
        flatten: bool = True,             # pass through wasabi.flatten() automatically
    ) -> pd.DataFrame
```

**Auth types supported:**

| `auth["type"]` | Header sent |
|---|---|
| `"bearer"` | `Authorization: Bearer <token>` |
| `"basic"` | `Authorization: Basic <base64(user:pass)>` |
| `"api_key"` | Configurable header or query param (e.g., `X-API-Key`) |
| `"oauth2"` | OAuth2 client-credentials flow (token refresh handled automatically) |

**Pagination strategies** (chosen with `pagination=`; `"auto"` by default):

| Strategy | Detection | Behavior |
|---|---|---|
| Page-number | `?page=N` pattern | Increments page until empty results |
| Cursor / token | `next_cursor` / `next_token` in response | Follows cursor until null |
| Offset/limit | `pagination="offset"` | Increments offset by `page_size` until a short or empty page |
| Link header | `Link: <url>; rel="next"` | Follows `next` link until absent |

`fetch()` is the highest-level method — calls `get()` with optional pagination, passes results through `wasabi.flatten()`, and returns a single `pd.DataFrame`. This is the method most callers want.

**Example:**

```python
from sushitruck import maki

client = maki.MakiClient(
    "https://api.example.com/v2",
    auth={"type": "bearer", "token": "my-token"},
    rate_limit=10.0,   # max 10 requests/second
    retries=3,
)

# Simple fetch → DataFrame
df = client.fetch("/prices", params={"symbol": "NVDA"})

# Paginated fetch → all pages merged into one DataFrame
df = client.fetch(
    "/transactions",
    paginate=True,
    page_size=200,
    results_key="data.transactions",   # nested key: response["data"]["transactions"]
)
```

**Design notes:**

- `rate_limit` and retry are delegated to `gari` internally — `maki` just calls `gari.throttle()` and `gari.retry()`
- `results_key` uses dot-notation to extract a nested list from the response: `"data.items"` → `response["data"]["items"]`
- OAuth2 token refresh is transparent — the client re-fetches the token when it expires without raising to the caller
- Session reuse: a single `requests.Session` is shared across all calls on a `MakiClient` instance for connection pooling

---

### `sashimi` — Raw File & Object Store Reader

**File:** `src/sushitruck/sashimi.py`  
**Core:** Local files (CSV, JSON, Parquet, JSONL)  
**Optional extras:** `[cloud]` for S3 and GCS

Named for sashimi — the raw, unadorned slice. No wrapping, no cooking. `sashimi` reads data from files or object stores in memory-safe chunks, yielding DataFrames. It handles local files, S3 objects, and GCS blobs through a unified interface.

**Planned signature:**

```python
def read(
    path: str | Path,
    *,
    format: str | None = None,       # "csv", "json", "jsonl", "parquet" — auto-detected from extension if None
    chunksize: int | None = None,     # None = load entire file; int = yield chunks of this row count
    schema: dict | None = None,       # optional wasabi schema for normalization per chunk
    storage: str = "local",          # "local", "s3", "gcs"
    storage_options: dict | None = None,  # credentials / region for S3 or GCS
    **read_kwargs,                    # passed through to the underlying pandas reader
) -> pd.DataFrame | Generator[pd.DataFrame, None, None]
```

When `chunksize` is `None`, returns a single `pd.DataFrame`. When `chunksize` is set, returns a generator of DataFrames — keeping memory usage flat regardless of file size.

**Supported formats:**

| Format | Extension | Backend |
|---|---|---|
| CSV | `.csv`, `.tsv`, `.txt` | `pd.read_csv` |
| JSON (array) | `.json` | `pd.read_json` |
| JSON Lines | `.jsonl`, `.ndjson` | `pd.read_json(lines=True)` |
| Parquet | `.parquet` | `pd.read_parquet` |

**Storage backends:**

| `storage` | Path format | Extra |
|---|---|---|
| `"local"` | `"/path/to/file.csv"` | core |
| `"s3"` | `"s3://bucket/prefix/file.csv"` | `[cloud]` |
| `"gcs"` | `"gs://bucket/prefix/file.csv"` | `[cloud]` |

For S3 and GCS, path parsing, credential handling, and streaming download are handled transparently — the caller just passes a URI.

**`list_objects()` utility:**

```python
def list_objects(
    prefix: str,
    *,
    storage: str = "local",
    storage_options: dict | None = None,
    pattern: str | None = None,      # glob pattern, e.g. "*.csv"
) -> list[str]
```

Returns a list of matching file paths or object URIs. Useful for batch ingestion via `temaki`.

**Examples:**

```python
from sushitruck import sashimi

# Local CSV — full load
df = sashimi.read("prices.csv")

# Local CSV — chunked (memory-safe for large files)
for chunk_df in sashimi.read("big_trades.csv", chunksize=50_000):
    process(chunk_df)

# S3 Parquet
df = sashimi.read(
    "s3://my-bucket/data/prices.parquet",
    storage="s3",
    storage_options={"region_name": "us-east-1"},
)

# S3 chunked CSV
for chunk_df in sashimi.read(
    "s3://my-bucket/data/trades.csv",
    chunksize=10_000,
    storage="s3",
    storage_options={"region_name": "us-east-1"},
):
    process(chunk_df)

# List all CSVs in an S3 prefix
paths = sashimi.list_objects(
    "s3://my-bucket/data/",
    storage="s3",
    pattern="*.csv",
    storage_options={"region_name": "us-east-1"},
)
```

**Design notes:**

- When `chunksize` is not set and the format supports it (CSV, JSONL), `sashimi` still reads in chunks internally to avoid a single massive `pd.read_csv()` call — it just concatenates them before returning
- `**read_kwargs` are forwarded to the underlying pandas reader, so `dtype=`, `usecols=`, `parse_dates=`, etc. all work as expected
- Format auto-detection from extension covers 95% of use cases; explicit `format=` override handles the rest (e.g., a `.txt` file that is actually CSV)

---

### `wasabi` — JSON Flattener & Schema Normalizer

**File:** `src/sushitruck/wasabi.py`  
**Core dependency:** `pandas` only

Named for wasabi — the sharp, clarifying hit that cuts through everything and makes structure apparent. `wasabi` is the normalization layer. It flattens nested JSON into a flat DataFrame, coerces types, renames columns, and enforces a schema — turning the messy raw payload into something ThaiTruck can work with.

This is the module used internally by `maki.fetch()`, `nigiri.stream()`, and `sashimi.read()` when `schema=` is passed, and is also available as a standalone tool.

**Planned signatures:**

```python
def flatten(
    data: dict | list[dict] | str,   # JSON object, list of objects, or raw JSON string
    *,
    separator: str = "_",             # separator for nested key paths ("a.b.c" → "a_b_c")
    max_depth: int | None = None,     # limit flattening depth (None = fully flat)
    drop_empty: bool = True,          # drop keys with None / empty-list values
) -> pd.DataFrame

def normalize(
    df: pd.DataFrame,
    schema: dict,
    *,
    strict: bool = False,             # True = raise on extra columns; False = warn and keep
    coerce: bool = True,              # attempt dtype coercion per schema spec
) -> pd.DataFrame

def infer_schema(
    df: pd.DataFrame,
    *,
    sample_n: int | None = 1000,      # rows to sample for type inference
) -> dict
```

**`flatten()` — nested JSON → flat DataFrame:**

Handles the common API response structures:

```python
# Nested object → dot-path columns
flatten({"user": {"id": 1, "name": "Alice"}, "score": 0.9})
# → pd.DataFrame([{"user_id": 1, "user_name": "Alice", "score": 0.9}])

# List of nested objects
flatten([{"event": {"type": "click", "ts": 1700000000}, "user_id": 42}, ...])
# → pd.DataFrame with columns: event_type, event_ts, user_id

# Raw JSON string
flatten('{"price": 142.5, "meta": {"source": "bloomberg"}}')
```

**`normalize()` — schema enforcement:**

The `schema` dict follows the same pattern as ThaiTruck's `nam_pla` (when that ships):

```python
schema = {
    "price":     {"dtype": float,  "nullable": False, "rename": "close_price"},
    "volume":    {"dtype": int,    "nullable": True},
    "symbol":    {"dtype": str,    "nullable": False},
    "timestamp": {"dtype": "datetime64[ns]", "nullable": False},
}

clean_df = wasabi.normalize(raw_df, schema, coerce=True)
```

Schema fields:

| Key | Description |
|---|---|
| `"dtype"` | Target dtype — Python type, numpy dtype string, or `"datetime64[ns]"` |
| `"nullable"` | If `False`, raises on any null values in this column |
| `"rename"` | Rename this column after coercion |
| `"default"` | Fill value for nulls before nullable check |

**`infer_schema()` — auto-generate a schema from a sample:**

Inspects a DataFrame (or a sample of it) and returns a `schema` dict suitable for passing to `normalize()`. Useful as a starting point — generate it once, tweak by hand, then use it in production.

```python
schema = wasabi.infer_schema(sample_df)
# Returns: {"price": {"dtype": float, "nullable": False}, "symbol": {"dtype": str, ...}, ...}
```

**Examples:**

```python
from sushitruck import wasabi

# Flatten a raw API payload
payload = [{"price": "142.5", "meta": {"source": "bloomberg", "delay": 0}}, ...]
df = wasabi.flatten(payload)
# Columns: price, meta_source, meta_delay

# Normalize with a schema
schema = {
    "price":      {"dtype": float, "nullable": False},
    "meta_source": {"dtype": str,  "nullable": True, "rename": "source"},
}
clean = wasabi.normalize(df, schema, coerce=True)
```

---

### `gari` — Rate Limiter & Retry

**File:** `src/sushitruck/gari.py`  
**Core dependency:** stdlib only (`time`, `functools`, `threading`)

Named for pickled ginger — the palate cleanser between pieces. `gari` is the reliability layer: rate limiting, retry with exponential backoff, and a circuit breaker. It is used internally by `maki`, and is also available as a standalone decorator for any callable that touches an external service.

**Planned signatures:**

```python
# Decorator: rate limit a function
@gari.rate_limit(calls_per_second=10.0)
def my_api_call(): ...

# Decorator: retry on failure
@gari.retry(
    max_attempts=3,
    backoff_base=2.0,     # seconds; doubles each attempt
    jitter=True,          # add random jitter to avoid thundering herd
    retry_on=(429, 500, 502, 503, 504),   # HTTP status codes OR exception types
)
def my_api_call(): ...

# Decorator: circuit breaker
@gari.circuit_breaker(
    failure_threshold=5,   # open circuit after 5 consecutive failures
    recovery_timeout=60,   # seconds to wait before trying again (half-open)
)
def my_api_call(): ...

# Composable: all three together. retry goes on top so that every attempt,
# including retries, passes through the rate limiter and the breaker below it.
@gari.retry(max_attempts=4)
@gari.rate_limit(calls_per_second=5.0)
@gari.circuit_breaker(failure_threshold=10)
def my_api_call(): ...

# Programmatic (non-decorator) usage
gari.sleep_until_ready(calls_per_second=10.0)   # block until the rate window allows a call
gari.backoff_sleep(attempt=2, base=2.0)          # sleep for base^attempt seconds with jitter
```

**`rate_limit` — token-bucket implementation:**

Maintains a token bucket per decorated function. Calls block (sleep) when the bucket is empty. Thread-safe using `threading.Lock`.

**`retry` — exponential backoff:**

| Attempt | Sleep (base=2.0, no jitter) |
|---|---|
| 1 (first retry) | 2.0 s |
| 2 | 4.0 s |
| 3 | 8.0 s |
| 4 | 16.0 s |

With `jitter=True`, sleep time is multiplied by `random.uniform(0.5, 1.5)` to spread out retries from multiple concurrent callers.

`retry_on` accepts HTTP status codes (when the wrapped function returns a `requests.Response`) and/or exception types.

**`circuit_breaker` — fail-fast protection:**

Three states:
- **Closed** (normal) — calls pass through; failures are counted
- **Open** — calls fail immediately without attempting the wrapped function; fires after `failure_threshold` consecutive failures
- **Half-open** — after `recovery_timeout` seconds, allows one trial call; if it succeeds, circuit closes; if it fails, circuit re-opens

**Design notes:**

- `rate_limit` uses wall-clock time (not a fixed window) — a token-bucket algorithm that smooths bursts rather than allowing all calls at the start of a window
- All three decorators are composable. Order matters: `retry` must be outermost, or retries bypass the rate limiter (the outer wrapper only waits once)
- Circuit breaker state is per-decorated-function and per-process (not shared across processes or threads unless a shared backend is added later)
- `gari` has zero external dependencies — pure stdlib — so it is always available as part of the core install

**Example:**

```python
from sushitruck import gari

# raise_for_status() turns 429/5xx into HTTPError, so retry on the exception type
# (status codes in retry_on only match when the function returns the Response).
@gari.retry(max_attempts=4, backoff_base=2.0, jitter=True,
            retry_on=(requests.HTTPError, requests.ConnectionError))
@gari.rate_limit(calls_per_second=5.0)
def fetch_price(symbol: str) -> dict:
    response = requests.get(f"https://api.example.com/price/{symbol}")
    response.raise_for_status()
    return response.json()
```

---

### `temaki` — Batch Ingestion Coordinator

**File:** `src/sushitruck/temaki.py`  
**Core dependency:** `pandas`, `sushitruck.sashimi`, `sushitruck.wasabi`

Named for the hand roll — you assemble it yourself from multiple pieces. `temaki` coordinates multi-source batch ingestion jobs. It takes a list of sources (files, API endpoints, S3 prefixes), reads them all, applies normalization, and returns a combined DataFrame (or a generator of batches).

Think of it as the batch-mode sibling to `nigiri`'s streaming approach: when you have a set of known files or API endpoints to process together, `temaki` runs them in sequence (or parallel) and hands back a unified result.

**Planned signatures:**

```python
class TemakiJob:
    def __init__(
        self,
        *,
        workers: int = 1,             # 1 = sequential, >1 = thread-pool parallel
        schema: dict | None = None,   # applied to each source after reading
        on_error: str = "raise",      # "raise", "warn", "skip"
    )

    def add_file(
        self,
        path: str | Path,
        *,
        format: str | None = None,
        chunksize: int | None = None,
        storage: str = "local",
        storage_options: dict | None = None,
        **read_kwargs,
    ) -> TemakiJob    # returns self for chaining

    def add_api(
        self,
        client: maki.MakiClient,
        endpoint: str,
        *,
        params: dict | None = None,
        paginate: bool = False,
        results_key: str | None = None,
    ) -> TemakiJob

    def add_glob(
        self,
        pattern: str,
        *,
        storage: str = "local",
        storage_options: dict | None = None,
        **read_kwargs,
    ) -> TemakiJob    # adds all files matching a glob / S3 prefix pattern

    def run(self) -> pd.DataFrame                              # fully materialized
    def stream(self) -> Generator[pd.DataFrame, None, None]   # source-by-source generator
```

**`TemakiResult`** (returned alongside `run()`):

| Field | Description |
|---|---|
| `df` | The merged DataFrame |
| `sources_processed` | Count of sources successfully read |
| `sources_failed` | List of sources that errored (when `on_error="warn"` or `"skip"`) |
| `total_rows` | Total row count before dedup/merge |
| `elapsed_s` | Wall-clock time for the full job |

**Example:**

```python
from sushitruck import temaki, maki

client = maki.MakiClient("https://api.example.com", auth={"type": "bearer", "token": "..."})

job = (
    temaki.TemakiJob(workers=4, on_error="warn")
    .add_glob("s3://my-bucket/prices/*.parquet", storage="s3",
              storage_options={"region_name": "us-east-1"})
    .add_api(client, "/supplemental", paginate=True, results_key="data")
    .add_file("local_overrides.csv")
)

result = job.run()      # TemakiResult
df = result.df
# or, memory-safe:
for batch_df in job.stream():
    process(batch_df)
```

**Design notes:**

- `workers=1` (sequential) is the default — safe, predictable, easy to debug
- `workers>1` uses `concurrent.futures.ThreadPoolExecutor` — suitable for I/O-bound sources; not multiprocessing (avoids pickling complexity)
- `on_error="skip"` continues past failed sources and records them in `TemakiResult.sources_failed` — useful when processing hundreds of files where a few may be corrupt or missing
- `add_glob()` calls `sashimi.list_objects()` internally to resolve the pattern before adding individual sources

---

### `tobiko` — Output Router

**File:** `src/sushitruck/tobiko.py`  
**Core dependency:** `pandas` only for local/CSV targets; `[cloud]` for S3/GCS targets

Named for tobiko — the tiny orange flying fish roe scattered on top. Small, numerous, precisely placed. `tobiko` routes a processed DataFrame to one or more downstream destinations: a local file, an S3/GCS object, or a message queue. SushiTruck's job ends here — it produces a clean DataFrame and delivers it wherever the caller needs it. What happens to that DataFrame next is the caller's business.

**Planned signatures:**

```python
def send(
    df: pd.DataFrame,
    target: str | list[str],
    *,
    format: str = "parquet",         # "parquet", "csv", "json", "jsonl"
    mode: str = "overwrite",         # "overwrite", "append"
    partition_by: str | None = None, # column name to partition output files by value
    storage_options: dict | None = None,
) -> TobikoResult

# Sink to a message queue (publish each row as a JSON message)
def publish(
    df: pd.DataFrame,
    target: str,                     # "kafka://topic", "kinesis://stream", etc.
    *,
    config: dict,
    serializer: callable | None = None,
) -> int  # returns count of messages published
```

**`send()` target formats:**

| Target string | Behavior |
|---|---|
| `"/path/to/file.parquet"` | Write local file |
| `"s3://bucket/prefix/file.parquet"` | Write to S3 (requires `[cloud]`) |
| `"gs://bucket/prefix/file.parquet"` | Write to GCS (requires `[cloud]`) |
| `["path1.parquet", "path2.csv"]` | Write to multiple targets simultaneously |

**Partitioned output (`partition_by=`):**

```python
tobiko.send(
    df,
    "s3://my-bucket/data/prices/",
    format="parquet",
    partition_by="date",  # writes s3://my-bucket/data/prices/date=2025-01-01/part.parquet, etc.
)
```

Partitioned output follows the Hive partition convention (`col=value/`) for compatibility with Athena, Spark, and other tools.

**`TobikoResult`:**

| Field | Description |
|---|---|
| `targets_written` | List of target paths/URIs successfully written |
| `rows_written` | Total row count written |
| `bytes_written` | Total bytes written (where determinable) |
| `elapsed_s` | Wall-clock time |

**Example:**

```python
from sushitruck import tobiko

# Write to S3 as partitioned Parquet
result = tobiko.send(
    clean_df,
    "s3://my-bucket/processed/trades/",
    format="parquet",
    partition_by="date",
    storage_options={"region_name": "us-east-1"},
)
print(f"Wrote {result.rows_written} rows to {result.targets_written}")

# Publish to Kafka
n = tobiko.publish(clean_df, "kafka://processed-trades", config=kafka_config)
print(f"Published {n} messages")
```

---

## Design Principles

### Generator-First

Streaming sources and chunked file readers always return generators, not loaded DataFrames. The caller decides whether to consume all batches (loop to exhaustion) or stop early. This keeps memory usage predictable regardless of source size.

### Normalize at the Boundary

Raw payloads — nested JSON, inconsistent column names, mixed dtypes — are normalized *inside* SushiTruck before they leave. By the time a DataFrame exits `maki.fetch()`, `nigiri.stream()`, or `sashimi.read()`, it is flat and type-coerced. Whatever the caller does with that DataFrame next is not SushiTruck's concern.

### No Mutation

Following the fleet convention: nothing mutates its inputs. `wasabi.normalize()` returns a new DataFrame; `gari` decorators wrap without modifying the original callable.

### Optional Extras, Not Mandatory Bloat

Kafka, Kinesis, boto3, and google-cloud-storage are each tens of megabytes of transitive dependencies. None are required for the common case (REST APIs + local files). They are always opt-in.

### Explicit Over Magic

`maki` does not auto-detect pagination strategy silently — the caller specifies `paginate=True` and optionally the strategy. `wasabi` does not silently drop columns — with `strict=False` it warns, with `strict=True` it raises. Surprises in data pipelines are expensive.

---

## Composing with the Fleet

SushiTruck, ThaiTruck, and RamenTruck are **fully independent packages**. None of them imports from another. They compose at the **application layer** through the common currency of `pd.DataFrame` — SushiTruck produces them, ThaiTruck transforms them, RamenTruck consumes them. The user's code is the wiring.

The packages do not know about each other. Adding `thaitruck` to a SushiTruck `pyproject.toml` dependency would be a bug, not a feature.

**How a user wires the fleet together (application-level code):**

```python
# This is USER code — not SushiTruck internals.
# Each package is imported independently; none calls another.

from sushitruck import maki, wasabi, tobiko
from thaitruck import orange_chicken, fried_rice, larb
from ramentruck import broth, soft_boiled_egg, chashu
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split

# --- SushiTruck produces DataFrames ---
client = maki.MakiClient(
    "https://api.example.com",
    auth={"type": "bearer", "token": "my-token"},
    rate_limit=10.0,
)

prices_df   = client.fetch("/prices",   paginate=True)
earnings_df = client.fetch("/earnings", paginate=True)

schema = {
    "close":  {"dtype": float, "nullable": False},
    "volume": {"dtype": int,   "nullable": True},
    "symbol": {"dtype": str,   "nullable": False},
    "date":   {"dtype": "datetime64[ns]", "nullable": False},
}
prices_df   = wasabi.normalize(prices_df,   schema)
earnings_df = wasabi.normalize(earnings_df, schema)

# --- ThaiTruck transforms DataFrames ---
prices_clean   = orange_chicken(prices_df,   heat=3)
earnings_clean = orange_chicken(earnings_df, heat=3)
merged = fried_rice(prices_clean, earnings_clean, freq="D")
profile = larb(merged, heat=3)

# --- RamenTruck consumes DataFrames / arrays ---
X = merged.drop(columns=["target"])
y = merged["target"]
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

result = broth(GradientBoostingClassifier(), X_train, y_train, X_val, y_val,
               metrics=["accuracy", "roc_auc"])
cv = soft_boiled_egg(result.model, X, y, strategy="stratified", learning_curve=True)
chashu.save(result.model, "models/gbm_v1.chashu",
            metadata={"val_auc": result.metrics["roc_auc"]})

# --- SushiTruck routes the output ---
tobiko.send(merged, "s3://my-bucket/processed/merged.parquet",
            storage_options={"region_name": "us-east-1"})
```

---

## The Food Truck Fleet

| Package | Status | Focus |
|---|---|---|
| **thaitruck** | Live on PyPI (v0.2.2) | Batch DataFrame cleaning, merging, profiling, caching |
| **sushitruck** | Live on PyPI (v0.2.2) | Streaming ingestion, REST API connectors, file reading, normalization, output routing |
| **ramentruck** | PyPI name secured (v0.1.0 stub) | ML/AI toolkit — training, tuning, cross-validation, explainability, deep learning |

Each package is fully independent — none imports from another. They compose at the application layer through `pd.DataFrame`. SushiTruck produces them. ThaiTruck transforms them. RamenTruck models them. The user's code is the only thing that knows about all three.

---

## Build & Publish Plan

### Before First Real Release (v0.2.0) — done

1. Implement core modules: `wasabi`, `gari`, `maki`, `sashimi` (local files only)
2. Add `tests/` — one file per module; use `responses` library to mock HTTP calls in `test_maki.py`
3. Declare optional extras in `pyproject.toml`
4. Add import guards in `nigiri.py` (Kafka/Kinesis/WebSocket paths) and `sashimi.py` (S3/GCS paths)
5. Add `py.typed` marker (PEP 561)
6. Add `CHANGELOG.md`
7. Add `.github/workflows/tests.yml` — pytest on Python 3.9/3.10/3.11/3.12 *(added in 0.2.2, with 3.13 plus Windows and macOS)*

### Recommended implementation order

1. `gari` — no dependencies, pure stdlib; foundational for everything else
2. `wasabi` — pandas only; needed by all other modules
3. `maki` — builds on `gari`; most immediately useful
4. `sashimi` (local only) — builds on `wasabi`; straightforward
5. `tobiko` (local output) — completes the local pipeline
6. `temaki` — builds on `sashimi` and `maki`
7. `nigiri` (WebSocket first, then Kafka/Kinesis) — streaming last; most complex

### Build commands

```bash
python -m build          # dist/*.whl + dist/*.tar.gz
twine check dist/*       # verify metadata
twine upload dist/*      # publish to PyPI
```

### Version bump (two places)

- `pyproject.toml` → `version = "x.y.z"`
- `src/sushitruck/__init__.py` → `__version__ = "x.y.z"`

---

*Last updated: 2026-10-09*

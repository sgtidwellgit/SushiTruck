# sushitruck

[![PyPI version](https://img.shields.io/pypi/v/sushitruck.svg)](https://pypi.org/project/sushitruck/)
[![Python versions](https://img.shields.io/pypi/pyversions/sushitruck.svg)](https://pypi.org/project/sushitruck/)
[![Tests](https://github.com/sgtidwellgit/SushiTruck/actions/workflows/tests.yml/badge.svg)](https://github.com/sgtidwellgit/SushiTruck/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/sgtidwellgit/SushiTruck/blob/main/LICENSE)

**SushiTruck is a streaming ingestion and API connector toolkit for pandas — the conveyor belt of the data pipeline.**

It handles everything that happens *before* analysis: calling REST APIs (with auth, pagination, rate limiting, and retry), reading large files from local disk, S3, or GCS in memory-safe chunks, consuming live streams from Kafka, Kinesis, WebSockets, or webhooks, flattening and type-checking messy JSON, and writing the cleaned result wherever it needs to go.

Every path ends in the same place: a clean `pandas.DataFrame`, or a generator of them.

```bash
pip install sushitruck
```

> Just like conveyor belt sushi: data flows continuously past, and you take exactly what you need, at your own pace.

---

## Table of contents

- [Why SushiTruck?](#why-sushitruck)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Examples](#examples) — nine complete programs you can run right now
- [The belt: module overview](#the-belt-module-overview)
- [`maki` — REST API client](#maki--rest-api-client)
- [`sashimi` — file and object store reader](#sashimi--file-and-object-store-reader)
- [`wasabi` — JSON flattening and schema enforcement](#wasabi--json-flattening-and-schema-enforcement)
- [`gari` — rate limiting, retry, and circuit breaking](#gari--rate-limiting-retry-and-circuit-breaking)
- [`temaki` — batch ingestion coordinator](#temaki--batch-ingestion-coordinator)
- [`tobiko` — output router](#tobiko--output-router)
- [`nigiri` — streaming source adapters](#nigiri--streaming-source-adapters)
- [Result objects](#result-objects)
- [Recipes](#recipes)
- [Errors and troubleshooting](#errors-and-troubleshooting)
- [Design principles](#design-principles)
- [Using SushiTruck with other packages](#using-sushitruck-with-other-packages)
- [Development](#development)
- [Changelog and versioning](#changelog-and-versioning)
- [License](#license)

---

## Why SushiTruck?

Most data projects start with the same chores, rewritten from scratch every time:

- An API returns nested JSON spread across 40 pages, and needs a bearer token, a rate limit, and retries on `503`.
- A 6 GB CSV won't fit in memory and needs to be read in chunks.
- A folder (or S3 prefix) of 300 Parquet files needs to become one DataFrame, and three of the files are corrupt.
- A Kafka topic needs to be read in micro-batches of 1,000 messages, with partial batches when traffic is quiet.
- The result needs to be written to S3, partitioned by date, in Hive layout.

SushiTruck packages each of those chores as a small, documented, tested module with a consistent interface:

| Principle | What it means in practice |
|---|---|
| **Generator-first** | Streams and chunked reads yield DataFrames lazily. Memory stays flat no matter how big the source is. |
| **Batteries included** | Auth, pagination, rate limiting, retry, and circuit breaking are built in, not left to the caller. |
| **Normalize at the boundary** | Pass a `schema=` and every DataFrame leaving SushiTruck is flattened, type-coerced, renamed, and null-checked. |
| **Optional extras, not bloat** | The core needs only `pandas`, `numpy`, and `requests`. Kafka, AWS, GCS, and WebSocket clients are opt-in. |
| **No mutation** | Nothing modifies its inputs. Every function returns a new DataFrame. |
| **Explicit over magic** | You choose the pagination strategy, the error policy, and the strictness of schema checks. Nothing is silently dropped. |

---

## Installation

SushiTruck supports **Python 3.9 through 3.13**.

```bash
pip install sushitruck
```

The core install is deliberately small: `pandas>=1.5`, `numpy>=1.23`, and `requests>=2.28`. That covers REST APIs, local files (CSV, JSON, JSON Lines), webhook streaming, and local output.

Heavier integrations are optional extras. Install only what you use:

| Extra | Adds | Installs |
|---|---|---|
| *(none)* | REST APIs, local files, webhook streams, local output | `pandas`, `numpy`, `requests` |
| `[cloud]` | Read from and write to **S3** and **GCS** | `boto3`, `google-cloud-storage` |
| `[kafka]` | **Kafka** streaming (`nigiri`) and publishing (`tobiko`) | `confluent-kafka` |
| `[kinesis]` | **AWS Kinesis** streaming and publishing | `boto3` |
| `[websocket]` | **WebSocket** streaming | `websockets>=14` |
| `[all]` | Everything above | all of the above |

```bash
pip install "sushitruck[cloud]"
pip install "sushitruck[kafka,cloud]"
pip install "sushitruck[all]"
```

> Quote the brackets as shown — some shells (zsh in particular) treat `[...]` as a glob pattern.

**Parquet support.** Reading and writing Parquet uses pandas' own Parquet engine, so you'll need `pyarrow` (or `fastparquet`) installed: `pip install pyarrow`.

If you call a feature whose extra isn't installed, SushiTruck raises an `ImportError` that names the exact `pip install` command to run. Nothing heavy is imported until you actually use it.

---

## Quick start

Pull every page of an API, enforce a schema, and write the result to partitioned files. This runs as-is: it uses [JSONPlaceholder](https://jsonplaceholder.typicode.com), a free public test API that needs no account or key.

```python
from sushitruck import maki, wasabi, tobiko

client = maki.MakiClient("https://jsonplaceholder.typicode.com", rate_limit=5.0, retries=3)

# Every page of /todos, merged into one DataFrame.
raw = client.fetch("/todos", paginate=True, pagination="page",
                   page_param="_page", page_size_param="_limit", page_size=50)

# Typed, renamed, and validated.
todos = wasabi.normalize(raw, {
    "id":        {"dtype": int, "nullable": False, "rename": "todo_id"},
    "userId":    {"dtype": int, "nullable": False, "rename": "user_id"},
    "title":     {"dtype": str},
    "completed": {"dtype": bool, "default": False},
})

# One file per user: todos/user_id=1/part.parquet, todos/user_id=2/part.parquet, ...
result = tobiko.send(todos, "todos/", format="parquet", partition_by="user_id")
print(f"wrote {result.rows_written} rows to {len(result.targets_written)} files")
# wrote 200 rows to 10 files
```

(Parquet output needs `pip install pyarrow`. Use `format="csv"` to skip that.)

---

## Examples

Each example below is a **complete program**: copy it into a `.py` file and run it. They use only the core install, local files, and free public APIs that need no sign-up ([JSONPlaceholder](https://jsonplaceholder.typicode.com) and the [GitHub REST API](https://docs.github.com/en/rest)), so you can try every one right now. The output shown under each is what it actually printed.

| # | Example | Shows |
|---|---|---|
| 1 | [Your first API call](#example-1--your-first-api-call) | `MakiClient`, `fetch()` |
| 2 | [Paginating an API three different ways](#example-2--paginating-an-api-three-different-ways) | page, offset, and Link-header pagination |
| 3 | [Flattening nested JSON and enforcing a schema](#example-3--flattening-nested-json-and-enforcing-a-schema) | `wasabi.flatten`, `wasabi.normalize` |
| 4 | [Processing a file too big for memory](#example-4--processing-a-file-too-big-for-memory) | `sashimi.read(chunksize=...)` |
| 5 | [Combining an API and a folder of files](#example-5--combining-an-api-and-a-folder-of-files) | `TemakiJob`, error handling |
| 6 | [Writing partitioned output](#example-6--writing-partitioned-output) | `tobiko.send`, partitions, append |
| 7 | [Making any function resilient](#example-7--making-any-function-resilient) | `@retry`, `@rate_limit`, `@circuit_breaker` |
| 8 | [Receiving a live stream of webhook events](#example-8--receiving-a-live-stream-of-webhook-events) | `nigiri.stream("webhook")` |
| 9 | [A complete extract-normalize-load job](#example-9--a-complete-extract-normalize-load-job) | all of the above together |

For S3, GCS, Kafka, and Kinesis, which need your own accounts, see the templates under [Recipes](#recipes).

### Example 1 — Your first API call

```python
from sushitruck import maki

# 1. Point a client at the API's base URL.
client = maki.MakiClient("https://jsonplaceholder.typicode.com")

# 2. Fetch an endpoint. The JSON list comes back as a DataFrame.
posts = client.fetch("/posts", params={"userId": 1})

print(posts.shape)
print(posts[["id", "title"]].head(3))
```

Output:

```text
(10, 4)
   id                                              title
0   1  sunt aut facere repellat provident occaecati e...
1   2                                       qui est esse
2   3  ea molestias quasi exercitationem repellat qui...
```

`fetch()` sends `GET https://jsonplaceholder.typicode.com/posts?userId=1`, parses the JSON, and turns the list of records into rows. To add authentication, pass `auth=` when creating the client — see [Authentication](#authentication).

### Example 2 — Paginating an API three different ways

APIs split large results into pages in different ways. Tell `maki` which scheme the API uses, and it collects every page into one DataFrame.

```python
from sushitruck import maki

client = maki.MakiClient("https://jsonplaceholder.typicode.com", rate_limit=5.0)

# Page numbers: ?_page=1&_limit=25, ?_page=2&_limit=25, ... until a page is empty.
by_page = client.fetch(
    "/comments",
    paginate=True,
    pagination="page",
    page_param="_page",
    page_size_param="_limit",
    page_size=25,
)
print("page strategy:  ", len(by_page), "rows")

# Offset/limit: ?_start=0&_limit=100, ?_start=100&_limit=100, ... until a short page.
by_offset = client.fetch(
    "/comments",
    paginate=True,
    pagination="offset",
    offset_param="_start",
    page_size_param="_limit",
    page_size=100,
)
print("offset strategy:", len(by_offset), "rows")

# Link headers: GitHub returns  Link: <...page=2>; rel="next"  until the last page.
github = maki.MakiClient("https://api.github.com", headers={"Accept": "application/vnd.github+json"})
releases = github.fetch(
    "/repos/psf/requests/releases",
    paginate=True,
    pagination="link",
    page_size=10,
)
print("link strategy:  ", len(releases), "releases")
print(releases[["tag_name", "published_at"]].head(3))
```

Output (the GitHub numbers change as new releases come out):

```text
page strategy:   500 rows
offset strategy: 500 rows
link strategy:   19 releases
  tag_name          published_at
0  v2.34.2  2026-05-14T19:27:15Z
1  v2.34.1  2026-05-13T19:23:51Z
2  v2.34.0  2026-05-11T19:40:27Z
```

`rate_limit=5.0` keeps the client to five requests per second, so walking 20 pages doesn't flood the server. Check your API's documentation for the names of its paging parameters, and pass them as `page_param`, `offset_param`, `page_size_param`, or `cursor_param`.

### Example 3 — Flattening nested JSON and enforcing a schema

```python
from sushitruck import maki, wasabi

client = maki.MakiClient("https://jsonplaceholder.typicode.com")

# Nested JSON ("address": {"geo": {"lat": ...}}) is flattened into columns automatically.
users = client.fetch("/users")
print(list(users.columns))

# Keep the columns you need, with the types and names you want.
schema = {
    "id":               {"dtype": int, "nullable": False, "rename": "user_id"},
    "name":             {"dtype": str, "nullable": False},
    "email":            {"dtype": str, "nullable": False},
    "address_city":     {"dtype": str, "rename": "city"},
    "address_geo_lat":  {"dtype": float, "rename": "lat"},
    "address_geo_lng":  {"dtype": float, "rename": "lng"},
}
columns = list(schema)
clean = wasabi.normalize(users[columns], schema, strict=True)

print(clean.dtypes)
print(clean.head(3).to_string())
```

Output:

```text
['id', 'name', 'username', 'email', 'address_street', 'address_suite', 'address_city', 'address_zipcode', 'address_geo_lat', 'address_geo_lng', 'phone', 'website', 'company_name', 'company_catchPhrase', 'company_bs']
user_id      int64
name        object
email       object
city        object
lat        float64
lng        float64
dtype: object
   user_id              name               email           city      lat      lng
0        1     Leanne Graham   Sincere@april.biz    Gwenborough -37.3159  81.1496
1        2      Ervin Howell   Shanna@melissa.tv    Wisokyburgh -43.9509 -34.4618
2        3  Clementine Bauch  Nathan@yesenia.net  McKenziehaven -68.6102 -47.0653
```

The API sends latitude and longitude as strings (`"-37.3159"`); the schema turns them into real floats. If a required field were missing or null, `normalize` would raise a `ValueError` naming the column, instead of letting bad data through.

### Example 4 — Processing a file too big for memory

```python
import numpy as np
import pandas as pd

from sushitruck import sashimi

# Make a 1,000,000-row CSV to stand in for "a file too big to load comfortably".
rng = np.random.default_rng(0)
pd.DataFrame({
    "symbol": rng.choice(["AAA", "BBB", "CCC"], size=1_000_000),
    "price": rng.uniform(10, 500, size=1_000_000).round(2),
}).to_csv("trades.csv", index=False)

# Read it 250,000 rows at a time and keep only a running total per symbol.
totals = {}
for chunk in sashimi.read("trades.csv", chunksize=250_000, dtype={"symbol": "category"}):
    for symbol, value in chunk.groupby("symbol", observed=True)["price"].sum().items():
        totals[symbol] = totals.get(symbol, 0.0) + value
    print(f"processed a chunk of {len(chunk):,} rows")

print({symbol: round(total) for symbol, total in sorted(totals.items())})

# List what's in a folder before reading it.
print(sashimi.list_objects(".", pattern="*.csv"))
```

Output:

```text
processed a chunk of 250,000 rows
processed a chunk of 250,000 rows
processed a chunk of 250,000 rows
processed a chunk of 250,000 rows
{'AAA': 84894892, 'BBB': 85089784, 'CCC': 85170430}
['trades.csv']
```

Only one 250,000-row chunk is in memory at a time, so the same loop works on a 50 GB file. Options such as `dtype=` pass straight through to `pandas.read_csv`. The same call reads from S3 or GCS by adding `storage="s3"` or `storage="gcs"` (with the `[cloud]` extra).

### Example 5 — Combining an API and a folder of files

```python
from pathlib import Path

from sushitruck import maki, temaki

# Older todos were archived to monthly CSV files; one of the files is corrupt.
archive = Path("archive")
archive.mkdir(exist_ok=True)
(archive / "2024-01.csv").write_text("userId,id,title,completed\n1,901,file taxes,True\n1,902,renew passport,False\n")
(archive / "2024-02.csv").write_text("userId,id,title,completed\n1,903,book dentist,True\n")
(archive / "2024-03.csv").write_bytes(b"\x00\xff not a csv \xfe")

client = maki.MakiClient("https://jsonplaceholder.typicode.com")

schema = {
    "userId":    {"dtype": int, "nullable": False},
    "id":        {"dtype": int, "nullable": False},
    "title":     {"dtype": str},
    "completed": {"dtype": bool},
}

# Combine the live API with the archive into one DataFrame, typed the same way.
job = (
    temaki.TemakiJob(on_error="skip", schema=schema)
    .add_api(client, "/todos", params={"userId": 1})   # 20 live todos
    .add_glob("archive/*.csv")                          # every CSV in the folder
)
result = job.run()

print(f"sources read:   {result.sources_processed}")
print(f"sources failed: {[Path(f['source']).name for f in result.sources_failed]}")
print(f"total rows:     {result.total_rows}")
print(result.df.tail(4).to_string())
```

Output:

```text
sources read:   3
sources failed: ['2024-03.csv']
total rows:     23
    userId   id                                      title  completed
19       1   20  ullam nobis libero sapiente ad optio sint       True
20       1  901                                 file taxes       True
21       1  902                             renew passport      False
22       1  903                               book dentist       True
```

With `on_error="skip"`, the corrupt file is recorded in `result.sources_failed` and the job carries on. Use `on_error="warn"` to also get a warning, or the default `"raise"` to stop at the first bad source. For hundreds of files or slow APIs, add `workers=8` to read sources in parallel.

### Example 6 — Writing partitioned output

```python
import pandas as pd

from sushitruck import sashimi, tobiko

sales = pd.DataFrame({
    "date":   ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03"],
    "sku":    ["A-1", "B-2", "A-1", "C-3"],
    "amount": [19.99, 5.00, 19.99, 42.50],
})

# One file per date, in the Hive layout (date=2024-01-01/part.csv) that Spark,
# DuckDB, Athena, and pandas all understand.
result = tobiko.send(sales, "lake/sales/", format="csv", partition_by="date")

print(f"{result.rows_written} rows -> {len(result.targets_written)} files")
for path in result.targets_written:
    print("  ", path)

# Write the same data to two places at once.
tobiko.send(sales, ["lake/sales.csv", "lake/sales.jsonl"], format="csv")

# Append new rows to an existing file.
tobiko.send(sales.head(1), "lake/sales.csv", format="csv", mode="append")
print(len(sashimi.read("lake/sales.csv")), "rows after append")
```

Output:

```text
4 rows -> 3 files
   lake/sales/date=2024-01-01/part.csv
   lake/sales/date=2024-01-02/part.csv
   lake/sales/date=2024-01-03/part.csv
5 rows after append
```

Change the target to `s3://my-bucket/sales/` or `gs://my-bucket/sales/` (with the `[cloud]` extra) and the same call writes to object storage.

### Example 7 — Making any function resilient

`gari`'s decorators work on any function — your own code, a third-party SDK, a database call — not just SushiTruck's.

```python
import time

from sushitruck import gari

calls = {"count": 0}


# A stand-in for any unreliable call: it fails twice, then succeeds.
@gari.retry(max_attempts=5, backoff_base=1.5, jitter=False, retry_on=(ConnectionError,))
@gari.rate_limit(calls_per_second=20)
def flaky_lookup(key):
    calls["count"] += 1
    if calls["count"] < 3:
        raise ConnectionError("temporary network blip")
    return {"key": key, "value": 42}


start = time.perf_counter()
print(flaky_lookup("abc"), f"after {calls['count']} attempts")
print(f"took {time.perf_counter() - start:.1f}s (backed off 1.5s, then 2.25s)")


# A circuit breaker stops hammering a service that is clearly down.
@gari.circuit_breaker(failure_threshold=3, recovery_timeout=30)
def broken_service():
    raise TimeoutError("service is down")


for attempt in range(1, 6):
    try:
        broken_service()
    except gari.CircuitBreakerOpenError:
        print(f"call {attempt}: circuit open - skipped without calling the service")
    except TimeoutError:
        print(f"call {attempt}: service failed")
```

Output:

```text
{'key': 'abc', 'value': 42} after 3 attempts
took 3.8s (backed off 1.5s, then 2.25s)
call 1: service failed
call 2: service failed
call 3: service failed
call 4: circuit open - skipped without calling the service
call 5: circuit open - skipped without calling the service
```

The wait before each retry grows exponentially (`backoff_base ** attempt`). In production, leave `jitter=True` (the default) so many clients don't all retry at the same moment. After 30 seconds, the breaker lets one trial call through to check whether the service has recovered.

### Example 8 — Receiving a live stream of webhook events

```python
import threading
import time

import requests

from sushitruck import nigiri


# Simulate another system POSTing events to us (in real use, this is a third party).
def send_events():
    time.sleep(0.5)  # give the listener a moment to start
    for i in range(7):
        requests.post("http://127.0.0.1:8080/ingest", json={"event": "click", "user": {"id": i}})


threading.Thread(target=send_events, daemon=True).start()

# Receive them as micro-batches of up to 3 rows. A quiet second yields a partial batch.
stream = nigiri.stream(
    "webhook",
    config={"host": "127.0.0.1", "port": 8080, "path": "/ingest"},
    batch_size=3,
    timeout_ms=1000,
    schema={"event": {"dtype": str}, "user_id": {"dtype": int}},  # flattened, then typed
    max_batches=3,
)
for batch in stream:
    print(f"batch of {len(batch)}: users {batch['user_id'].tolist()}")
```

Output:

```text
batch of 3: users [0, 1, 2]
batch of 3: users [3, 4, 5]
batch of 1: users [6]
```

The seventh event arrives alone. After `timeout_ms` with nothing else coming in, it is delivered as a batch of one rather than waiting forever. Swap `"webhook"` for `"websocket"`, `"kafka"`, or `"kinesis"` and change `config` (see [`nigiri`](#nigiri--streaming-source-adapters)); the loop stays the same.

### Example 9 — A complete extract-normalize-load job

```python
"""A complete ingestion job: API -> typed DataFrame -> partitioned files -> read back."""

from sushitruck import maki, sashimi, tobiko, wasabi

# 1. Extract: every todo, across pages, with polite rate limiting and retries.
client = maki.MakiClient("https://jsonplaceholder.typicode.com", rate_limit=5.0, retries=3)
raw = client.fetch(
    "/todos", paginate=True, pagination="page",
    page_param="_page", page_size_param="_limit", page_size=50,
)

# 2. Normalize: enforce types, rename columns, and fail loudly on bad data.
todos = wasabi.normalize(raw, {
    "id":        {"dtype": int, "nullable": False, "rename": "todo_id"},
    "userId":    {"dtype": int, "nullable": False, "rename": "user_id"},
    "title":     {"dtype": str},
    "completed": {"dtype": bool, "default": False},
}, strict=True)

# 3. Load: one file per user.
result = tobiko.send(todos, "warehouse/todos/", format="jsonl", partition_by="user_id")
print(f"wrote {result.rows_written} rows to {len(result.targets_written)} partitions "
      f"in {result.elapsed_s:.2f}s")

# 4. Read one partition back.
user_3 = sashimi.read("warehouse/todos/user_id=3/part.jsonl")
print(f"user 3 has {len(user_3)} todos, {int(user_3['completed'].sum())} completed")
```

Output:

```text
wrote 200 rows to 10 partitions in 0.01s
user 3 has 20 todos, 7 completed
```

This is the shape of most real ingestion jobs. To point it at your own systems, change the base URL and `auth=`, adjust the schema to your fields, and change the output path to `s3://...` or `gs://...`.

---

## The belt: module overview

Every module is importable directly from the package: `from sushitruck import maki, sashimi, ...`.

| Module | Named for | Purpose | Main entry points |
|---|---|---|---|
| [`maki`](#maki--rest-api-client) | the neat, tightly wrapped roll | REST API client: auth, pagination, rate limiting, retry | `MakiClient`, `.get()`, `.post()`, `.fetch()` |
| [`sashimi`](#sashimi--file-and-object-store-reader) | the raw, unadorned slice | Read local / S3 / GCS files, whole or in chunks | `read()`, `list_objects()` |
| [`wasabi`](#wasabi--json-flattening-and-schema-enforcement) | the sharp, clarifying hit | Flatten nested JSON; enforce and infer schemas | `flatten()`, `normalize()`, `infer_schema()` |
| [`gari`](#gari--rate-limiting-retry-and-circuit-breaking) | the palate cleanser between pieces | Rate limiting, retry with backoff, circuit breaker | `@rate_limit`, `@retry`, `@circuit_breaker` |
| [`temaki`](#temaki--batch-ingestion-coordinator) | the hand roll you assemble yourself | Combine many files, globs, and API calls into one result | `TemakiJob` |
| [`tobiko`](#tobiko--output-router) | the tiny roe scattered on top | Write DataFrames to local / S3 / GCS; publish rows to Kafka / Kinesis | `send()`, `publish()` |
| [`nigiri`](#nigiri--streaming-source-adapters) | one clean piece at a time | Consume webhook, WebSocket, Kafka, or Kinesis streams as micro-batches | `stream()` |

The package also exports the most-used classes at the top level:

```python
from sushitruck import MakiClient, TemakiJob, TemakiResult, TobikoResult
import sushitruck
print(sushitruck.__version__)
```

---

## `maki` — REST API client

`MakiClient` wraps a `requests.Session` pointed at one base URL. It handles authentication, query parameters, pagination, rate limiting, retry with exponential backoff, and converting responses into DataFrames.

### Creating a client

```python
from sushitruck import maki

client = maki.MakiClient(
    "https://api.example.com/v2",
    auth={"type": "bearer", "token": "my-token"},
    headers={"Accept": "application/json"},
    timeout=30,
    rate_limit=5.0,
    retries=4,
    retry_on=(429, 500, 502, 503, 504),
)
```

| Parameter | Default | Description |
|---|---|---|
| `base_url` | *(required)* | Base URL that endpoint paths are joined to. A trailing `/` is ignored. |
| `auth` | `None` | Auth spec dict — see [Authentication](#authentication). |
| `headers` | `None` | Extra headers sent with every request. |
| `timeout` | `30` | Per-request timeout in seconds. |
| `rate_limit` | `None` | Maximum requests per second (token bucket). `None` disables throttling. |
| `retries` | `3` | Total attempts per request, *including* the first one. |
| `retry_on` | `(429, 500, 502, 503, 504)` | HTTP status codes that trigger a retry. |
| `session` | `None` | Bring your own `requests.Session` (for proxies, custom adapters, mTLS, etc.). One is created if omitted. |

A single session is reused for every call on the client, so connections are pooled.

### Authentication

| `auth["type"]` | Required keys | What is sent |
|---|---|---|
| `"bearer"` | `token` | `Authorization: Bearer <token>` |
| `"basic"` | `username`, `password` | `Authorization: Basic <base64(username:password)>` |
| `"api_key"` | `key`, plus `header` **or** `param` | The key in a header (default `X-API-Key`) or as a query parameter |
| `"oauth2"` | `token_url`, `client_id`, `client_secret` | OAuth2 client-credentials flow: fetches a token, caches it, refreshes it automatically before it expires |

```python
# Bearer token
maki.MakiClient(url, auth={"type": "bearer", "token": "abc123"})

# HTTP Basic
maki.MakiClient(url, auth={"type": "basic", "username": "me", "password": "s3cret"})

# API key in a custom header
maki.MakiClient(url, auth={"type": "api_key", "key": "abc123", "header": "X-Api-Token"})

# API key as a query parameter (?apikey=abc123)
maki.MakiClient(url, auth={"type": "api_key", "key": "abc123", "param": "apikey"})

# OAuth2 client credentials
maki.MakiClient(url, auth={
    "type": "oauth2",
    "token_url": "https://auth.example.com/oauth/token",
    "client_id": "my-client",
    "client_secret": "my-secret",
})
```

For OAuth2, the token response's `expires_in` is honored (default 3600 seconds), and the token is refreshed 30 seconds early so a request never goes out with an expired token. An unsupported `type` raises `ValueError` when the client is created, not on the first request.

> **Tip:** keep secrets out of source code — read them from environment variables, e.g. `{"type": "bearer", "token": os.environ["API_TOKEN"]}`.

### `get()`, `post()`, and `fetch()`

```python
# Raw parsed JSON (dict or list)
body = client.get("/prices", params={"symbol": "NVDA"})

# Extract a nested value with dot notation: body["data"]["prices"]
prices = client.get("/prices", params={"symbol": "NVDA"}, results_key="data.prices")

# POST a JSON body (or form data via body=)
created = client.post("/orders", json={"symbol": "NVDA", "qty": 10})

# The one most callers want: GET -> results_key -> flatten -> DataFrame
df = client.fetch("/prices", params={"symbol": "NVDA"}, results_key="data.prices")
```

- `get()` returns the parsed JSON body, or the value at `results_key` if given. With `paginate=True` it returns a list of results from every page.
- `post(endpoint, *, body=None, json=None)` sends form data (`body=`) or JSON (`json=`) and returns the parsed response.
- `fetch()` takes the same arguments as `get()` plus `flatten=True`. It runs the results through [`wasabi.flatten`](#flatten--nested-json-to-columns), so nested objects become columns like `meta_source`. Pass `flatten=False` to keep nested values as Python objects.

`results_key` raises `KeyError` with a clear message if the path isn't in the response, so a changed API shape fails loudly instead of returning an empty DataFrame.

### Pagination

Pass `paginate=True` to follow pages until the API runs out of data. Choose the strategy that matches your API with `pagination=`:

| `pagination=` | How it requests pages | When it stops | Parameters |
|---|---|---|---|
| `"page"` | `?page=1`, `?page=2`, ... | A page returns no results | `page_param="page"`, `page_size_param="per_page"` |
| `"offset"` | `?offset=0&limit=100`, `?offset=100&limit=100`, ... | A page returns fewer than `page_size` results | `offset_param="offset"`, `page_size_param="limit"` |
| `"cursor"` | Sends the response's `next_cursor` / `next_token` back as `?cursor=...` | A response has no cursor | `cursor_param="cursor"` |
| `"link"` | Follows the `Link: <url>; rel="next"` header (RFC 8288, used by GitHub and others) | No `rel="next"` link | — |
| `"auto"` *(default)* | Starts with page numbers; switches to cursor or Link-header mode as soon as a response carries one | Whatever the detected strategy says | all of the above |

`page_size` (default `100`) is sent as the page-size parameter for every strategy.

```python
# Page numbers: ?page=N&per_page=200
df = client.fetch("/transactions", paginate=True, pagination="page",
                  page_size=200, results_key="data.transactions")

# Offset/limit: ?offset=N&limit=500
df = client.fetch("/events", paginate=True, pagination="offset",
                  page_size=500, results_key="items")

# Offset/limit with non-standard names: ?skip=N&take=50
df = client.fetch("/rows", paginate=True, pagination="offset",
                  offset_param="skip", page_size_param="take", page_size=50)

# Cursor tokens: the API returns {"items": [...], "next_cursor": "abc"}
df = client.fetch("/feed", paginate=True, pagination="cursor",
                  cursor_param="after", results_key="items")

# Link headers (absolute "next" URLs are followed as-is)
df = client.fetch("/repos/org/project/issues", paginate=True, pagination="link")
```

`results_key` is applied to **every page**, so `results_key="data.transactions"` collects `page["data"]["transactions"]` from each response and concatenates them.

> **Note:** `"auto"` is convenient for exploration, but naming the strategy is more predictable in production. It also avoids an extra request: with `"auto"`, a page-numbered API is only known to be finished when it returns an empty page.

### Rate limiting and retry

- **Rate limiting** uses a thread-safe token bucket (from [`gari`](#gari--rate-limiting-retry-and-circuit-breaking)). With `rate_limit=10.0`, calls are smoothed to at most 10 per second, and threads sharing a client share the same bucket.
- **Retry** re-sends a request whose response status is in `retry_on`. Waits grow exponentially (2 s, 4 s, 8 s, ...) with random jitter so many clients don't retry in lockstep. `retries` is the *total* number of attempts.
- After the last attempt, or for any status not in `retry_on` (like `404`), `requests.HTTPError` is raised via `raise_for_status()`.

```python
import requests

try:
    df = client.fetch("/maybe-missing")
except requests.HTTPError as exc:
    print("API error:", exc.response.status_code)
```

---

## `sashimi` — file and object store reader

`sashimi.read()` loads CSV, JSON, JSON Lines, or Parquet from local disk, S3, or GCS, either all at once or as a generator of chunks.

### Reading files

```python
from sushitruck import sashimi

# Whole file -> one DataFrame
df = sashimi.read("prices.csv")

# Big file -> generator of 50,000-row DataFrames (memory stays flat)
for chunk in sashimi.read("big_trades.csv", chunksize=50_000):
    process(chunk)

# Any pandas reader option passes straight through
df = sashimi.read("prices.csv", usecols=["date", "close"], parse_dates=["date"], dtype={"close": "float64"})

# A .txt or .dat file that's really CSV: override detection
df = sashimi.read("export.dat", format="csv", sep="|")

# Normalize every chunk on the way in
schema = {"close": {"dtype": float, "nullable": False}}
for chunk in sashimi.read("prices.csv", chunksize=10_000, schema=schema):
    ...
```

| Parameter | Default | Description |
|---|---|---|
| `path` | *(required)* | Local path, `s3://bucket/key`, or `gs://bucket/key` |
| `format` | auto | `"csv"`, `"json"`, `"jsonl"`, or `"parquet"`; detected from the extension if omitted |
| `chunksize` | `None` | Rows per chunk. When set, `read()` returns a **generator** (CSV and JSON Lines only) |
| `schema` | `None` | A [`wasabi` schema](#normalize--schema-enforcement) applied to the result or to every chunk |
| `storage` | `"local"` | `"local"`, `"s3"`, or `"gcs"` |
| `storage_options` | `None` | Passed to the cloud client — e.g. `{"region_name": "us-east-1"}` for S3, `{"project": "my-project"}` for GCS |
| `**read_kwargs` | | Forwarded to `pd.read_csv` / `pd.read_json` / `pd.read_parquet` |

**Format detection:**

| Extension | Format | pandas reader | Chunkable? |
|---|---|---|---|
| `.csv`, `.tsv`, `.txt` | `csv` | `pd.read_csv` | Yes |
| `.json` | `json` | `pd.read_json` | No |
| `.jsonl`, `.ndjson` | `jsonl` | `pd.read_json(lines=True)` | Yes |
| `.parquet` | `parquet` | `pd.read_parquet` | No |

For `.tsv` files, pass `sep="\t"`. Asking for `chunksize` on JSON or Parquet raises `ValueError` rather than silently loading the whole file.

### Reading from S3 and GCS

Requires `pip install "sushitruck[cloud]"`. Set `storage=` explicitly to match the URI:

```python
# S3 — credentials come from the normal boto3 chain (env vars, ~/.aws, instance role)
df = sashimi.read(
    "s3://my-bucket/data/prices.parquet",
    storage="s3",
    storage_options={"region_name": "us-east-1"},
)

# S3, chunked
for chunk in sashimi.read("s3://my-bucket/logs/events.jsonl", storage="s3", chunksize=10_000):
    ...

# GCS — credentials come from Application Default Credentials
df = sashimi.read("gs://my-bucket/exports/users.csv", storage="gcs",
                  storage_options={"project": "my-project"})
```

> Cloud objects are downloaded into memory before parsing. `chunksize` keeps the *DataFrames* small, but the raw object is held in memory while it's read. For very large objects, split them into several smaller objects and read them with [`temaki`](#temaki--batch-ingestion-coordinator).

### Listing objects

```python
sashimi.list_objects("data/", pattern="*.csv")
# ['data/a.csv', 'data/b.csv']

sashimi.list_objects("s3://my-bucket/prices/", storage="s3", pattern="*.parquet")
# ['s3://my-bucket/prices/2024-01.parquet', 's3://my-bucket/prices/2024-02.parquet', ...]

sashimi.list_objects("gs://my-bucket/exports/", storage="gcs")
```

`pattern` is a glob matched against each object's file name. Results are always sorted, and S3 listings follow every page of results, so prefixes with more than 1,000 objects are listed completely.

---

## `wasabi` — JSON flattening and schema enforcement

`wasabi` turns raw payloads into tidy, typed DataFrames. Other modules call it whenever you pass `schema=`, and you can use it on its own.

### `flatten` — nested JSON to columns

```python
from sushitruck import wasabi

wasabi.flatten({"user": {"id": 1, "name": "Alice", "tags": ["a", "b"]}, "score": 0.9, "empty": None})
#    user_id user_name user_tags  score
# 0        1     Alice    [a, b]    0.9
```

- Accepts a dict, a list of dicts, or a raw JSON string.
- Nested objects become `parent_child` columns. Lists are kept as values, not exploded into extra rows.
- `separator="."` gives `user.id`-style names instead.
- `max_depth=1` flattens one level and leaves deeper objects as dict values.
- `drop_empty=True` (default) drops columns that are `None` or `[]` in every record (`empty` above).

### `normalize` — schema enforcement

```python
schema = {
    "price":     {"dtype": float, "nullable": False, "rename": "close_price"},
    "volume":    {"dtype": int, "default": 0},
    "symbol":    {"dtype": str, "nullable": False},
    "timestamp": {"dtype": "datetime64[ns]", "nullable": False},
}

clean = wasabi.normalize(raw_df, schema)
```

For each column in the schema, in order: fill `default` → coerce to `dtype` → check `nullable` → apply `rename`.

| Spec key | Meaning |
|---|---|
| `"dtype"` | Target type: a Python type (`int`, `float`, `str`, `bool`), a numpy/pandas dtype string (`"int64"`, `"float32"`, `"category"`, ...), or `"datetime64[ns]"` (parsed with `pd.to_datetime`) |
| `"nullable"` | `False` raises `ValueError` if any nulls remain after `default` is filled. Defaults to `True` |
| `"default"` | Value used to fill nulls before coercion |
| `"rename"` | New column name, applied last |

| Option | Default | Behavior |
|---|---|---|
| `strict` | `False` | `True` raises `ValueError` if the DataFrame has columns the schema doesn't mention. `False` keeps them unchanged and issues a `UserWarning` naming them |
| `coerce` | `True` | Set `False` to only check nulls and rename, without changing types |

A schema column that's missing from the DataFrame always raises `ValueError`. The input DataFrame is never modified.

> **Integers and nulls:** coercing a column with nulls to `int` fails, as it does in pandas. Give it a `"default"`, or use the nullable dtype `"Int64"`.

To silence the extra-columns warning for a known case, use the standard library:

```python
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    clean = wasabi.normalize(df, schema)
```

### `infer_schema` — bootstrap a schema

```python
schema = wasabi.infer_schema(sample_df, sample_n=1000)
# {'price': {'dtype': <class 'float'>, 'nullable': False},
#  'symbol': {'dtype': <class 'str'>, 'nullable': False}, ...}
```

Inspects the first `sample_n` rows (`None` for all of them) and returns a schema ready for `normalize()`. Use it as a starting point: generate it once, edit it by hand (add renames, tighten `nullable`, turn date strings into `"datetime64[ns]"`), then keep it in your code.

---

## `gari` — rate limiting, retry, and circuit breaking

`gari` is the reliability layer: pure standard library, always available, and usable on **any** function, not just SushiTruck's own.

### `@rate_limit`

```python
from sushitruck import gari

@gari.rate_limit(calls_per_second=5.0)
def fetch_quote(symbol):
    ...
```

A thread-safe token bucket per decorated function. Bursts up to `calls_per_second` calls are allowed; after that, callers sleep until a token frees up. For a limiter shared across several functions, create one directly:

```python
limiter = gari.RateLimiter(calls_per_second=2.0)

def call_a():
    limiter.sleep_until_ready()
    ...

def call_b():
    limiter.sleep_until_ready()
    ...
```

### `@retry`

```python
import requests

@gari.retry(max_attempts=4, backoff_base=2.0, jitter=True,
            retry_on=(429, 503, requests.ConnectionError, TimeoutError))
def fetch_price(symbol):
    return requests.get(f"https://api.example.com/price/{symbol}", timeout=10)
```

| Parameter | Default | Description |
|---|---|---|
| `max_attempts` | `3` | Total attempts, including the first |
| `backoff_base` | `2.0` | The wait before retry *n* is `backoff_base ** n` seconds |
| `jitter` | `True` | Multiplies each wait by a random factor between 0.5 and 1.5 |
| `retry_on` | `(429, 500, 502, 503, 504)` | Any mix of **HTTP status codes** (checked against a returned `requests.Response`) and **exception types** |

| Attempt | Wait before it (base 2.0, no jitter) |
|---|---|
| 1 | — |
| 2 | 2 s |
| 3 | 4 s |
| 4 | 8 s |

If attempts run out on an exception, the last exception is re-raised. If they run out on a retryable status code, the last response is returned, so you can inspect it or call `raise_for_status()`. Exceptions not listed in `retry_on` pass straight through on the first failure.

### `@circuit_breaker`

```python
@gari.circuit_breaker(failure_threshold=5, recovery_timeout=60)
def call_flaky_service():
    ...

try:
    call_flaky_service()
except gari.CircuitBreakerOpenError:
    # Too many recent failures: fail fast instead of piling on.
    use_cached_value()
```

- **Closed** (normal): calls pass through and consecutive failures are counted.
- **Open**: after `failure_threshold` consecutive failures, calls raise `CircuitBreakerOpenError` immediately, without running the function.
- **Half-open**: after `recovery_timeout` seconds, the next call is tried. Success closes the circuit; failure opens it again.

### Stacking decorators

All three compose, and order matters: decorators apply from the bottom up, and the top one runs first. Put `retry` on top so every attempt passes through the layers below it, `rate_limit` next so each attempt — including retries — waits for a token, and `circuit_breaker` at the bottom so each attempt counts toward the failure threshold:

```python
@gari.retry(max_attempts=4, retry_on=(429, 503, ConnectionError))
@gari.rate_limit(calls_per_second=5.0)
@gari.circuit_breaker(failure_threshold=10, recovery_timeout=30)
def call_api():
    ...
```

With `rate_limit` on top instead, only the first attempt would wait for a token, and retries would bypass the limit.

### Helpers

```python
gari.backoff_sleep(attempt=2, base=2.0, jitter=True)  # sleeps ~4 s; returns seconds slept
gari.sleep_until_ready(calls_per_second=10.0)         # one-off bucket; returns immediately
```

---

## `temaki` — batch ingestion coordinator

`TemakiJob` gathers many sources — files, glob patterns, S3/GCS prefixes, and API endpoints — and returns one combined result. It is the batch-mode counterpart to `nigiri`'s streaming.

```python
from sushitruck import temaki, maki

client = maki.MakiClient("https://api.example.com", auth={"type": "bearer", "token": "..."})

job = (
    temaki.TemakiJob(workers=4, on_error="warn", schema={"price": {"dtype": float}})
    .add_glob("s3://my-bucket/prices/*.parquet", storage="s3",
              storage_options={"region_name": "us-east-1"})
    .add_api(client, "/supplemental", paginate=True, pagination="cursor", results_key="data")
    .add_file("local_overrides.csv")
)

result = job.run()
df = result.df
print(f"{result.sources_processed} sources, {result.total_rows} rows, {result.elapsed_s:.1f}s")
for failure in result.sources_failed:
    print("failed:", failure["source"], "-", failure["error"])
```

### Constructor

| Parameter | Default | Description |
|---|---|---|
| `workers` | `1` | `1` reads sources one after another. `>1` reads them concurrently in a thread pool (ideal for I/O-bound sources like S3 and APIs) |
| `schema` | `None` | `wasabi` schema applied to every source after it's read |
| `on_error` | `"raise"` | `"raise"` stops on the first failure. `"warn"` issues a `UserWarning`, records the failure, and continues. `"skip"` records the failure silently and continues |

### Adding sources

Every `add_*` method returns the job, so calls chain.

| Method | Adds |
|---|---|
| `add_file(path, *, format=None, chunksize=None, storage="local", storage_options=None, **read_kwargs)` | One file, read with [`sashimi.read`](#reading-files) |
| `add_glob(pattern, *, storage="local", storage_options=None, **read_kwargs)` | Every file matching a pattern such as `data/*.csv` or `s3://bucket/prefix/*.parquet`. The pattern is resolved when `add_glob` is called |
| `add_api(client, endpoint, *, params=None, paginate=False, results_key=None, **fetch_kwargs)` | One endpoint, read with [`MakiClient.fetch`](#get-post-and-fetch). `fetch_kwargs` passes pagination options such as `pagination="offset"` or `page_size=500` |

### Running

- **`run()`** reads everything and returns a [`TemakiResult`](#result-objects) whose `.df` is the combined DataFrame. With `workers>1`, row order follows the order in which sources *finish*, not the order they were added. Sort afterward if order matters.
- **`stream()`** reads sources one at a time and yields a DataFrame per source, or per chunk for files added with `chunksize`. Use it when the combined result won't fit in memory. It always runs sequentially and honors `on_error` the same way.

```python
for batch in job.stream():
    tobiko.send(batch, "output/combined.csv", format="csv", mode="append")
```

---

## `tobiko` — output router

`tobiko` delivers a finished DataFrame wherever it needs to go: files, object stores, or message queues.

### `send()` — write files

```python
from sushitruck import tobiko

# One local file (parent directories are created as needed)
tobiko.send(df, "output/prices.parquet")

# Several targets at once
tobiko.send(df, ["output/prices.csv", "s3://my-bucket/prices.csv"], format="csv")

# Hive-partitioned output: output/trades/date=2024-01-01/part.parquet, ...
tobiko.send(df, "output/trades/", format="parquet", partition_by="date")

# Append to an existing CSV or JSON Lines file
tobiko.send(new_rows, "output/log.jsonl", format="jsonl", mode="append")

# S3 / GCS (requires [cloud])
tobiko.send(df, "s3://my-bucket/processed/trades/", partition_by="date",
            storage_options={"region_name": "us-east-1"})
tobiko.send(df, "gs://my-bucket/processed/users.parquet")
```

| Parameter | Default | Description |
|---|---|---|
| `target` | *(required)* | A path/URI or a list of them. `s3://` and `gs://` go to the cloud; anything else is a local path |
| `format` | `"parquet"` | `"parquet"`, `"csv"`, `"json"` (a JSON array of records), or `"jsonl"` |
| `mode` | `"overwrite"` | `"overwrite"` or `"append"`. Append works for local `csv`/`jsonl` files only (object stores have no append, so it raises `ValueError` for `s3://` / `gs://` targets); CSV appends skip the header row |
| `partition_by` | `None` | Column to split output by. `target` is treated as a directory and one `col=value/part.<format>` file is written per value, in the layout Athena, Spark, Hive, and DuckDB read natively |
| `storage_options` | `None` | Passed to the S3 / GCS client |

`send()` returns a [`TobikoResult`](#result-objects) listing every file written, the row and byte counts, and the elapsed time.

### `publish()` — send rows to a queue

Each row becomes one message, serialized as JSON by default.

```python
# Kafka (requires [kafka]); config is passed to confluent_kafka.Producer
n = tobiko.publish(df, "kafka://processed-trades",
                   config={"bootstrap.servers": "localhost:9092"})

# Kinesis (requires [kinesis]); config is boto3 client kwargs plus an optional partition_key
n = tobiko.publish(df, "kinesis://market-events",
                   config={"region_name": "us-east-1", "partition_key": "trades"})

# Custom serialization
import msgpack
n = tobiko.publish(df, "kafka://t", config=cfg, serializer=lambda row: msgpack.packb(row))
```

`publish()` returns the number of messages sent. Kafka producers are flushed before it returns. Kinesis records all share one `partition_key` (default `"sushitruck"`).

> `df.to_dict(orient="records")` produces each row dict, so timestamps and other non-JSON types need a `serializer` that handles them, e.g. `lambda row: json.dumps(row, default=str).encode()`.

---

## `nigiri` — streaming source adapters

`nigiri.stream()` connects to a continuous source and yields **micro-batched DataFrames**. Every source has the same interface: a generator you consume with a `for` loop.

```python
from sushitruck import nigiri

for batch in nigiri.stream("kafka", config=kafka_config, batch_size=1_000, timeout_ms=2_000):
    handle(batch)   # a pd.DataFrame of up to 1,000 rows
```

| Parameter | Default | Description |
|---|---|---|
| `source` | *(required)* | `"webhook"`, `"websocket"`, `"kafka"`, or `"kinesis"` |
| `config` | *(required)* | Source-specific settings — see below |
| `batch_size` | `500` | Maximum rows per DataFrame |
| `timeout_ms` | `1000` | Maximum wait for a full batch. When it expires, whatever has arrived is yielded as a partial batch, so a quiet stream never stalls your loop |
| `schema` | `None` | Each batch is run through `wasabi.flatten` and then `wasabi.normalize` with this schema |
| `deserializer` | `json.loads` | Turns one raw message (bytes or str) into a record dict. Use it for Avro, Protobuf, MessagePack, CSV lines, ... |
| `max_batches` | `None` | Stop after this many batches. `None` runs until you break out, or until the source closes |

Closing the generator — calling `.close()`, or breaking out of a `for` loop that owns it — shuts down the underlying server, connection, or consumer.

### Sources and their `config`

| Source | Extra | `config` keys |
|---|---|---|
| `"webhook"` | *(core)* | `host` (default `"0.0.0.0"`), `port` (default `8080`), `path` (default `"/ingest"`) |
| `"websocket"` | `[websocket]` | `uri` (required), `headers` (optional dict) |
| `"kafka"` | `[kafka]` | `topic` (required), plus any `confluent_kafka.Consumer` setting such as `bootstrap.servers`, `group.id`, `auto.offset.reset` |
| `"kinesis"` | `[kinesis]` | `stream_name`, `shard_id` (required), `region_name`, `iterator_type` (default `"TRIM_HORIZON"`) |

**Webhook** — starts a small HTTP server and turns every `POST` to `path` into one record. `POST`s to other paths get a `404`.

```python
for batch in nigiri.stream("webhook", config={"host": "127.0.0.1", "port": 8080, "path": "/ingest"}):
    tobiko.send(batch, "landing/events.jsonl", format="jsonl", mode="append")
```

> The webhook listener is a plain HTTP server with no TLS and no authentication. Bind it to `127.0.0.1` or a private network, and put a reverse proxy in front of it if it must face the internet.

**WebSocket** — connects to `uri` and treats each text or binary message as one record.

```python
config = {"uri": "wss://data.example.com/feed", "headers": {"Authorization": "Bearer <token>"}}
for batch in nigiri.stream("websocket", config=config, batch_size=200, timeout_ms=500):
    ...
```

**Kafka** — subscribes to `topic` as a consumer. The consumer is closed when the generator ends, and a broker error raises `RuntimeError`.

```python
config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "sushitruck-consumer",
    "auto.offset.reset": "earliest",
    "topic": "trades",
}
for batch in nigiri.stream("kafka", config=config, batch_size=1_000):
    ...
```

**Kinesis** — reads one shard from the chosen iterator position. The stream ends when the shard is closed and fully read.

```python
config = {
    "stream_name": "market-events",
    "shard_id": "shardId-000000000000",
    "region_name": "us-east-1",
    "iterator_type": "LATEST",
}
for batch in nigiri.stream("kinesis", config=config, max_batches=100):
    ...
```

---

## Result objects

Both result types are frozen dataclasses, importable from the top-level package.

**`TemakiResult`** — returned by `TemakiJob.run()`

| Field | Type | Description |
|---|---|---|
| `df` | `pd.DataFrame` | All sources combined |
| `sources_processed` | `int` | Sources read successfully |
| `sources_failed` | `list[dict]` | `{"source": label, "error": message}` for each failure (with `on_error="warn"` or `"skip"`) |
| `total_rows` | `int` | Rows in `df` |
| `elapsed_s` | `float` | Wall-clock time for the job |

**`TobikoResult`** — returned by `tobiko.send()`

| Field | Type | Description |
|---|---|---|
| `targets_written` | `list[str]` | Every file or object written (one per partition when `partition_by` is used) |
| `rows_written` | `int` | Total rows written across all targets |
| `bytes_written` | `int` | Total bytes written |
| `elapsed_s` | `float` | Wall-clock time |

---

## Recipes

### API to partitioned Parquet on S3

```python
import os
from sushitruck import maki, wasabi, tobiko

client = maki.MakiClient(
    "https://api.example.com/v2",
    auth={"type": "bearer", "token": os.environ["API_TOKEN"]},
    rate_limit=5.0,
)

raw = client.fetch("/orders", params={"since": "2024-01-01"},
                   paginate=True, pagination="cursor", results_key="orders")

orders = wasabi.normalize(raw, {
    "id":         {"dtype": str, "nullable": False},
    "amount":     {"dtype": float, "default": 0.0},
    "created_at": {"dtype": "datetime64[ns]", "nullable": False},
}, strict=False)
orders["date"] = orders["created_at"].dt.date.astype(str)

tobiko.send(orders, "s3://lake/raw/orders/", partition_by="date",
            storage_options={"region_name": "us-east-1"})
```

### A folder of messy CSVs to one clean DataFrame

```python
from sushitruck import temaki

schema = {
    "date":  {"dtype": "datetime64[ns]", "nullable": False},
    "sku":   {"dtype": str, "nullable": False},
    "units": {"dtype": int, "default": 0},
}

result = (
    temaki.TemakiJob(workers=8, on_error="skip", schema=schema)
    .add_glob("exports/2024/*.csv", usecols=["date", "sku", "units"])
    .run()
)
sales = result.df.sort_values("date", ignore_index=True)
print(f"{len(result.sources_failed)} files skipped")
```

### Kafka topic to hourly files

```python
from datetime import datetime, timezone
from sushitruck import nigiri, tobiko

config = {"bootstrap.servers": "broker:9092", "group.id": "archiver", "topic": "clicks"}
schema = {"user_id": {"dtype": str}, "ts": {"dtype": "datetime64[ns]"}}

for batch in nigiri.stream("kafka", config=config, batch_size=5_000, timeout_ms=10_000, schema=schema):
    hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
    tobiko.send(batch, f"archive/clicks/{hour}.jsonl", format="jsonl", mode="append")
```

### Wrapping any third-party call with resilience

```python
from sushitruck import gari

@gari.retry(max_attempts=5, retry_on=(ConnectionError, TimeoutError))
@gari.rate_limit(calls_per_second=1.0)
@gari.circuit_breaker(failure_threshold=3, recovery_timeout=120)
def geocode(address):
    return some_sdk.geocode(address)
```

---

## Errors and troubleshooting

| You see | Why | Fix |
|---|---|---|
| `ImportError: S3 support requires boto3. Install it with: pip install sushitruck[cloud]` | The feature's optional extra isn't installed | Run the command in the message |
| `ImportError` about `pyarrow` / `fastparquet` | Parquet needs a pandas Parquet engine | `pip install pyarrow` |
| `ValueError: Could not auto-detect format` | Unrecognized file extension | Pass `format="csv"` (etc.) |
| `ValueError: chunksize is not supported for format 'parquet'` | Only CSV and JSON Lines can be chunked | Drop `chunksize`, or convert to CSV / JSON Lines |
| `KeyError: results_key 'data.items' not found in response` | The response doesn't have that path | Inspect `client.get(endpoint)` and fix `results_key` |
| `requests.HTTPError` | Non-retryable status, or retries ran out | Check `exc.response.status_code` and the API's docs |
| `ValueError: Column 'x' is non-nullable but contains null values.` | A schema column marked `nullable: False` has nulls | Add a `"default"`, or relax `nullable` |
| `ValueError: Schema columns missing from DataFrame` | The schema names a column the data doesn't have | Check spelling, or check that `flatten` produced the name you expect |
| `UserWarning: Columns not in schema were kept unchanged` | `normalize(strict=False)` found extra columns | Add them to the schema, select them away, use `strict=True`, or filter the warning |
| `gari.CircuitBreakerOpenError` | Too many consecutive failures | Wait `recovery_timeout`, or handle it with a fallback |
| `s3://...` path treated as a local file | `storage` defaults to `"local"` | Pass `storage="s3"` (or `"gcs"`) |

---

## Design principles

- **Generator-first.** Streaming sources and chunked reads return generators, never fully loaded data. You decide how much to consume.
- **Normalize at the boundary.** Raw payloads are flattened and schema-checked *inside* SushiTruck, so downstream code receives clean, typed DataFrames.
- **No mutation.** Inputs are never modified: `normalize()` returns a new DataFrame, and `gari` decorators wrap your function without changing it.
- **Optional extras, not mandatory bloat.** Kafka, AWS, and GCS clients add tens of megabytes of dependencies. None of them is needed for the common case.
- **Explicit over magic.** You choose the pagination strategy, the error policy, and the strictness of schema checks. Extra columns are never dropped silently: they're kept with a warning, or rejected with `strict=True`.
- **Fully typed.** The package ships a `py.typed` marker (PEP 561), so mypy and pyright use its annotations.

---

## Using SushiTruck with other packages

SushiTruck is one of a small family of independent packages that share one convention: `pandas.DataFrame` in, `pandas.DataFrame` out.

| Package | Role |
|---|---|
| **sushitruck** *(this package)* | Ingestion: APIs, streams, files, object stores, normalization, output routing |
| [**thaitruck**](https://pypi.org/project/thaitruck/) | Batch DataFrame cleaning, merging, and profiling |
| [**ramentruck**](https://pypi.org/project/ramentruck/) | Machine-learning workbench: training, validation, explainability |

**None of them imports another.** Installing SushiTruck never installs the others, and SushiTruck has no code that knows they exist. They combine only in *your* application code, by passing DataFrames from one to the next:

```python
# Application code: each package is imported and used independently.
from sushitruck import maki, wasabi
import thaitruck

client = maki.MakiClient("https://api.example.com", auth={"type": "bearer", "token": "..."})
prices = wasabi.normalize(client.fetch("/prices", paginate=True), price_schema)

clean = thaitruck.orange_chicken(prices, heat=3)   # any DataFrame tool works here
```

Any DataFrame-based library — pandas itself, scikit-learn, DuckDB, Polars (via `pl.from_pandas`) — fits in the same place.

---

## Development

```bash
git clone https://github.com/sgtidwellgit/SushiTruck.git
cd SushiTruck
python -m pip install -e ".[dev,websocket]"

python -m pytest                                   # run the test suite
python -m pytest --cov=sushitruck --cov-report=term-missing
```

- Source code lives in `src/sushitruck/`, and there is one test file per module in `tests/`.
- HTTP calls are mocked with [`responses`](https://pypi.org/project/responses/). S3, GCS, Kafka, and Kinesis are replaced in tests by small in-memory stand-ins (see `tests/conftest.py`), so **the whole suite runs offline**, with no cloud credentials and no extras beyond `dev`.
- CI runs the suite on Python 3.9–3.13 (Linux) plus Windows and macOS, then builds the package and checks the distributions.

Building a release:

```bash
python -m build                     # dist/*.whl and dist/*.tar.gz
python -m twine check --strict dist/*
python -m twine upload dist/*
```

When bumping the version, update it in **both** `pyproject.toml` and `src/sushitruck/__init__.py`.

The full design document — each module's original specification, design notes, and roadmap — is in [`PROJECT.md`](https://github.com/sgtidwellgit/SushiTruck/blob/main/PROJECT.md).

---

## Changelog and versioning

SushiTruck follows [semantic versioning](https://semver.org/). While the major version is 0, minor releases may include small API changes, which are always listed in the changelog.

See [`CHANGELOG.md`](https://github.com/sgtidwellgit/SushiTruck/blob/main/CHANGELOG.md) for release notes.

---

## License

MIT — see [`LICENSE`](https://github.com/sgtidwellgit/SushiTruck/blob/main/LICENSE).

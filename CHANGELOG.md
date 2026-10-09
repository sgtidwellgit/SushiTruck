# Changelog

All notable changes to SushiTruck are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), Versions up to 0.2.3 used semantic versioning; from 2026.10.9 on, versions are the release date (`YEAR.MONTH.DAY`, with a fourth number for a second release on the same day). Breaking changes are always listed under **Breaking** or **Changed**.

## [2026.10.9] - 2026-10-09

First date-versioned release. It installs as an upgrade from 0.2.3: pip orders `2026.10.9` after every `0.x` version.

### Added

- `nigiri.stream("sse")` — a server-sent events source, needing nothing beyond the core install. It parses the SSE standard (multi-line data, comments, event types, ids, every line-ending style) and delivers events as they arrive. On a dropped, silent, or 5xx/408/429 connection it reconnects automatically, honoring the server's `retry:` delay and sending `Last-Event-ID` so the server can resume. Other 4xx errors are raised instead of retried; HTTP 204 ends the stream. Options: `events`, `reconnect`, `retry_ms`, `max_retries`, `read_timeout`, `headers`, `params`.
- `nigiri.stream("kinesis")` reads **every shard** when `shard_id` is omitted (paginating the shard list), rotating between shards so none starves the others, and follows child shards after a reshard so no records are missed. Passing `shard_id` keeps the previous single-shard behavior.
- `sashimi.read()`, `sashimi.list_objects()`, and `TemakiJob.add_file()` / `add_glob()` infer `storage` from the path: `s3://` is S3, `gs://` is GCS, anything else is local. An explicit `storage=` still takes precedence.
- README: Example 10 (live Wikipedia edits over SSE), and reference sections for SSE, multi-shard Kinesis, and streaming cloud reads.

### Changed

- **S3 and GCS reads now stream.** CSV and JSON Lines objects are parsed directly from the network, so a chunked read of a very large object uses about one chunk of memory instead of downloading the whole object first. JSON and Parquet objects are copied to a temporary buffer that spills to disk beyond 64 MB, instead of being held entirely in memory. Connections are closed when a read finishes or is abandoned. GCS objects are read with `blob.open("rb")`.
- When a schema is applied to many DataFrames — `nigiri` stream batches, chunked `sashimi` reads, and `TemakiJob` sources — the "columns not in schema" warning is shown **once** per stream, read, or job, instead of once per batch. Direct `wasabi.normalize()` calls still warn every time.
- The "columns not in schema" warning names at most 10 columns, followed by "and N more".

### Removed

- The design document no longer promises internal chunked reading when `chunksize` isn't set; it brought no benefit (see `PROJECT.md`).

## [0.2.3] - 2026-10-09

### Fixed

- The `[dev]` extra now installs `pyarrow`, so the Parquet tests pass in a fresh development environment (and in CI).
- `nigiri.stream("webhook")` no longer does a reverse-DNS lookup of the bind address when the listener starts. Where reverse DNS is slow (common on macOS), startup took seconds, and events sent in the meantime were refused.
- The webhook tests retry until the listener accepts connections instead of assuming a fixed startup time, so they can't hang on a slow machine.

### Changed

- CI uses `actions/checkout@v5` and `actions/setup-python@v6`.
- CI fails a stuck test after 120 seconds (via `pytest-timeout`, now in the `[dev]` extra) and a stuck job after 15 minutes, and prints each test name as it runs.

## [0.2.2] - 2026-10-09

### Added

- `maki` — offset/limit pagination (`pagination="offset"`), and an explicit `pagination=` choice of `"auto"`, `"page"`, `"offset"`, `"cursor"`, or `"link"`. New `offset_param` and `cursor_param` options name the query parameters.
- `maki.MakiClient.fetch()` — accepts every pagination option `get()` does (`pagination`, `page_param`, `page_size_param`, `page_size`, `offset_param`, `cursor_param`).
- `temaki.TemakiJob.add_api()` — forwards extra keyword arguments (e.g. `pagination=`, `page_size=`) to `fetch()`.
- `wasabi.normalize(strict=False)` — now issues a `UserWarning` naming any columns that aren't in the schema, as the design specifies (they are still kept).
- Continuous integration: tests on Python 3.9–3.13 (Linux), plus Windows and macOS, and a package build check.
- Tests for the S3, GCS, Kafka, Kinesis, and OAuth2 code paths, which run offline. Coverage rose from 75% to 96%.
- `LICENSE` file (MIT), included in the source distribution.
- A fully rewritten `README.md`: a runnable quick start, nine complete example programs (each with its real output) that use free public APIs and local files, and a complete reference for every module.
- Python 3.13 classifier; project URLs for the repository, issues, and changelog.

### Fixed

- `maki.MakiClient.fetch(paginate=True, results_key=...)` ignored `results_key` and collected whole response bodies. With page-number pagination, that meant it never stopped requesting pages. `results_key` is now applied to every page.
- `maki` Link-header pagination joined the absolute `next` URL onto `base_url`, requesting a malformed URL. Absolute URLs are now used as-is.
- `maki` cursor pagination kept sending `page=1` alongside the cursor and always named the parameter `cursor`. It now sends only the cursor, under a configurable name.
- `sashimi.read()` left local files open after reading. Files are now closed, including when a chunked read is abandoned partway.
- `tobiko.send(mode="append")` to an `s3://` or `gs://` target silently overwrote the object. It now raises `ValueError`, since object stores have no append.
- The `[websocket]` extra allowed `websockets` 11–13, which lack the connection API `nigiri` uses. The minimum is now 14.0.

### Changed

- `maki.MakiClient.get()` now applies `results_key` to non-paginated requests too, returning the extracted value rather than the whole body.

## [0.2.1] - 2026-09-09

### Fixed

- The source distribution is now built from an explicit allowlist of files (`src/`, `tests/`, and the top-level docs), so untracked local files in the working directory can never be packaged.

## [0.2.0] - 2026-07-31

First real release. Implements the full belt of modules described in `PROJECT.md`.

### Added

- `gari` — token-bucket rate limiting, retry with exponential backoff, and a circuit breaker. Pure stdlib.
- `wasabi` — nested JSON flattening (`flatten`), schema enforcement (`normalize`), and schema inference (`infer_schema`).
- `maki` — `MakiClient` REST client with bearer/basic/api_key/oauth2 auth, page-number/cursor/Link-header pagination, and built-in rate limiting and retry via `gari`.
- `sashimi` — `read()` and `list_objects()` for local files (CSV, JSON, JSONL, Parquet), with chunked/generator reads and optional S3/GCS support (`[cloud]`).
- `tobiko` — `send()` for writing DataFrames to local/S3/GCS targets (with Hive-style partitioning) and `publish()` for pushing rows to Kafka/Kinesis.
- `temaki` — `TemakiJob` batch ingestion coordinator that merges files, globs, and API endpoints into one DataFrame, sequentially or via a thread pool.
- `nigiri` — `stream()` generator for webhook and WebSocket sources (core), plus Kafka (`[kafka]`) and Kinesis (`[kinesis]`) streaming.
- `results` — `TemakiResult` and `TobikoResult` typed result containers.
- Optional extras: `kafka`, `kinesis`, `cloud`, `websocket`, `all`, `dev`.
- Full pytest suite covering every module.
- PEP 561 `py.typed` marker.

## [0.1.0] - 2026-06-15

Stub release to secure the `sushitruck` name on PyPI. No functionality.

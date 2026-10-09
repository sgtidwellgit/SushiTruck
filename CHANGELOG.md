# Changelog

All notable changes to SushiTruck are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [semantic versioning](https://semver.org/).

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

# Changelog

All notable changes to SushiTruck are documented here.

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

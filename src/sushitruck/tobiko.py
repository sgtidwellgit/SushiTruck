"""tobiko — routes a processed DataFrame to one or more downstream destinations."""

from __future__ import annotations

import io
import json
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .results import TobikoResult

_WRITERS: dict[str, Callable[[pd.DataFrame, io.BytesIO], None]] = {
    "parquet": lambda df, buf: df.to_parquet(buf),
    "csv": lambda df, buf: buf.write(df.to_csv(index=False).encode()),
    "json": lambda df, buf: buf.write(df.to_json(orient="records").encode()),
    "jsonl": lambda df, buf: buf.write(df.to_json(orient="records", lines=True).encode()),
}


def _storage_of(target: str) -> str:
    if target.startswith("s3://"):
        return "s3"
    if target.startswith("gs://"):
        return "gcs"
    return "local"


def _serialize(df: pd.DataFrame, format: str) -> bytes:
    if format not in _WRITERS:
        raise ValueError(f"Unsupported format: {format!r}. Supported: {sorted(_WRITERS)}")

    buf = io.BytesIO()
    _WRITERS[format](df, buf)
    return buf.getvalue()


def _write_local(df: pd.DataFrame, path: Path, format: str, mode: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)

    if mode == "append" and path.exists():
        if format not in ("csv", "jsonl"):
            raise ValueError(f"mode='append' is only supported for csv/jsonl, not {format!r}.")
        if format == "csv":
            payload = df.to_csv(index=False, header=False).encode()
        else:
            payload = df.to_json(orient="records", lines=True).encode()
        with open(path, "ab") as fh:
            fh.write(payload)
        return len(payload)

    payload = _serialize(df, format)
    path.write_bytes(payload)
    return len(payload)


def _split_uri(path: str) -> tuple[str, str]:
    without_scheme = path.split("://", 1)[1]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _write_s3(df: pd.DataFrame, target: str, format: str, storage_options: dict[str, Any] | None) -> int:
    try:
        import boto3
    except ImportError as e:
        raise ImportError(
            "S3 support requires boto3. Install it with: pip install sushitruck[cloud]"
        ) from e

    payload = _serialize(df, format)
    bucket, key = _split_uri(target)
    boto3.client("s3", **(storage_options or {})).put_object(Bucket=bucket, Key=key, Body=payload)
    return len(payload)


def _write_gcs(df: pd.DataFrame, target: str, format: str, storage_options: dict[str, Any] | None) -> int:
    try:
        from google.cloud import storage as gcs_storage
    except ImportError as e:
        raise ImportError(
            "GCS support requires google-cloud-storage. "
            "Install it with: pip install sushitruck[cloud]"
        ) from e

    payload = _serialize(df, format)
    bucket_name, key = _split_uri(target)
    client = gcs_storage.Client(**(storage_options or {}))
    client.bucket(bucket_name).blob(key).upload_from_string(payload)
    return len(payload)


def _target_path(target: str, partition_value: Any, partition_by: str | None, format: str) -> str:
    if partition_by is None:
        return target

    base = target.rstrip("/")
    return f"{base}/{partition_by}={partition_value}/part.{format}"


def send(
    df: pd.DataFrame,
    target: str | list[str],
    *,
    format: str = "parquet",
    mode: str = "overwrite",
    partition_by: str | None = None,
    storage_options: dict[str, Any] | None = None,
) -> TobikoResult:
    """
    Write a DataFrame to one or more local, S3, or GCS targets.

    Parameters
    ----------
    df
        DataFrame to write. Not mutated.
    target
        A path/URI, or a list of them to write the same data to multiple
        destinations. Recognizes ``s3://`` and ``gs://`` prefixes; anything
        else is treated as a local path.
    format
        ``"parquet"``, ``"csv"``, ``"json"``, or ``"jsonl"``.
    mode
        ``"overwrite"`` (default) or ``"append"``. Append is only supported
        for ``csv``/``jsonl`` targets.
    partition_by
        Column name to partition output by by value, following the Hive
        convention (``target/col=value/part.<format>``). When set, ``target``
        is treated as a directory prefix rather than a single file path.
    storage_options
        Backend-specific credentials/config for S3 or GCS targets.

    Returns
    -------
    TobikoResult
        Targets written, total rows written, total bytes written (when
        determinable), and elapsed time.
    """

    if mode not in ("overwrite", "append"):
        raise ValueError(f"mode must be 'overwrite' or 'append', got {mode!r}")

    targets = [target] if isinstance(target, str) else list(target)
    start = time.perf_counter()

    targets_written: list[str] = []
    bytes_written = 0
    rows_written = 0

    for raw_target in targets:
        if partition_by is not None:
            for value, group in df.groupby(partition_by):
                path_str = _target_path(raw_target, value, partition_by, format)
                bytes_written += _write_one(group, path_str, format, mode, storage_options)
                targets_written.append(path_str)
                rows_written += len(group)
        else:
            bytes_written += _write_one(df, raw_target, format, mode, storage_options)
            targets_written.append(raw_target)
            rows_written += len(df)

    return TobikoResult(
        targets_written=targets_written,
        rows_written=rows_written,
        bytes_written=bytes_written,
        elapsed_s=time.perf_counter() - start,
    )


def _write_one(
    df: pd.DataFrame,
    path_str: str,
    format: str,
    mode: str,
    storage_options: dict[str, Any] | None,
) -> int:
    storage = _storage_of(path_str)
    if storage == "local":
        return _write_local(df, Path(path_str), format, mode)
    if storage == "s3":
        return _write_s3(df, path_str, format, storage_options)
    return _write_gcs(df, path_str, format, storage_options)


def _publish_kafka(rows: list[bytes], target: str, config: dict[str, Any]) -> int:
    try:
        from confluent_kafka import Producer
    except ImportError as e:
        raise ImportError(
            "Kafka support requires confluent-kafka. "
            "Install it with: pip install sushitruck[kafka]"
        ) from e

    topic = target.split("://", 1)[1]
    producer = Producer(config)
    for message in rows:
        producer.produce(topic, message)
    producer.flush()
    return len(rows)


def _publish_kinesis(rows: list[bytes], target: str, config: dict[str, Any]) -> int:
    try:
        import boto3
    except ImportError as e:
        raise ImportError(
            "Kinesis support requires boto3. Install it with: pip install sushitruck[kinesis]"
        ) from e

    stream_name = target.split("://", 1)[1]
    client = boto3.client("kinesis", **{k: v for k, v in config.items() if k != "partition_key"})
    partition_key = config.get("partition_key", "sushitruck")

    for message in rows:
        client.put_record(StreamName=stream_name, Data=message, PartitionKey=partition_key)
    return len(rows)


def publish(
    df: pd.DataFrame,
    target: str,
    *,
    config: dict[str, Any],
    serializer: Callable[[dict[str, Any]], bytes] | None = None,
) -> int:
    """
    Publish each row of a DataFrame as a message to a queue.

    Parameters
    ----------
    df
        DataFrame whose rows become messages, one per row.
    target
        ``"kafka://topic"`` or ``"kinesis://stream-name"``.
    config
        Backend connection config (e.g. Kafka producer config, or a
        Kinesis client's boto3 kwargs plus an optional ``"partition_key"``).
    serializer
        Converts a row (as a dict) to bytes. Defaults to
        ``json.dumps(row).encode()``.

    Returns
    -------
    int
        Count of messages published.
    """

    serialize = serializer or (lambda row: json.dumps(row).encode())
    rows = [serialize(row) for row in df.to_dict(orient="records")]

    if target.startswith("kafka://"):
        return _publish_kafka(rows, target, config)
    if target.startswith("kinesis://"):
        return _publish_kinesis(rows, target, config)

    raise ValueError(f"Unsupported publish target: {target!r}. Use 'kafka://' or 'kinesis://'.")

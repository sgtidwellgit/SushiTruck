"""sashimi — raw file and object store reader, yielding memory-safe DataFrame chunks."""

from __future__ import annotations

import fnmatch
import io
from pathlib import Path
from typing import Any, Generator

import pandas as pd

_EXTENSION_FORMATS = {
    ".csv": "csv",
    ".tsv": "csv",
    ".txt": "csv",
    ".json": "json",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".parquet": "parquet",
}

_CHUNKABLE_FORMATS = {"csv", "jsonl"}


def _detect_format(path: str, format: str | None) -> str:
    if format is not None:
        return format

    suffix = Path(path).suffix.lower()
    if suffix not in _EXTENSION_FORMATS:
        raise ValueError(
            f"Could not auto-detect format for '{path}'. Pass format= explicitly."
        )
    return _EXTENSION_FORMATS[suffix]


def _split_uri(path: str) -> tuple[str, str]:
    """Split an 's3://bucket/key' or 'gs://bucket/key' URI into (bucket, key)."""

    without_scheme = path.split("://", 1)[1]
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _open_local(path: str) -> Any:
    return open(path, "rb")


def _open_s3(path: str, storage_options: dict[str, Any] | None) -> Any:
    try:
        import boto3
    except ImportError as e:
        raise ImportError(
            "S3 support requires boto3. Install it with: pip install sushitruck[cloud]"
        ) from e

    bucket, key = _split_uri(path)
    client = boto3.client("s3", **(storage_options or {}))
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return io.BytesIO(body)


def _open_gcs(path: str, storage_options: dict[str, Any] | None) -> Any:
    try:
        from google.cloud import storage as gcs_storage
    except ImportError as e:
        raise ImportError(
            "GCS support requires google-cloud-storage. "
            "Install it with: pip install sushitruck[cloud]"
        ) from e

    bucket_name, key = _split_uri(path)
    client = gcs_storage.Client(**(storage_options or {}))
    blob = client.bucket(bucket_name).blob(key)
    return io.BytesIO(blob.download_as_bytes())


def _open_source(path: str, storage: str, storage_options: dict[str, Any] | None) -> Any:
    if storage == "local":
        return _open_local(path)
    if storage == "s3":
        return _open_s3(path, storage_options)
    if storage == "gcs":
        return _open_gcs(path, storage_options)
    raise ValueError(f"Unsupported storage backend: {storage!r}")


def _read_full(source: Any, fmt: str, **read_kwargs: Any) -> pd.DataFrame:
    if fmt == "csv":
        return pd.read_csv(source, **read_kwargs)
    if fmt == "json":
        return pd.read_json(source, **read_kwargs)
    if fmt == "jsonl":
        return pd.read_json(source, lines=True, **read_kwargs)
    if fmt == "parquet":
        return pd.read_parquet(source, **read_kwargs)
    raise ValueError(f"Unsupported format: {fmt!r}")


def _read_chunked(source: Any, fmt: str, chunksize: int, **read_kwargs: Any) -> Generator[pd.DataFrame, None, None]:
    if fmt == "csv":
        yield from pd.read_csv(source, chunksize=chunksize, **read_kwargs)
    elif fmt == "jsonl":
        yield from pd.read_json(source, lines=True, chunksize=chunksize, **read_kwargs)
    else:
        raise ValueError(f"Format {fmt!r} does not support chunked reading.")


def read(
    path: str | Path,
    *,
    format: str | None = None,
    chunksize: int | None = None,
    schema: dict[str, dict] | None = None,
    storage: str = "local",
    storage_options: dict[str, Any] | None = None,
    **read_kwargs: Any,
) -> pd.DataFrame | Generator[pd.DataFrame, None, None]:
    """
    Read a file or object-store object into DataFrame(s).

    Parameters
    ----------
    path
        Local path, or ``s3://bucket/key`` / ``gs://bucket/key`` URI.
    format
        ``"csv"``, ``"json"``, ``"jsonl"``, or ``"parquet"``. Auto-detected
        from the file extension when omitted.
    chunksize
        When set, return a generator yielding DataFrames of up to this many
        rows each (only supported for ``"csv"`` and ``"jsonl"``). When
        ``None``, the entire source is loaded and a single DataFrame is
        returned.
    schema
        Optional :mod:`sushitruck.wasabi` schema applied to each returned
        chunk (or the single result) via :func:`sushitruck.wasabi.normalize`.
    storage
        ``"local"``, ``"s3"``, or ``"gcs"``.
    storage_options
        Backend-specific credentials/config (e.g. ``region_name`` for S3).
    **read_kwargs
        Forwarded to the underlying pandas reader (``dtype=``, ``usecols=``,
        ``parse_dates=``, etc.).

    Returns
    -------
    pd.DataFrame | Generator[pd.DataFrame, None, None]
        A single DataFrame, or a generator of DataFrames when ``chunksize``
        is set.
    """

    path = str(path)
    fmt = _detect_format(path, format)

    if chunksize is not None:
        if fmt not in _CHUNKABLE_FORMATS:
            raise ValueError(
                f"chunksize is not supported for format {fmt!r}; "
                f"supported chunked formats are {sorted(_CHUNKABLE_FORMATS)}."
            )
        return _read_chunked_and_normalize(path, fmt, chunksize, storage, storage_options, schema, read_kwargs)

    with _open_source(path, storage, storage_options) as source:
        df = _read_full(source, fmt, **read_kwargs)

    if schema is not None:
        from . import wasabi

        df = wasabi.normalize(df, schema)

    return df


def _read_chunked_and_normalize(
    path: str,
    fmt: str,
    chunksize: int,
    storage: str,
    storage_options: dict[str, Any] | None,
    schema: dict[str, dict] | None,
    read_kwargs: dict[str, Any],
) -> Generator[pd.DataFrame, None, None]:
    with _open_source(path, storage, storage_options) as source:
        for chunk in _read_chunked(source, fmt, chunksize, **read_kwargs):
            if schema is not None:
                from . import wasabi

                chunk = wasabi.normalize(chunk, schema)
            yield chunk


def _list_local(prefix: str, pattern: str | None) -> list[str]:
    base = Path(prefix)
    search_root = base if base.is_dir() else base.parent
    glob_pattern = pattern or "*"
    return sorted(str(p) for p in search_root.glob(glob_pattern) if p.is_file())


def _list_s3(prefix: str, pattern: str | None, storage_options: dict[str, Any] | None) -> list[str]:
    try:
        import boto3
    except ImportError as e:
        raise ImportError(
            "S3 support requires boto3. Install it with: pip install sushitruck[cloud]"
        ) from e

    bucket, key_prefix = _split_uri(prefix)
    client = boto3.client("s3", **(storage_options or {}))
    paginator = client.get_paginator("list_objects_v2")

    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    if pattern:
        keys = [k for k in keys if fnmatch.fnmatch(Path(k).name, pattern)]

    return sorted(f"s3://{bucket}/{k}" for k in keys)


def _list_gcs(prefix: str, pattern: str | None, storage_options: dict[str, Any] | None) -> list[str]:
    try:
        from google.cloud import storage as gcs_storage
    except ImportError as e:
        raise ImportError(
            "GCS support requires google-cloud-storage. "
            "Install it with: pip install sushitruck[cloud]"
        ) from e

    bucket_name, key_prefix = _split_uri(prefix)
    client = gcs_storage.Client(**(storage_options or {}))
    blobs = client.list_blobs(bucket_name, prefix=key_prefix)

    names = [blob.name for blob in blobs]
    if pattern:
        names = [n for n in names if fnmatch.fnmatch(Path(n).name, pattern)]

    return sorted(f"gs://{bucket_name}/{n}" for n in names)


def list_objects(
    prefix: str,
    *,
    storage: str = "local",
    storage_options: dict[str, Any] | None = None,
    pattern: str | None = None,
) -> list[str]:
    """
    List file paths or object URIs under a prefix, optionally glob-filtered.

    Parameters
    ----------
    prefix
        Local directory path, or ``s3://bucket/prefix`` / ``gs://bucket/prefix``.
    storage
        ``"local"``, ``"s3"``, or ``"gcs"``.
    storage_options
        Backend-specific credentials/config.
    pattern
        Glob pattern matched against each object's basename (e.g. ``"*.csv"``).

    Returns
    -------
    list[str]
        Matching paths or URIs, sorted.
    """

    if storage == "local":
        return _list_local(prefix, pattern)
    if storage == "s3":
        return _list_s3(prefix, pattern, storage_options)
    if storage == "gcs":
        return _list_gcs(prefix, pattern, storage_options)
    raise ValueError(f"Unsupported storage backend: {storage!r}")

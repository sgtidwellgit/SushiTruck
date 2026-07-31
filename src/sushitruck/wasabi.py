"""wasabi — JSON flattening and schema normalization. Depends only on pandas."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd


def _flatten_value(
    value: Any,
    prefix: str,
    separator: str,
    max_depth: int | None,
    depth: int,
    out: dict[str, Any],
) -> None:
    """Recursively flatten a nested dict into ``out``, keyed by joined paths.

    ``depth`` counts nesting levels already descended into (0 for a
    top-level record field), so ``max_depth=1`` flattens one level of
    nesting before leaving deeper values untouched.
    """

    if isinstance(value, dict) and value and (max_depth is None or depth < max_depth):
        for key, sub_value in value.items():
            new_prefix = f"{prefix}{separator}{key}"
            _flatten_value(sub_value, new_prefix, separator, max_depth, depth + 1, out)
    else:
        out[prefix] = value


def flatten(
    data: dict | list[dict] | str,
    *,
    separator: str = "_",
    max_depth: int | None = None,
    drop_empty: bool = True,
) -> pd.DataFrame:
    """
    Flatten nested JSON records into a flat DataFrame.

    Parameters
    ----------
    data
        A single JSON object, a list of JSON objects, or a raw JSON string
        (parsed with :func:`json.loads` before flattening).
    separator
        Separator joining nested key paths (``"a.b.c"`` -> ``"a_b_c"``).
    max_depth
        Maximum nesting depth to flatten. ``None`` flattens fully.
    drop_empty
        Whether to drop columns that are entirely ``None`` or empty lists
        across all records.

    Returns
    -------
    pd.DataFrame
        One row per input record, with nested keys expanded into columns.
    """

    if isinstance(data, str):
        data = json.loads(data)

    records = data if isinstance(data, list) else [data]

    rows: list[dict[str, Any]] = []
    for record in records:
        flat: dict[str, Any] = {}
        for key, value in record.items():
            _flatten_value(value, str(key), separator, max_depth, 0, flat)
        rows.append(flat)

    df = pd.DataFrame(rows)

    if drop_empty and not df.empty:
        empty_cols = [
            col
            for col in df.columns
            if df[col].map(lambda v: v is None or v == []).all()
        ]
        df = df.drop(columns=empty_cols)

    return df


_DTYPE_ALIASES = {
    "int": "int64",
    "float": "float64",
    "str": "object",
    "bool": "bool",
    "datetime": "datetime64[ns]",
}


def _resolve_dtype(dtype: Any) -> str:
    """Normalize a schema dtype spec (Python type, alias, or dtype string) to a dtype string."""

    if isinstance(dtype, type):
        return _DTYPE_ALIASES.get(dtype.__name__, dtype.__name__)
    return _DTYPE_ALIASES.get(str(dtype), str(dtype))


def normalize(
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]],
    *,
    strict: bool = False,
    coerce: bool = True,
) -> pd.DataFrame:
    """
    Enforce a schema on a DataFrame: coerce dtypes, rename, and check nulls.

    Parameters
    ----------
    df
        Input DataFrame. Not mutated.
    schema
        Mapping of source column name to a spec dict with optional keys
        ``"dtype"``, ``"nullable"`` (default ``True``), ``"rename"``, and
        ``"default"`` (fill value applied before the nullable check).
    strict
        If ``True``, raise on columns present in ``df`` but absent from
        ``schema``. If ``False``, extra columns are kept as-is.
    coerce
        Whether to attempt dtype coercion per the schema's ``"dtype"`` spec.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with schema columns coerced, renamed, and validated.

    Raises
    ------
    ValueError
        If ``strict=True`` and extra columns are present, if a schema column
        is missing from ``df``, or if a non-nullable column contains nulls
        after coercion and default-filling.
    """

    extra_columns = set(df.columns) - set(schema)
    if strict and extra_columns:
        raise ValueError(f"Unexpected columns not in schema: {sorted(extra_columns)}")

    missing_columns = set(schema) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Schema columns missing from DataFrame: {sorted(missing_columns)}")

    result = df.copy()
    rename_map: dict[str, str] = {}

    for column, spec in schema.items():
        default = spec.get("default")
        if default is not None:
            result[column] = result[column].fillna(default)

        if coerce and "dtype" in spec:
            target_dtype = _resolve_dtype(spec["dtype"])
            if target_dtype.startswith("datetime64"):
                result[column] = pd.to_datetime(result[column])
            else:
                result[column] = result[column].astype(target_dtype)

        nullable = spec.get("nullable", True)
        if not nullable and result[column].isna().any():
            raise ValueError(f"Column '{column}' is non-nullable but contains null values.")

        if "rename" in spec:
            rename_map[column] = spec["rename"]

    if rename_map:
        result = result.rename(columns=rename_map)

    return result


def infer_schema(df: pd.DataFrame, *, sample_n: int | None = 1000) -> dict[str, dict[str, Any]]:
    """
    Infer a :func:`normalize`-compatible schema from a DataFrame sample.

    Parameters
    ----------
    df
        DataFrame to inspect.
    sample_n
        Number of rows to sample for inference. ``None`` uses the full
        DataFrame.

    Returns
    -------
    dict
        Mapping of column name to a spec dict with ``"dtype"`` (a Python
        type, or ``"datetime64[ns]"`` for datetime columns) and
        ``"nullable"``.
    """

    sample = df if sample_n is None else df.head(sample_n)

    schema: dict[str, dict[str, Any]] = {}
    for column in sample.columns:
        series = sample[column]
        nullable = bool(series.isna().any())

        if pd.api.types.is_datetime64_any_dtype(series):
            dtype: Any = "datetime64[ns]"
        elif pd.api.types.is_bool_dtype(series):
            dtype = bool
        elif pd.api.types.is_integer_dtype(series):
            dtype = int
        elif pd.api.types.is_float_dtype(series):
            dtype = float
        else:
            dtype = str

        schema[column] = {"dtype": dtype, "nullable": nullable}

    return schema

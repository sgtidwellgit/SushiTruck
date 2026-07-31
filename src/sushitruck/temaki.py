"""temaki — coordinates multi-source batch ingestion jobs."""

from __future__ import annotations

import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Generator

import pandas as pd

from . import maki, sashimi
from .results import TemakiResult

_ON_ERROR_MODES = ("raise", "warn", "skip")


def _split_glob(pattern: str) -> tuple[str, str]:
    """Split a combined prefix+glob string into (prefix, glob) parts.

    URI-style patterns (``s3://``, ``gs://``) always use ``/`` as the
    separator; local paths use :mod:`pathlib` so native OS separators
    (e.g. Windows ``\\``) are handled correctly.
    """

    if "://" in pattern:
        prefix, sep, glob_part = pattern.rpartition("/")
        return (prefix + sep if sep else ""), (glob_part or "*")

    path = Path(pattern)
    return str(path.parent), (path.name or "*")


class TemakiJob:
    """Coordinates reading multiple sources (files, API endpoints) into one result."""

    def __init__(
        self,
        *,
        workers: int = 1,
        schema: dict[str, dict] | None = None,
        on_error: str = "raise",
    ) -> None:
        """
        Configure a batch ingestion job.

        Parameters
        ----------
        workers
            ``1`` runs sources sequentially. ``>1`` reads sources
            concurrently with a thread pool (used by :meth:`run` only —
            :meth:`stream` always runs sequentially).
        schema
            Optional :mod:`sushitruck.wasabi` schema applied to every
            source's result after it is read.
        on_error
            ``"raise"`` propagates the first source failure immediately.
            ``"warn"`` emits a ``UserWarning`` and continues, recording the
            failure. ``"skip"`` silently continues and records the failure.
        """

        if on_error not in _ON_ERROR_MODES:
            raise ValueError(f"on_error must be one of {_ON_ERROR_MODES}, got {on_error!r}")

        self.workers = workers
        self._schema = schema
        self.on_error = on_error
        self._sources: list[dict[str, Any]] = []

    def add_file(
        self,
        path: str | Path,
        *,
        format: str | None = None,
        chunksize: int | None = None,
        storage: str = "local",
        storage_options: dict[str, Any] | None = None,
        **read_kwargs: Any,
    ) -> "TemakiJob":
        """Add a single local/S3/GCS file as a source. Returns ``self`` for chaining."""

        self._sources.append({
            "label": str(path),
            "reader": lambda: sashimi.read(
                path,
                format=format,
                chunksize=chunksize,
                storage=storage,
                storage_options=storage_options,
                **read_kwargs,
            ),
        })
        return self

    def add_api(
        self,
        client: "maki.MakiClient",
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        paginate: bool = False,
        results_key: str | None = None,
    ) -> "TemakiJob":
        """Add a REST API endpoint as a source. Returns ``self`` for chaining."""

        self._sources.append({
            "label": f"api:{endpoint}",
            "reader": lambda: client.fetch(
                endpoint, params=params, paginate=paginate, results_key=results_key
            ),
        })
        return self

    def add_glob(
        self,
        pattern: str,
        *,
        storage: str = "local",
        storage_options: dict[str, Any] | None = None,
        **read_kwargs: Any,
    ) -> "TemakiJob":
        """Add every file matching a glob / object-store prefix pattern. Returns ``self``."""

        prefix, glob_part = _split_glob(pattern)
        matched = sashimi.list_objects(
            prefix, storage=storage, storage_options=storage_options, pattern=glob_part
        )

        for path in matched:
            self.add_file(path, storage=storage, storage_options=storage_options, **read_kwargs)

        return self

    def _apply_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._schema is None:
            return df

        from . import wasabi

        return wasabi.normalize(df, self._schema)

    @staticmethod
    def _materialize(result: Any) -> pd.DataFrame:
        if isinstance(result, pd.DataFrame):
            return result
        chunks = list(result)
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    def _process(self, source: dict[str, Any]) -> pd.DataFrame:
        return self._apply_schema(self._materialize(source["reader"]()))

    def run(self) -> TemakiResult:
        """
        Read all sources and merge them into a single result.

        Returns
        -------
        TemakiResult
            ``df`` (merged DataFrame), ``sources_processed``,
            ``sources_failed`` (list of ``{"source", "error"}`` dicts, only
            populated when ``on_error`` is ``"warn"`` or ``"skip"``),
            ``total_rows``, and ``elapsed_s``.
        """

        start = time.perf_counter()
        frames: list[pd.DataFrame] = []
        failed: list[dict[str, str]] = []
        processed = 0

        if self.workers <= 1:
            for source in self._sources:
                try:
                    frames.append(self._process(source))
                    processed += 1
                except Exception as exc:
                    self._handle_failure(source, exc, failed)
        else:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                future_to_source = {
                    executor.submit(self._process, source): source for source in self._sources
                }
                for future in as_completed(future_to_source):
                    source = future_to_source[future]
                    try:
                        frames.append(future.result())
                        processed += 1
                    except Exception as exc:
                        self._handle_failure(source, exc, failed)

        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        return TemakiResult(
            df=combined,
            sources_processed=processed,
            sources_failed=failed,
            total_rows=len(combined),
            elapsed_s=time.perf_counter() - start,
        )

    def _handle_failure(
        self, source: dict[str, Any], exc: Exception, failed: list[dict[str, str]]
    ) -> None:
        if self.on_error == "raise":
            raise exc

        if self.on_error == "warn":
            warnings.warn(
                f"TemakiJob source '{source['label']}' failed: {exc}",
                UserWarning,
                stacklevel=3,
            )

        failed.append({"source": source["label"], "error": str(exc)})

    def stream(self) -> Generator[pd.DataFrame, None, None]:
        """
        Read sources sequentially, yielding a DataFrame per source (or per chunk).

        Sources added with a file ``chunksize`` yield one DataFrame per
        chunk; all other sources yield a single DataFrame.
        """

        for source in self._sources:
            try:
                result = source["reader"]()
                if isinstance(result, pd.DataFrame):
                    yield self._apply_schema(result)
                else:
                    for chunk in result:
                        yield self._apply_schema(chunk)
            except Exception as exc:
                if self.on_error == "raise":
                    raise
                if self.on_error == "warn":
                    warnings.warn(
                        f"TemakiJob source '{source['label']}' failed: {exc}",
                        UserWarning,
                        stacklevel=2,
                    )

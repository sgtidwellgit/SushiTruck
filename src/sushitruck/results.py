"""Shared result containers for SushiTruck."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TemakiResult:
    """Result returned by :meth:`sushitruck.temaki.TemakiJob.run`."""

    df: Any
    sources_processed: int
    sources_failed: list[Any] = field(default_factory=list)
    total_rows: int = 0
    elapsed_s: float = 0.0


@dataclass(frozen=True)
class TobikoResult:
    """Result returned by :func:`sushitruck.tobiko.send`."""

    targets_written: list[str]
    rows_written: int
    bytes_written: int | None
    elapsed_s: float

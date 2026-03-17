"""Chunk accumulator protocol for the single-pass pipeline."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class ChunkAccumulator(Protocol):
    def add_chunk(self, chunk: pl.DataFrame) -> None: ...

    def result(self) -> Any: ...

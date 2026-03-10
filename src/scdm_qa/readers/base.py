from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Protocol, runtime_checkable

import polars as pl


@dataclass(frozen=True)
class TableMetadata:
    file_path: Path
    file_format: str  # "parquet" or "sas7bdat"
    column_names: tuple[str, ...]
    row_count: int | None  # None if unknown without full scan


@runtime_checkable
class TableReader(Protocol):
    def metadata(self) -> TableMetadata: ...
    def chunks(self) -> Iterator[pl.DataFrame]: ...

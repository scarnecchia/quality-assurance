from __future__ import annotations

from pathlib import Path
from typing import Iterator

import polars as pl

from scdm_qa.readers.base import TableMetadata


class ParquetReader:
    def __init__(self, file_path: Path, *, chunk_size: int = 500_000) -> None:
        if chunk_size < 1:
            raise ValueError(f"chunk_size must be positive, got {chunk_size}")
        self._file_path = file_path
        self._chunk_size = chunk_size

    def metadata(self) -> TableMetadata:
        schema = pl.read_parquet_schema(self._file_path)
        row_count = (
            pl.scan_parquet(self._file_path)
            .select(pl.len())
            .collect()
            .item()
        )
        return TableMetadata(
            file_path=self._file_path,
            file_format="parquet",
            column_names=tuple(schema.keys()),
            row_count=row_count,
        )

    def chunks(self) -> Iterator[pl.DataFrame]:
        lf = pl.scan_parquet(self._file_path)
        # NOTE: collect_batches is marked unstable in Polars but is functional.
        # Pinned to polars <2 in pyproject.toml.
        yield from lf.collect_batches(chunk_size=self._chunk_size)

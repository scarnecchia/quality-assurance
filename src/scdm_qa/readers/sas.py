from __future__ import annotations

from pathlib import Path
from typing import Iterator

import polars as pl
import pyreadstat

from scdm_qa.readers.base import TableMetadata


class SasReader:
    def __init__(self, file_path: Path, *, chunk_size: int = 500_000) -> None:
        if chunk_size < 1:
            raise ValueError(f"chunk_size must be positive, got {chunk_size}")
        self._file_path = file_path
        self._chunk_size = chunk_size

    def metadata(self) -> TableMetadata:
        _, meta = pyreadstat.read_sas7bdat(str(self._file_path), metadataonly=True)
        return TableMetadata(
            file_path=self._file_path,
            file_format="sas7bdat",
            column_names=tuple(meta.column_names),
            row_count=meta.number_rows,
        )

    def chunks(self) -> Iterator[pl.DataFrame]:
        reader = pyreadstat.read_file_in_chunks(
            pyreadstat.read_sas7bdat,
            str(self._file_path),
            chunksize=self._chunk_size,
        )
        for pandas_chunk, _ in reader:
            yield pl.from_pandas(pandas_chunk)

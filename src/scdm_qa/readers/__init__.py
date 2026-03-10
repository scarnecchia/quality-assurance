from __future__ import annotations

from pathlib import Path

from scdm_qa.readers.base import TableMetadata, TableReader


class UnsupportedFormatError(Exception):
    pass


def create_reader(file_path: Path, chunk_size: int = 500_000) -> TableReader:
    suffix = file_path.suffix.lower()
    if suffix == ".parquet":
        from scdm_qa.readers.parquet import ParquetReader
        return ParquetReader(file_path, chunk_size=chunk_size)
    elif suffix == ".sas7bdat":
        from scdm_qa.readers.sas import SasReader
        return SasReader(file_path, chunk_size=chunk_size)
    else:
        raise UnsupportedFormatError(
            f"unsupported file format: {suffix!r}. Expected .parquet or .sas7bdat"
        )


__all__ = [
    "TableMetadata",
    "TableReader",
    "UnsupportedFormatError",
    "create_reader",
]

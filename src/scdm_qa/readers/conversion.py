"""SAS-to-Parquet conversion utilities for DuckDB-based checks."""

from __future__ import annotations

import contextlib
import structlog
import tempfile
from collections.abc import Generator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scdm_qa.schemas import get_schema
from scdm_qa.schemas.models import TableSchema

log = structlog.get_logger(__name__)

_SCDM_TYPE_MAP: dict[str, pa.DataType] = {
    "Numeric": pa.float64(),
    "Character": pa.utf8(),
}


def build_arrow_schema(
    table_schema: TableSchema,
    *,
    data_columns: tuple[str, ...] | None = None,
) -> pa.Schema:
    """Build a canonical pyarrow.Schema from an SCDM TableSchema.

    Args:
        table_schema: SCDM table definition with column types and nullability.
        data_columns: If provided, only include spec columns present in this
            sequence, ordered to match data column order. Columns in data but
            not in spec are excluded (caller merges inferred types separately).

    Returns:
        pyarrow.Schema with canonical types for SCDM columns.

    Raises:
        ValueError: If a ColumnDef has an unrecognised col_type.
    """
    col_lookup = {c.name: c for c in table_schema.columns}

    if data_columns is not None:
        ordered_names = [n for n in data_columns if n in col_lookup]
    else:
        ordered_names = [c.name for c in table_schema.columns]

    fields: list[pa.Field] = []
    for name in ordered_names:
        col_def = col_lookup[name]
        arrow_type = _SCDM_TYPE_MAP.get(col_def.col_type)
        if arrow_type is None:
            raise ValueError(
                f"unrecognised SCDM col_type {col_def.col_type!r} "
                f"for column {col_def.name!r}"
            )
        fields.append(pa.field(name, arrow_type, nullable=col_def.missing_allowed))

    return pa.schema(fields)


def _build_write_schema(
    canonical_schema: TableSchema | None,
    data_schema: pa.Schema,
) -> pa.Schema:
    """Build a write schema merging canonical SCDM types with inferred types.

    Canonical types are used for columns defined in the SCDM spec. Columns
    present in data but not in spec keep their inferred types. Column order
    follows the data schema.

    Args:
        canonical_schema: SCDM TableSchema, or None for unknown tables.
        data_schema: Schema inferred from the first data chunk.

    Returns:
        Merged pyarrow.Schema with canonical types where available.
    """
    if canonical_schema is None:
        return data_schema

    data_col_names = tuple(data_schema.names)
    canonical = build_arrow_schema(canonical_schema, data_columns=data_col_names)
    canonical_lookup = {f.name: f for f in canonical}

    merged_fields: list[pa.Field] = []
    for i, name in enumerate(data_col_names):
        if name in canonical_lookup:
            merged_fields.append(canonical_lookup[name])
        else:
            merged_fields.append(data_schema.field(i))

    return pa.schema(merged_fields)


def convert_sas_to_parquet(
    sas_path: Path,
    chunk_size: int = 500_000,
    *,
    table_key: str,
) -> Path:
    """Convert SAS7BDAT to temp Parquet via streaming writes.

    Each chunk is cast to a canonical schema (from the SCDM spec) and written
    as a separate Parquet row group, keeping memory bounded to one chunk.

    Args:
        sas_path: Path to .sas7bdat file.
        chunk_size: Chunk size for reading.
        table_key: SCDM table key for canonical schema lookup.

    Returns:
        Path to temporary Parquet file.
    """
    from scdm_qa.readers import create_reader

    reader = create_reader(sas_path, chunk_size=chunk_size)

    # Resolve canonical schema (None if table_key unknown)
    canonical_schema: TableSchema | None = None
    try:
        canonical_schema = get_schema(table_key)
    except KeyError:
        log.warning(
            "no SCDM spec for table; schema will be inferred from data",
            table_key=table_key,
        )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    writer: pq.ParquetWriter | None = None
    write_schema: pa.Schema | None = None
    total_rows = 0

    try:
        for chunk_df in reader.chunks():
            arrow_table = chunk_df.to_arrow()

            if write_schema is None:
                write_schema = _build_write_schema(
                    canonical_schema, arrow_table.schema
                )
                writer = pq.ParquetWriter(str(tmp_path), write_schema)

            arrow_table = arrow_table.cast(write_schema)
            writer.write_table(arrow_table)
            total_rows += arrow_table.num_rows

        # Handle zero-chunk case
        if writer is None:
            if canonical_schema is not None:
                write_schema = build_arrow_schema(canonical_schema)
            else:
                write_schema = pa.schema([])
            writer = pq.ParquetWriter(str(tmp_path), write_schema)
    finally:
        if writer is not None:
            writer.close()

    log.info(
        "converted SAS file to temp parquet (streaming)",
        sas_path=str(sas_path),
        tmp_path=str(tmp_path),
        n_rows=total_rows,
        n_columns=len(write_schema) if write_schema else 0,
    )
    return tmp_path


@contextlib.contextmanager
def converted_parquet(
    sas_path: Path,
    chunk_size: int = 500_000,
    *,
    table_key: str,
) -> Generator[Path, None, None]:
    """Context manager that converts SAS to temp Parquet and cleans up on exit.

    Yields:
        Path to temporary Parquet file.
    """
    tmp_path = convert_sas_to_parquet(sas_path, chunk_size, table_key=table_key)
    try:
        yield tmp_path
    finally:
        try:
            tmp_path.unlink()
        except OSError as e:
            log.warning("failed to delete temp parquet file", path=str(tmp_path), error=str(e))

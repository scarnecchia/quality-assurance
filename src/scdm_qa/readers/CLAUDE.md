# Readers Domain

Last verified: 2026-03-16

## Purpose
Provides a uniform chunked-reading interface over heterogeneous file formats, and SAS-to-Parquet conversion utilities so that DuckDB-based checks can operate on any input format.

## Contracts
- **Exposes**: `create_reader(file_path, chunk_size) -> TableReader`, `TableReader` protocol, `TableMetadata` dataclass, `build_arrow_schema(table_schema, *, data_columns) -> pa.Schema`, `convert_sas_to_parquet(sas_path, out_path, schema, chunk_size)`, `converted_parquet(file_path, table_key, config) -> Generator[Path]` context manager
- **Guarantees**: `chunks()` yields `polars.DataFrame` instances. Factory selects reader by file extension. Unsupported formats raise `UnsupportedFormatError`. SAS-to-Parquet conversion enforces canonical SCDM types from the spec; unknown tables fall back to inferred types. Streaming writes keep memory bounded to one chunk.
- **Expects**: File exists and is readable. Extension is `.parquet` or `.sas7bdat`. Conversion functions require a `TableSchema` for canonical type mapping.

## Dependencies
- **Uses**: polars, pyarrow (Parquet reader + streaming ParquetWriter), pyreadstat (SAS), schemas (for `get_schema`, `TableSchema` in conversion)
- **Used by**: pipeline (via `create_reader`), validation (via `converted_parquet` for DuckDB global and cross-table checks)
- **Boundary**: Readers do not validate data. Conversion is format-level (type coercion to SCDM spec), not semantic validation.

## Key Decisions
- Protocol over ABC: `TableReader` is a `runtime_checkable Protocol`, not an abstract base class
- Lazy imports: Format-specific readers are imported inside `create_reader()` to avoid loading pyreadstat when only using Parquet
- Conversion in readers, not validation: SAS-to-Parquet conversion lives here because it is a format concern (type mapping), not a validation concern. Validation and cross-table modules import it.

## Invariants
- Every chunk is a `polars.DataFrame` (never pandas, never raw arrays)
- `metadata().row_count` may be `None` for formats where row count requires a full scan (SAS)
- `converted_parquet()` yields a temporary file that is cleaned up on context exit

## Key Files
- `base.py` - `TableReader` protocol, `TableMetadata` dataclass
- `parquet.py` - Parquet reader (row-group chunked)
- `sas.py` - SAS reader (pyreadstat chunked)
- `conversion.py` - SAS-to-Parquet streaming conversion, canonical Arrow schema builder

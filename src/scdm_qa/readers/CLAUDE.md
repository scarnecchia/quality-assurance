# Readers Domain

Last verified: 2026-03-09

## Purpose
Provides a uniform chunked-reading interface over heterogeneous file formats, so the pipeline never knows or cares what format the source data is in.

## Contracts
- **Exposes**: `create_reader(file_path, chunk_size) -> TableReader`, `TableReader` protocol, `TableMetadata` dataclass
- **Guarantees**: `chunks()` yields `polars.DataFrame` instances. Factory selects reader by file extension. Unsupported formats raise `UnsupportedFormatError`.
- **Expects**: File exists and is readable. Extension is `.parquet` or `.sas7bdat`.

## Dependencies
- **Uses**: polars, pyarrow (Parquet), pyreadstat (SAS)
- **Used by**: pipeline, validation/runner (via pipeline)
- **Boundary**: Readers do not validate or transform data -- they only yield chunks

## Key Decisions
- Protocol over ABC: `TableReader` is a `runtime_checkable Protocol`, not an abstract base class
- Lazy imports: Format-specific readers are imported inside `create_reader()` to avoid loading pyreadstat when only using Parquet

## Invariants
- Every chunk is a `polars.DataFrame` (never pandas, never raw arrays)
- `metadata().row_count` may be `None` for formats where row count requires a full scan (SAS)

## Key Files
- `base.py` - `TableReader` protocol, `TableMetadata` dataclass
- `parquet.py` - Parquet reader (row-group chunked)
- `sas.py` - SAS reader (pyreadstat chunked)

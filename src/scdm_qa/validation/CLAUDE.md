# Validation Domain

Last verified: 2026-03-16

## Purpose
Runs per-chunk pointblank validation against SCDM schemas, accumulates results across chunks into a single coherent result per table, and executes cross-table validation checks using DuckDB. TableValidator orchestrates the L1 lifecycle, broadcasting chunks to multiple accumulators via ThreadPoolExecutor and executing DuckDB global checks.

## Contracts
- **Exposes**: `ChunkAccumulator` (protocol), `ValidationChunkAccumulator`, `TableValidator`, `TableValidatorResult`, `run_validation(reader, schema, ...) -> ValidationResult`, `run_cross_table_checks(config, checks, ...) -> list[StepResult]`, `build_arrow_schema(table_schema, *, data_columns=None) -> pa.Schema`, `check_uniqueness(conn, view_name, schema, ...)`, `check_sort_order(conn, view_name, schema)`, `check_not_populated(conn, view_name, schema)`, `check_date_ordering(conn, view_name, schema, ...)`, `check_cause_of_death(conn, view_name, schema, ...)`, `check_overlapping_spans(conn, view_name, schema, ...)`, `check_enrollment_gaps(conn, view_name, schema, ...)`, `check_enc_combinations(conn, view_name, schema, ...)`, `StepResult`, `ValidationResult`
- **Guarantees**: Results accumulate correctly across chunks (pass/fail counts merge). Failing row samples are bounded by `max_failing_rows`. Global checks run via DuckDB SQL against a pre-registered view. Cross-table checks operate on full tables via DuckDB, converting SAS files to temporary Parquet via streaming writes (memory bounded to one chunk). SAS-to-Parquet conversion enforces canonical SCDM types from the spec; unknown tables fall back to inferred types.
- **Expects**: A `TableReader` that yields `polars.DataFrame` chunks. A `TableSchema` with validation rules. For global checks: a `duckdb.DuckDBPyConnection` with the table registered as a view. For cross-table checks: `QAConfig` with table paths and `CrossTableCheckDef` definitions.

## Dependencies
- **Uses**: schemas (for `build_validation`, `TableSchema`, `CrossTableCheckDef`), readers (via `TableReader`), pointblank, polars, pyarrow (canonical schema construction and streaming Parquet writes), duckdb (global checks and cross-table checks)
- **Used by**: pipeline (which delegates per-table L1 orchestration to `TableValidator`)
- **Boundary**: Does not produce reports or handle I/O beyond reading chunks. Does not create or manage DuckDB connections (TableValidator creates and manages them).

## Key Decisions
- TableValidator orchestrates L1 lifecycle: Chunk broadcasting via ThreadPoolExecutor, DuckDB global checks, and result assembly. Pipeline owns the high-level flow but delegates table processing to TableValidator.
- ChunkAccumulator protocol enables extensibility: Any object with `add_chunk()` and `result()` methods can be registered with `TableValidator` and receives every chunk. New accumulators require zero modifications to `TableValidator` or the read loop.
- TableValidator owns DuckDB connection lifecycle: Creates a single DuckDB connection per table, registers the Parquet file (or converted SAS file) as a view, passes `conn` + `view_name` to all global check functions, and closes it after.
- Chunked accumulation: Validation runs per-chunk, `ValidationAccumulator` merges step results across chunks to handle datasets larger than memory. `ValidationChunkAccumulator` wraps this pipeline behind the `ChunkAccumulator` protocol for single-pass architecture.
- DuckDB for all global checks: All global checks (uniqueness, sort order, not-populated, date ordering, cause of death, overlapping spans, enrollment gaps, ENC combinations) use SQL against a DuckDB view. No in-memory Polars fallback -- DuckDB is required.
- SAS files participate in DuckDB global checks: SAS files are converted to temporary Parquet via `converted_parquet()` context manager, which uses streaming writes to keep memory bounded.
- Streaming SAS-to-Parquet: Conversion uses `pyarrow.parquet.ParquetWriter` writing one row group per chunk, keeping memory bounded. Schema is derived from SCDM spec (canonical types) merged with inferred types for non-spec columns
- Single-pass protocol adapter: `ValidationChunkAccumulator` internalises the per-chunk validation pipeline (`build_validation -> interrogate -> accumulate`), exposing only `add_chunk()` and `result()` via `ChunkAccumulator` protocol. It composes `ValidationAccumulator` and imports `build_step_descriptions` from `runner.py`.

## Invariants
- `StepResult` and `ValidationResult` are frozen dataclasses
- `StepResult` carries `check_id: str | None` and `severity: str | None` ("Fail" | "Warn" | "Note" | None) for traceability and exit code logic
- `f_failed` and `f_passed` are derived properties, never stored
- Failing row samples never exceed `max_failing_rows`
- Note-severity steps are informational and never escalate exit codes
- Global check functions never create or close DuckDB connections

## Key Files
- `runner.py` - Main validation orchestrator (includes step description builder for code/cross-table checks); exports `build_step_descriptions()` for cross-module use
- `accumulator.py` - `ValidationAccumulator` for cross-chunk merging
- `accumulator_protocol.py` - `ChunkAccumulator` runtime-checkable Protocol for chunk consumer extensibility
- `validation_chunk_accumulator.py` - `ValidationChunkAccumulator` wrapping per-chunk validation pipeline behind `ChunkAccumulator` protocol
- `table_validator.py` - `TableValidator` orchestrating L1 lifecycle (chunk broadcasting, DuckDB global checks, result assembly), `TableValidatorResult` data model
- `results.py` - `StepResult`, `ValidationResult` data models
- `global_checks.py` - All global checks via DuckDB: L0 (uniqueness, sort order), L1 check 111 (not populated), L2 checks 226, 236/237, 215/216, 244/245
- `duckdb_utils.py` - `create_connection()` helper for DuckDB config (memory limit, threads, temp dir)
- `cross_table.py` - Cross-table validation engine using DuckDB (referential integrity, length consistency, cross-date comparison, length excess, column mismatch)

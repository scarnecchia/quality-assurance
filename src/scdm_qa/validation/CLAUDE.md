# Validation Domain

Last verified: 2026-03-10

## Purpose
Runs per-chunk pointblank validation against SCDM schemas and accumulates results across chunks into a single coherent result per table.

## Contracts
- **Exposes**: `run_validation(reader, schema, ...) -> ValidationResult`, `check_uniqueness(...)`, `check_sort_order(...)`, `check_not_populated(...)`, `check_date_ordering(...)`, `check_cause_of_death(...)`, `check_overlapping_spans(...)`, `check_enrollment_gaps(...)`, `check_enc_combinations(...)`, `StepResult`, `ValidationResult`
- **Guarantees**: Results accumulate correctly across chunks (pass/fail counts merge). Failing row samples are bounded by `max_failing_rows`. Global checks run separately from per-chunk validation and are wired into pipeline.
- **Expects**: A `TableReader` that yields `polars.DataFrame` chunks. A `TableSchema` with validation rules.

## Dependencies
- **Uses**: schemas (for `build_validation`, `TableSchema`), readers (via `TableReader`), pointblank, polars, optionally duckdb (fast-path uniqueness)
- **Used by**: pipeline
- **Boundary**: Does not produce reports or handle I/O beyond reading chunks

## Key Decisions
- Chunked accumulation: Validation runs per-chunk, `ValidationAccumulator` merges step results across chunks to handle datasets larger than memory
- DuckDB fast-path for uniqueness: When duckdb is installed, uniqueness checks use SQL `GROUP BY` instead of polars; falls back gracefully
- Profiling piggybacks on validation: `ProfilingAccumulator` is called inside the validation loop to avoid a second scan

## Invariants
- `StepResult` and `ValidationResult` are frozen dataclasses
- `StepResult` carries `check_id: str | None` and `severity: str | None` ("Fail" | "Warn" | "Note" | None) for traceability and exit code logic
- `f_failed` and `f_passed` are derived properties, never stored
- Failing row samples never exceed `max_failing_rows`
- Note-severity steps are informational and never escalate exit codes

## Key Files
- `runner.py` - Main validation orchestrator
- `accumulator.py` - `ValidationAccumulator` for cross-chunk merging
- `results.py` - `StepResult`, `ValidationResult` data models
- `global_checks.py` - L0 global checks (uniqueness, sort order), L1 check 111 (not populated), L2 checks 226, 236/237, 215/216, 244/245

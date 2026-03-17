# SCDM Quality Assurance

Last verified: 2026-03-16

## Tech Stack
- Language: Python 3.12+
- CLI: Typer
- Data: Polars (DataFrames), DuckDB (global checks + cross-table), pointblank (validation rules)
- Formats: Parquet (pyarrow), SAS7BDAT (pyreadstat)
- Reporting: Jinja2, vendored Tabulator + Plotly (self-contained HTML dashboard)
- Logging: structlog
- Build: uv (uv_build backend)
- Testing: pytest

## Commands
- `uv run pytest` - Run tests
- `uv run scdm-qa run <config.toml>` - Validate tables (L1 + L2)
- `uv run scdm-qa run <config.toml> --l1-only` - Per-table validation only
- `uv run scdm-qa run <config.toml> --l2-only` - Cross-table validation only
- `uv run scdm-qa profile <config.toml>` - Profile only (no validation)
- `uv run scdm-qa schema [TABLE]` - Show SCDM schema definitions
- `uv run scdm-qa serve <report-dir>` - Browse HTML reports locally

## Project Structure
- `src/scdm_qa/` - Main package
  - `cli.py` - Typer CLI entry point (4 subcommands: run, profile, schema, serve)
  - `config.py` - TOML config loader (`QAConfig` dataclass)
  - `pipeline.py` - Orchestrator: L1 per-table validation, L2 cross-table validation, exit code logic
  - `logging.py` - structlog setup (console + JSON file)
  - `schemas/` - SCDM table definitions, JSON spec parser, pointblank rule builder, L1/L2 check registry, code check defs, cross-table check defs
  - `readers/` - Chunked file readers (Parquet, SAS) behind a Protocol, SAS-to-Parquet conversion utilities
  - `validation/` - Per-chunk validation runner, chunk accumulators, TableValidator (L1 orchestrator), L0/L1/L2 global checks, cross-table validation engine
  - `profiling/` - Streaming column statistics accumulator
  - `reporting/` - Dashboard report generator (serialise.py, dashboard.py, Jinja2 templates, vendored JS/CSS)
- `tests/` - pytest tests (one file per module)
- `docs/implementation-plans/` - Phase plans (reference only)

## Conventions
- All data models are frozen dataclasses
- Chunks are `polars.DataFrame` throughout the pipeline
- Validation uses pointblank for rule expression
- Single-pass architecture: profiling runs inside the validation loop
- Global checks (uniqueness, sort order, L1/L2 checks) run via DuckDB SQL against Parquet views; SAS files are converted to temporary Parquet for global checks via streaming writes (memory bounded)
- Pipeline runs in two phases: L1 (per-table) then L2 (cross-table); each can be run independently via `--l1-only` / `--l2-only` CLI flags or `run_l1` / `run_l2` config options
- StepResult carries `check_id` and `severity` ("Fail" | "Warn" | "Note" | None)
- Exit codes are severity-aware: 0=pass (no non-Note failures), 1=warnings (failures within threshold), 2=errors or threshold exceeded (Note-severity checks are informational and never escalate exit code)

## Configuration
TOML config with `[tables]` section mapping table keys to file paths, plus `[options]` for chunk_size, max_failing_rows, error_threshold, output_dir, custom_rules_dir, log_file, verbose, run_l1, run_l2, duckdb_memory_limit, duckdb_threads, duckdb_temp_directory.

## Boundaries
- Safe to edit: `src/`, `tests/`
- Do not edit: `uv.lock`, `src/scdm_qa/schemas/tables_documentation.json` (upstream SCDM spec)

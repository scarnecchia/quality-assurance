# SCDM Quality Assurance

Last verified: 2026-03-09

## Tech Stack
- Language: Python 3.12+
- CLI: Typer
- Data: Polars (DataFrames), pointblank (validation rules)
- Formats: Parquet (pyarrow), SAS7BDAT (pyreadstat)
- Reporting: Jinja2, great-tables, css-inline
- Logging: structlog
- Build: uv (uv_build backend)
- Testing: pytest

## Commands
- `uv run pytest` - Run tests
- `uv run scdm-qa run <config.toml>` - Validate tables
- `uv run scdm-qa profile <config.toml>` - Profile only (no validation)
- `uv run scdm-qa schema [TABLE]` - Show SCDM schema definitions
- `uv run scdm-qa serve <report-dir>` - Browse HTML reports locally

## Project Structure
- `src/scdm_qa/` - Main package
  - `cli.py` - Typer CLI entry point (4 subcommands: run, profile, schema, serve)
  - `config.py` - TOML config loader (`QAConfig` dataclass)
  - `pipeline.py` - Orchestrator: per-table isolation, exit code logic
  - `logging.py` - structlog setup (console + JSON file)
  - `schemas/` - SCDM table definitions, JSON spec parser, pointblank rule builder
  - `readers/` - Chunked file readers (Parquet, SAS) behind a Protocol
  - `validation/` - Per-chunk validation runner, accumulator, global checks
  - `profiling/` - Streaming column statistics accumulator
  - `reporting/` - HTML report builder and multi-table index page
- `tests/` - pytest tests (one file per module)
- `docs/implementation-plans/` - Phase plans (reference only)

## Conventions
- All data models are frozen dataclasses
- Chunks are `polars.DataFrame` throughout the pipeline
- Validation uses pointblank for rule expression
- Single-pass architecture: profiling runs inside the validation loop
- Global checks (uniqueness, sort order) require separate scans
- Exit codes: 0=pass, 1=warnings (within threshold), 2=errors/threshold exceeded

## Configuration
TOML config with `[tables]` section mapping table keys to file paths, plus `[options]` for chunk_size, max_failing_rows, error_threshold, output_dir, custom_rules_dir, log_file, verbose.

## Boundaries
- Safe to edit: `src/`, `tests/`
- Do not edit: `uv.lock`, `src/scdm_qa/schemas/tables_documentation.json` (upstream SCDM spec)

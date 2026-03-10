# SCDM-QA Implementation Plan — Phase 8: CLI Integration & Error Handling

**Goal:** Wire all pipeline components into the CLI, implement per-table isolation, error handling, exit codes, and the serve command.

**Architecture:** The `run` command orchestrates the full pipeline per table: create reader → run per-chunk validation + profiling in single pass → run global checks → build report. Each table is processed independently — a failure in one does not block others. The `profile` command runs profiling only. The `serve` command launches a local HTTP server. Exit codes reflect overall outcome.

**Tech Stack:** Python >=3.12, typer 0.24.x, http.server (stdlib)

**Scope:** 8 phases from original design (phase 8 of 8)

**Codebase verified:** 2026-03-09

---

## Acceptance Criteria Coverage

This phase implements and tests:

### scdm-qa.AC1: CLI tool validates SCDM data and produces reports
- **scdm-qa.AC1.1 Success:** `scdm-qa run config.toml` validates all configured tables and produces HTML reports in output directory
- **scdm-qa.AC1.3 Success:** When one table fails, remaining tables still validate successfully
- **scdm-qa.AC1.4 Success:** Exit code 0 when all checks pass, 1 when warnings (threshold exceeded), 2 when failures
- **scdm-qa.AC1.5 Success:** `scdm-qa profile config.toml` runs profiling only (no rule validation)
- **scdm-qa.AC1.6 Success:** `scdm-qa serve ./qa-reports/` launches local HTTP server serving report files

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Create pipeline orchestrator

**Files:**
- Create: `src/scdm_qa/pipeline.py`

**Step 1: Create the file**

This module contains the core pipeline logic, separate from the CLI, to keep the CLI thin and the logic testable.

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import structlog

from scdm_qa.config import QAConfig
from scdm_qa.profiling.accumulator import ProfilingAccumulator
from scdm_qa.profiling.results import ProfilingResult
from scdm_qa.readers import UnsupportedFormatError, create_reader
from scdm_qa.reporting.builder import save_table_report
from scdm_qa.reporting.index import make_report_summary, save_index
from scdm_qa.schemas import get_schema
from scdm_qa.schemas.custom_rules import load_custom_rules
from scdm_qa.validation.global_checks import check_sort_order, check_uniqueness
from scdm_qa.validation.results import StepResult, ValidationResult
from scdm_qa.validation.runner import run_validation

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class TableOutcome:
    table_key: str
    success: bool
    validation_result: ValidationResult | None = None
    profiling_result: ProfilingResult | None = None
    error: str | None = None


def run_pipeline(
    config: QAConfig,
    *,
    table_filter: str | None = None,
    profile_only: bool = False,
) -> list[TableOutcome]:
    tables = config.tables
    if table_filter:
        if table_filter not in tables:
            log.error("table not found in config", table=table_filter, available=list(tables.keys()))
            return [TableOutcome(table_key=table_filter, success=False, error=f"table {table_filter!r} not in config")]
        tables = {table_filter: tables[table_filter]}

    outcomes: list[TableOutcome] = []
    report_summaries: list[dict] = []

    for table_key, file_path in tables.items():
        log.info("processing table", table=table_key, file=str(file_path))
        try:
            outcome = _process_table(
                table_key,
                file_path,
                config,
                profile_only=profile_only,
            )
            outcomes.append(outcome)

            if outcome.validation_result and outcome.profiling_result:
                save_table_report(
                    config.output_dir,
                    table_key,
                    outcome.validation_result,
                    outcome.profiling_result,
                )
                report_summaries.append(
                    make_report_summary(
                        table_key,
                        outcome.validation_result.table_name,
                        outcome.validation_result.total_rows,
                        len(outcome.validation_result.steps),
                        outcome.validation_result.total_failures,
                    )
                )
            elif outcome.profiling_result:
                # Profile-only mode: create a report with just profiling data
                empty_vr = ValidationResult(
                    table_key=table_key,
                    table_name=outcome.profiling_result.table_name,
                    steps=(),
                    total_rows=outcome.profiling_result.total_rows,
                    chunks_processed=0,
                )
                save_table_report(
                    config.output_dir,
                    table_key,
                    empty_vr,
                    outcome.profiling_result,
                )
                report_summaries.append(
                    make_report_summary(
                        table_key,
                        outcome.profiling_result.table_name,
                        outcome.profiling_result.total_rows,
                        0,
                        0,
                    )
                )

        except Exception as exc:
            log.error("table processing failed", table=table_key, error=str(exc))
            outcomes.append(TableOutcome(table_key=table_key, success=False, error=str(exc)))

    if report_summaries:
        save_index(config.output_dir, report_summaries)

    return outcomes


def _process_table(
    table_key: str,
    file_path: Path,
    config: QAConfig,
    *,
    profile_only: bool = False,
) -> TableOutcome:
    if not file_path.exists():
        return TableOutcome(table_key=table_key, success=False, error=f"file not found: {file_path}")

    schema = get_schema(table_key)
    reader = create_reader(file_path, chunk_size=config.chunk_size)
    custom_extend_fn = load_custom_rules(table_key, config.custom_rules_dir)

    profiling_acc = ProfilingAccumulator(schema)

    if profile_only:
        for chunk in reader.chunks():
            profiling_acc.add_chunk(chunk)
        return TableOutcome(
            table_key=table_key,
            success=True,
            profiling_result=profiling_acc.result(),
        )

    # Single-pass: profiling accumulator runs inside validation runner
    validation_result = run_validation(
        reader,
        schema,
        max_failing_rows=config.max_failing_rows,
        profiling_accumulator=profiling_acc,
        custom_extend_fn=custom_extend_fn,
    )

    profiling_result = profiling_acc.result()

    # Global checks (uniqueness + sort order)
    global_steps: list[StepResult] = []

    if schema.unique_row:
        uniqueness_reader = create_reader(file_path, chunk_size=config.chunk_size)
        uniqueness_step = check_uniqueness(
            file_path,
            schema,
            chunks=uniqueness_reader.chunks(),
            max_failing_rows=config.max_failing_rows,
        )
        if uniqueness_step is not None:
            global_steps.append(uniqueness_step)

    if schema.sort_order:
        # NOTE: This requires a second scan. Could be optimised by collecting
        # chunk boundary rows during the validation pass.
        sort_reader = create_reader(file_path, chunk_size=config.chunk_size)
        sort_step = check_sort_order(schema, sort_reader.chunks())
        if sort_step is not None:
            global_steps.append(sort_step)

    if global_steps:
        all_steps = list(validation_result.steps) + global_steps
        validation_result = ValidationResult(
            table_key=validation_result.table_key,
            table_name=validation_result.table_name,
            steps=tuple(all_steps),
            total_rows=validation_result.total_rows,
            chunks_processed=validation_result.chunks_processed,
        )

    return TableOutcome(
        table_key=table_key,
        success=True,
        validation_result=validation_result,
        profiling_result=profiling_result,
    )


def compute_exit_code(
    outcomes: list[TableOutcome],
    *,
    error_threshold: float = 0.05,
) -> int:
    """Compute CLI exit code from pipeline outcomes.

    Returns:
        0: all checks pass (no failures)
        1: some failures exist but all within threshold (warnings)
        2: processing errors or at least one step exceeds error threshold
    """
    has_errors = any(not o.success for o in outcomes)
    if has_errors:
        return 2

    has_failures = False
    has_threshold_exceedance = False

    for o in outcomes:
        if o.validation_result is None:
            continue
        for step in o.validation_result.steps:
            if step.n_failed > 0:
                has_failures = True
                if step.f_failed > error_threshold:
                    has_threshold_exceedance = True

    if has_threshold_exceedance:
        return 2
    if has_failures:
        return 1
    return 0
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.pipeline import run_pipeline, compute_exit_code; print('pipeline imported OK')"`
Expected: `pipeline imported OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/pipeline.py
git commit -m "feat: add pipeline orchestrator with per-table isolation and exit codes"
```
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update CLI with full implementation

**Files:**
- Modify: `src/scdm_qa/cli.py`

**Step 1: Replace CLI stubs with full implementation**

Replace the stub implementations with calls to the pipeline orchestrator. The `run` command calls `run_pipeline()`, the `profile` command calls it with `profile_only=True`, and the `serve` command launches `http.server`.

```python
from __future__ import annotations

import http.server
import sys
import threading
import webbrowser
from pathlib import Path
from typing import Annotated, Optional

import typer

from scdm_qa.config import ConfigError, load_config
from scdm_qa.logging import configure_logging, get_logger
from scdm_qa.pipeline import compute_exit_code, run_pipeline

app = typer.Typer(
    name="scdm-qa",
    help="Validate Sentinel Common Data Model (SCDM) data tables.",
    no_args_is_help=True,
)


@app.command()
def run(
    config: Annotated[Path, typer.Argument(help="Path to TOML configuration file")],
    table: Annotated[Optional[str], typer.Option(help="Validate only this table")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable verbose output")] = False,
) -> None:
    """Validate SCDM tables and produce HTML reports."""
    cfg = _load_and_configure(config, verbose)
    log = get_logger("scdm_qa.cli")
    log.info("starting validation", tables=list(cfg.tables.keys()), table_filter=table)

    outcomes = run_pipeline(cfg, table_filter=table)

    for outcome in outcomes:
        if outcome.success and outcome.validation_result:
            vr = outcome.validation_result
            status = "PASS" if vr.all_passed else "FAIL"
            typer.echo(f"  {outcome.table_key}: {status} ({vr.total_rows:,} rows, {vr.total_failures:,} failures)")
        elif outcome.success:
            typer.echo(f"  {outcome.table_key}: OK")
        else:
            typer.echo(f"  {outcome.table_key}: ERROR — {outcome.error}")

    exit_code = compute_exit_code(outcomes, error_threshold=cfg.error_threshold)
    typer.echo(f"\nReports written to: {cfg.output_dir}")
    raise typer.Exit(code=exit_code)


@app.command()
def profile(
    config: Annotated[Path, typer.Argument(help="Path to TOML configuration file")],
    table: Annotated[Optional[str], typer.Option(help="Profile only this table")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable verbose output")] = False,
) -> None:
    """Run profiling only (no rule validation)."""
    cfg = _load_and_configure(config, verbose)
    log = get_logger("scdm_qa.cli")
    log.info("starting profiling", tables=list(cfg.tables.keys()), table_filter=table)

    outcomes = run_pipeline(cfg, table_filter=table, profile_only=True)

    for outcome in outcomes:
        if outcome.success:
            typer.echo(f"  {outcome.table_key}: profiled")
        else:
            typer.echo(f"  {outcome.table_key}: ERROR — {outcome.error}")

    typer.echo(f"\nReports written to: {cfg.output_dir}")


@app.command()
def schema(
    table: Annotated[Optional[str], typer.Argument(help="Show schema for this table")] = None,
) -> None:
    """Display SCDM schema definitions."""
    from scdm_qa.schemas import get_schema, list_table_keys

    if table is None:
        keys = list_table_keys()
        typer.echo(f"{len(keys)} SCDM tables:")
        for key in keys:
            s = get_schema(key)
            typer.echo(f"  {key}: {len(s.columns)} columns, unique_row={list(s.unique_row)}")
    else:
        try:
            s = get_schema(table)
        except KeyError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(code=2) from e
        typer.echo(f"{s.table_name} ({s.table_key})")
        typer.echo(f"  Sort order: {list(s.sort_order)}")
        typer.echo(f"  Unique row: {list(s.unique_row)}")
        typer.echo(f"  Columns ({len(s.columns)}):")
        for col in s.columns:
            nullable = "nullable" if col.missing_allowed else "NOT NULL"
            typer.echo(f"    {col.name}: {col.col_type} ({nullable})")


@app.command()
def serve(
    report_dir: Annotated[Path, typer.Argument(help="Directory containing HTML reports")],
    port: Annotated[int, typer.Option(help="Port to serve on")] = 8080,
) -> None:
    """Launch local HTTP server for browsing reports."""
    if not report_dir.exists():
        typer.echo(f"error: report directory not found: {report_dir}", err=True)
        raise typer.Exit(code=2)

    import os
    os.chdir(report_dir)

    handler = http.server.SimpleHTTPRequestHandler
    server = http.server.HTTPServer(("", port), handler)

    url = f"http://localhost:{port}/"
    typer.echo(f"Serving reports at {url}")
    typer.echo("Press Ctrl+C to stop.")

    threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        typer.echo("\nStopped.")
        server.server_close()


def _load_and_configure(config_path: Path, verbose: bool) -> "QAConfig":
    from scdm_qa.config import QAConfig

    try:
        cfg = load_config(config_path)
    except ConfigError as e:
        typer.echo(f"error: {e}", err=True)
        raise typer.Exit(code=2) from e

    log_file = cfg.log_file
    configure_logging(log_file=log_file, verbose=verbose or cfg.verbose)
    return cfg
```

**Step 2: Verify operationally**

Run: `uv run scdm-qa --help`
Expected: Shows all four subcommands with descriptions.

Run: `uv run scdm-qa schema`
Expected: Lists all 19 SCDM table keys.

**Step 3: Commit**

```bash
git add src/scdm_qa/cli.py
git commit -m "feat: wire full pipeline into CLI with per-table isolation and exit codes"
```
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Test CLI integration and pipeline

**Verifies:** scdm-qa.AC1.1, scdm-qa.AC1.3, scdm-qa.AC1.4, scdm-qa.AC1.5, scdm-qa.AC1.6

**Files:**
- Modify: `tests/test_cli.py` (replace Phase 1 stub tests with full integration tests)

**Implementation:**

Integration tests create Parquet files with known data, write TOML configs, and invoke the CLI commands via typer's CliRunner. Tests verify exit codes, report file creation, per-table isolation (one table failing doesn't block others), and the profile-only mode.

**Testing:**
- scdm-qa.AC1.1: `scdm-qa run config.toml` creates HTML reports in output directory
- scdm-qa.AC1.3: When one table has failures, the other table's report still gets created
- scdm-qa.AC1.4: Exit code 0 for clean data, exit code 1 for data with failures
- scdm-qa.AC1.5: `scdm-qa profile config.toml` runs without validation steps
- scdm-qa.AC1.6: `scdm-qa serve` stub test (cannot test full HTTP server in unit tests — verify it starts and stops)

```python
from __future__ import annotations

from pathlib import Path

import polars as pl
from typer.testing import CliRunner

from scdm_qa.cli import app

runner = CliRunner()


class TestCLIHelp:
    def test_help_shows_subcommands(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "profile" in result.output
        assert "schema" in result.output
        assert "serve" in result.output


class TestSchemaCommand:
    def test_lists_all_tables(self) -> None:
        result = runner.invoke(app, ["schema"])
        assert result.exit_code == 0
        assert "19 SCDM tables" in result.output

    def test_shows_specific_table(self) -> None:
        result = runner.invoke(app, ["schema", "demographic"])
        assert result.exit_code == 0
        assert "PatID" in result.output


class TestRunCommand:
    def _make_config_and_data(self, tmp_path: Path, *, with_nulls: bool = False) -> Path:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        patids = ["P1", "P2", "P3", "P4", "P5"]
        if with_nulls:
            patids[2] = None

        df = pl.DataFrame({
            "PatID": patids,
            "Birth_Date": [1000, 2000, 3000, 4000, 5000],
            "Sex": ["F", "M", "F", "M", "F"],
            "Hispanic": ["Y", "N", "Y", "N", "Y"],
            "Race": ["1", "2", "3", "1", "2"],
        })
        df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\nchunk_size = 2\n'
        )
        return config_file

    def test_produces_reports_for_clean_data(self, tmp_path: Path) -> None:
        config_file = self._make_config_and_data(tmp_path)
        result = runner.invoke(app, ["run", str(config_file)])
        assert result.exit_code == 0
        assert (tmp_path / "reports" / "demographic.html").exists()
        assert (tmp_path / "reports" / "index.html").exists()

    def test_exit_code_2_when_failures_exceed_threshold(self, tmp_path: Path) -> None:
        config_file = self._make_config_and_data(tmp_path, with_nulls=True)
        result = runner.invoke(app, ["run", str(config_file)])
        # 1 null out of 5 = 20% failure rate > default 5% threshold → exit 2
        assert result.exit_code == 2

    def test_missing_config_exits_2(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["run", str(tmp_path / "missing.toml")])
        assert result.exit_code == 2


class TestRunCommandTableIsolation:
    def test_one_table_failure_doesnt_block_others(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Good table
        pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }).write_parquet(data_dir / "demographic.parquet")

        # Bad table (missing file to trigger error)
        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\n'
            f'demographic = "{data_dir / "demographic.parquet"}"\n'
            f'enrollment = "{data_dir / "nonexistent.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\n'
        )
        result = runner.invoke(app, ["run", str(config_file)])
        # Should still create demographic report even though enrollment fails
        assert (tmp_path / "reports" / "demographic.html").exists()


class TestProfileCommand:
    def test_runs_profiling_only(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }).write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\n'
        )
        result = runner.invoke(app, ["profile", str(config_file)])
        assert result.exit_code == 0
        assert "profiled" in result.output


class TestServeCommand:
    def test_nonexistent_dir_exits_2(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["serve", str(tmp_path / "nope")])
        assert result.exit_code == 2
```

**Verification:**

Run: `uv run pytest tests/test_cli.py -v`
Expected: All tests pass.

**Commit:** `test: add full CLI integration tests with pipeline verification`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

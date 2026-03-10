from __future__ import annotations

import functools
import http.server
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

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(report_dir))
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

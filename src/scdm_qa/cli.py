from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer

from scdm_qa.config import ConfigError, load_config
from scdm_qa.logging import configure_logging, get_logger

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
    log.info("run command invoked", tables=list(cfg.tables.keys()), table_filter=table)
    typer.echo("Run command: not yet implemented")


@app.command()
def profile(
    config: Annotated[Path, typer.Argument(help="Path to TOML configuration file")],
    table: Annotated[Optional[str], typer.Option(help="Profile only this table")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable verbose output")] = False,
) -> None:
    """Run profiling only (no rule validation)."""
    cfg = _load_and_configure(config, verbose)
    log = get_logger("scdm_qa.cli")
    log.info("profile command invoked", tables=list(cfg.tables.keys()), table_filter=table)
    typer.echo("Profile command: not yet implemented")


@app.command()
def schema(
    table: Annotated[Optional[str], typer.Argument(help="Show schema for this table")] = None,
) -> None:
    """Display SCDM schema definitions."""
    typer.echo("Schema command: not yet implemented")


@app.command()
def serve(
    report_dir: Annotated[Path, typer.Argument(help="Directory containing HTML reports")],
    port: Annotated[int, typer.Option(help="Port to serve on")] = 8080,
) -> None:
    """Launch local HTTP server for browsing reports."""
    typer.echo(f"Serve command: not yet implemented (would serve {report_dir} on port {port})")


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

# SCDM-QA Implementation Plan — Phase 1: Project Scaffolding

**Goal:** Installable Python package with CLI entry point, structured logging, and TOML config loading.

**Architecture:** Standard src-layout Python package using typer for CLI, structlog for dual-output logging (console + JSON file), and tomllib for TOML config parsing. Follows the Functional Core / Imperative Shell pattern established by sibling project qa_aggregate.

**Tech Stack:** Python >=3.12, typer, structlog, tomllib (stdlib), uv (build/run)

**Scope:** 8 phases from original design (phase 1 of 8)

**Codebase verified:** 2026-03-09

---

## Acceptance Criteria Coverage

This phase is infrastructure scaffolding. **Verifies: None** — verification is operational (install succeeds, CLI runs, logging works, config loads).

---

<!-- START_TASK_1 -->
### Task 1: Create pyproject.toml

**Files:**
- Create: `pyproject.toml`

**Step 1: Create the file**

```toml
[build-system]
requires = ["uv_build>=0.10.2,<0.11.0"]
build-backend = "uv_build"

[project]
name = "scdm-qa"
version = "0.1.0"
description = "Validates health data tables conforming to the Sentinel Common Data Model (SCDM)"
requires-python = ">=3.12"
dependencies = [
    "typer>=0.24,<1",
    "structlog>=25,<26",
    "polars>=1.38,<2",
    "pointblank>=0.6,<1",
    "pyreadstat>=1.3,<2",
    "great-tables>=0.21,<1",
]

[project.scripts]
scdm-qa = "scdm_qa.cli:app"

[project.optional-dependencies]
duckdb = [
    "duckdb>=1,<2",
]

[dependency-groups]
dev = [
    "pytest>=9,<10",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = "-v --tb=short"
```

**Step 2: Verify operationally**

Run: `uv sync`
Expected: Dependencies install without errors, `uv.lock` is generated.

**Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add pyproject.toml with dependencies and CLI entry point"
```
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create package structure and __init__.py

**Files:**
- Create: `src/scdm_qa/__init__.py`
- Create: `tests/__init__.py`

**Step 1: Create the files**

Create `src/scdm_qa/__init__.py`:
```python
"""SCDM Quality Assurance — validates Sentinel Common Data Model tables."""
```

Create `tests/__init__.py` (empty file).

**Step 2: Verify operationally**

Run: `uv run python -c "import scdm_qa; print(scdm_qa.__doc__)"`
Expected: Prints the module docstring without errors.

**Step 3: Commit**

```bash
git add src/scdm_qa/__init__.py tests/__init__.py
git commit -m "chore: create scdm_qa package and tests directory"
```
<!-- END_TASK_2 -->

<!-- START_SUBCOMPONENT_A (tasks 3-4) -->
<!-- START_TASK_3 -->
### Task 3: Create structlog logging configuration

**Files:**
- Create: `src/scdm_qa/logging.py`

**Step 1: Create the file**

```python
from __future__ import annotations

import logging
import sys
from pathlib import Path

import structlog


def configure_logging(*, log_file: Path | None = None, verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    handlers: list[logging.Handler] = []

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(),
        ],
    )
    console_handler.setFormatter(console_formatter)
    handlers.append(console_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG)
    for handler in handlers:
        root_logger.addHandler(handler)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.logging import configure_logging, get_logger; configure_logging(); log = get_logger('test'); log.info('hello', key='value')"`
Expected: Prints a colourized log line to stderr with timestamp, level, and key-value pair.

**Step 3: Commit**

```bash
git add src/scdm_qa/logging.py
git commit -m "feat: add structlog logging with console and JSON file output"
```
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Test logging configuration

**Verifies:** None (infrastructure verification)

**Files:**
- Create: `tests/test_logging.py`

**Step 1: Write the tests**

```python
from __future__ import annotations

import json
import logging
from pathlib import Path

import structlog

from scdm_qa.logging import configure_logging, get_logger


class TestConfigureLogging:
    def test_configures_console_handler(self) -> None:
        configure_logging()

        root = logging.getLogger()
        assert any(isinstance(h, logging.StreamHandler) for h in root.handlers)

    def test_configures_file_handler_when_log_file_provided(self, tmp_path: Path) -> None:
        log_file = tmp_path / "test.log"
        configure_logging(log_file=log_file)

        root = logging.getLogger()
        assert any(isinstance(h, logging.FileHandler) for h in root.handlers)

    def test_file_handler_writes_json(self, tmp_path: Path) -> None:
        log_file = tmp_path / "test.log"
        configure_logging(log_file=log_file)

        logger = get_logger("test")
        logger.info("test_event", key="value")

        content = log_file.read_text().strip()
        assert content, "log file should not be empty"
        record = json.loads(content.splitlines()[-1])
        assert record["event"] == "test_event"
        assert record["key"] == "value"

    def test_creates_log_directory_if_missing(self, tmp_path: Path) -> None:
        log_file = tmp_path / "subdir" / "nested" / "test.log"
        configure_logging(log_file=log_file)

        assert log_file.parent.exists()


class TestGetLogger:
    def test_returns_bound_logger(self) -> None:
        configure_logging()
        logger = get_logger("test")
        assert isinstance(logger, structlog.stdlib.BoundLogger)
```

**Step 2: Run tests**

Run: `uv run pytest tests/test_logging.py -v`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add tests/test_logging.py
git commit -m "test: add logging configuration tests"
```
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 5-6) -->
<!-- START_TASK_5 -->
### Task 5: Create TOML config loader

**Files:**
- Create: `src/scdm_qa/config.py`

**Step 1: Create the file**

```python
from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class QAConfig:
    tables: dict[str, Path]
    output_dir: Path = Path("./qa-reports")
    chunk_size: int = 500_000
    max_failing_rows: int = 500
    error_threshold: float = 0.05
    custom_rules_dir: Path | None = None
    log_file: Path | None = None
    verbose: bool = False


class ConfigError(Exception):
    pass


def load_config(config_path: Path) -> QAConfig:
    if not config_path.exists():
        raise ConfigError(f"config file not found: {config_path}")

    if not config_path.suffix == ".toml":
        raise ConfigError(f"config file must be .toml: {config_path}")

    with open(config_path, "rb") as f:
        raw = tomllib.load(f)

    tables_raw = raw.get("tables")
    if not tables_raw or not isinstance(tables_raw, dict):
        raise ConfigError("config must contain a [tables] section with at least one table")

    tables: dict[str, Path] = {}
    for name, path_str in tables_raw.items():
        p = Path(path_str)
        tables[name] = p

    options = raw.get("options", {})

    output_dir = Path(options.get("output_dir", "./qa-reports"))
    chunk_size = options.get("chunk_size", 500_000)
    max_failing_rows = options.get("max_failing_rows", 500)
    error_threshold = options.get("error_threshold", 0.05)

    custom_rules_dir_str = options.get("custom_rules_dir")
    custom_rules_dir = Path(custom_rules_dir_str) if custom_rules_dir_str else None

    log_file_str = options.get("log_file")
    log_file = Path(log_file_str) if log_file_str else None

    verbose = options.get("verbose", False)

    if not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ConfigError(f"chunk_size must be a positive integer, got: {chunk_size}")

    if not isinstance(max_failing_rows, int) or max_failing_rows <= 0:
        raise ConfigError(f"max_failing_rows must be a positive integer, got: {max_failing_rows}")

    if not isinstance(error_threshold, (int, float)) or error_threshold < 0 or error_threshold > 1:
        raise ConfigError(f"error_threshold must be a float between 0 and 1, got: {error_threshold}")

    return QAConfig(
        tables=tables,
        output_dir=output_dir,
        chunk_size=chunk_size,
        max_failing_rows=max_failing_rows,
        error_threshold=float(error_threshold),
        custom_rules_dir=custom_rules_dir,
        log_file=log_file,
        verbose=verbose,
    )
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.config import load_config; print(load_config.__doc__)"`
Expected: No import errors.

**Step 3: Commit**

```bash
git add src/scdm_qa/config.py
git commit -m "feat: add TOML config loader with validation"
```
<!-- END_TASK_5 -->

<!-- START_TASK_6 -->
### Task 6: Test config loader

**Verifies:** None (infrastructure verification)

**Files:**
- Create: `tests/test_config.py`

**Step 1: Write the tests**

```python
from __future__ import annotations

from pathlib import Path

import pytest

from scdm_qa.config import ConfigError, QAConfig, load_config


class TestLoadConfig:
    def test_loads_minimal_config(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/enrollment.parquet"\n'
        )
        cfg = load_config(config_file)

        assert "enrollment" in cfg.tables
        assert cfg.tables["enrollment"] == Path("/data/enrollment.parquet")
        assert cfg.chunk_size == 500_000
        assert cfg.output_dir == Path("./qa-reports")

    def test_loads_full_config(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\n'
            'enrollment = "/data/enrollment.parquet"\n'
            'demographic = "/data/demographic.sas7bdat"\n'
            '\n'
            '[options]\n'
            'chunk_size = 100000\n'
            'output_dir = "./output"\n'
            'max_failing_rows = 100\n'
            'error_threshold = 0.10\n'
            'custom_rules_dir = "./rules"\n'
            'log_file = "./logs/qa.log"\n'
            'verbose = true\n'
        )
        cfg = load_config(config_file)

        assert len(cfg.tables) == 2
        assert cfg.chunk_size == 100_000
        assert cfg.output_dir == Path("./output")
        assert cfg.max_failing_rows == 100
        assert cfg.error_threshold == 0.10
        assert cfg.custom_rules_dir == Path("./rules")
        assert cfg.log_file == Path("./logs/qa.log")
        assert cfg.verbose is True

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="config file not found"):
            load_config(tmp_path / "nonexistent.toml")

    def test_raises_on_non_toml_extension(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text("")
        with pytest.raises(ConfigError, match="must be .toml"):
            load_config(config_file)

    def test_raises_on_missing_tables_section(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text('[options]\nchunk_size = 100\n')
        with pytest.raises(ConfigError, match="must contain a \\[tables\\] section"):
            load_config(config_file)

    def test_raises_on_invalid_chunk_size(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/e.parquet"\n\n'
            '[options]\nchunk_size = -1\n'
        )
        with pytest.raises(ConfigError, match="chunk_size must be a positive integer"):
            load_config(config_file)


class TestQAConfig:
    def test_is_frozen(self) -> None:
        cfg = QAConfig(tables={"a": Path("/a")})
        with pytest.raises(AttributeError):
            cfg.chunk_size = 999  # type: ignore[misc]
```

**Step 2: Run tests**

Run: `uv run pytest tests/test_config.py -v`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add tests/test_config.py
git commit -m "test: add config loader tests"
```
<!-- END_TASK_6 -->
<!-- END_SUBCOMPONENT_B -->

<!-- START_TASK_7 -->
### Task 7: Create typer CLI with subcommand stubs

**Files:**
- Create: `src/scdm_qa/cli.py`

**Step 1: Create the file**

```python
from __future__ import annotations

import sys
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
```

**Step 2: Verify operationally**

Run: `uv run scdm-qa --help`
Expected: Prints usage with `run`, `profile`, `schema`, `serve` subcommands.

Run: `uv run scdm-qa run --help`
Expected: Prints help for the run subcommand showing `CONFIG` argument and `--table`/`--verbose` options.

**Step 3: Commit**

```bash
git add src/scdm_qa/cli.py
git commit -m "feat: add typer CLI with run, profile, schema, serve subcommands"
```
<!-- END_TASK_7 -->

<!-- START_TASK_8 -->
### Task 8: Verify end-to-end CLI + config + logging integration

**Files:**
- Create: `tests/test_cli.py`

**Step 1: Write the tests**

```python
from __future__ import annotations

from pathlib import Path

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


class TestRunCommand:
    def test_run_with_valid_config(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/enrollment.parquet"\n'
        )
        result = runner.invoke(app, ["run", str(config_file)])
        assert result.exit_code == 0

    def test_run_with_missing_config(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["run", str(tmp_path / "missing.toml")])
        assert result.exit_code == 2

    def test_run_with_table_filter(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/enrollment.parquet"\n'
        )
        result = runner.invoke(app, ["run", str(config_file), "--table", "enrollment"])
        assert result.exit_code == 0


class TestProfileCommand:
    def test_profile_with_valid_config(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/enrollment.parquet"\n'
        )
        result = runner.invoke(app, ["profile", str(config_file)])
        assert result.exit_code == 0


class TestServeCommand:
    def test_serve_prints_stub_message(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["serve", str(tmp_path)])
        assert result.exit_code == 0
        assert "not yet implemented" in result.output
```

**Step 2: Run all tests**

Run: `uv run pytest tests/ -v`
Expected: All tests in test_logging.py, test_config.py, and test_cli.py pass.

**Step 3: Commit**

```bash
git add tests/test_cli.py
git commit -m "test: add CLI integration tests"
```
<!-- END_TASK_8 -->

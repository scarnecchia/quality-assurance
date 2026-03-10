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

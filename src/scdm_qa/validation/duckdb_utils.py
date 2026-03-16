"""DuckDB connection factory with resource constraints."""

from __future__ import annotations

from pathlib import Path

import duckdb
import structlog

log = structlog.get_logger(__name__)


def create_connection(
    *,
    memory_limit: str = "96GB",
    threads: int = 10,
    temp_directory: Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB in-memory connection with resource constraints.

    Args:
        memory_limit: Maximum memory DuckDB may use (e.g. "96GB", "4GB", "512MB").
        threads: Maximum number of threads DuckDB may use.
        temp_directory: Directory for spill-to-disk temp files. None uses DuckDB default.

    Returns:
        Configured DuckDB connection.
    """
    conn = duckdb.connect(":memory:")
    conn.execute(f"SET memory_limit = '{memory_limit}'")
    conn.execute(f"SET threads = {threads}")
    if temp_directory is not None:
        safe_dir = str(temp_directory).replace("'", "''")
        conn.execute(f"SET temp_directory = '{safe_dir}'")

    log.debug(
        "duckdb connection created",
        memory_limit=memory_limit,
        threads=threads,
        temp_directory=str(temp_directory) if temp_directory else None,
    )
    return conn

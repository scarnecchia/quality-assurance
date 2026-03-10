"""Cross-table validation orchestrator using DuckDB SQL engine."""

# pattern: Imperative Shell

from __future__ import annotations

import structlog
import tempfile
from pathlib import Path

import duckdb
import polars as pl

from scdm_qa.config import QAConfig
from scdm_qa.schemas import get_schema
from scdm_qa.schemas.models import CrossTableCheckDef
from scdm_qa.validation.results import StepResult

log = structlog.get_logger(__name__)


def run_cross_table_checks(
    config: QAConfig,
    checks: tuple[CrossTableCheckDef, ...],
    *,
    table_filter: str | None = None,
) -> list[StepResult]:
    """Execute cross-table validation checks via DuckDB.

    Args:
        config: QAConfig with table file paths
        checks: Tuple of cross-table check definitions
        table_filter: Optional filter to run only checks involving this table

    Returns:
        List of StepResult objects (one per check executed)
    """
    results: list[StepResult] = []
    registered_views: set[str] = set()
    temp_parquet_files: list[Path] = []
    conn: duckdb.DuckDBPyConnection | None = None

    try:
        conn = duckdb.connect(":memory:")

        # Register all tables as views
        for table_key, file_path in config.tables.items():
            try:
                if file_path.suffix.lower() == ".sas7bdat":
                    temp_path = _convert_sas_to_parquet(file_path, chunk_size=config.chunk_size)
                    temp_parquet_files.append(temp_path)
                    safe_path = str(temp_path).replace("'", "''")
                else:
                    safe_path = str(file_path).replace("'", "''")

                conn.execute(f'CREATE VIEW "{table_key}" AS SELECT * FROM read_parquet(\'{safe_path}\')')
                registered_views.add(table_key)
                log.info(f"registered view {table_key}", extra={"path": str(file_path)})
            except Exception as e:
                log.warning(f"failed to register table {table_key}", extra={"error": str(e)})

        # Filter checks if table_filter is specified
        checks_to_run = checks
        if table_filter:
            checks_to_run = tuple(
                c
                for c in checks
                if c.source_table == table_filter or c.reference_table == table_filter
            )

        # Execute each check
        for check in checks_to_run:
            # Verify referenced tables exist
            if check.source_table not in registered_views:
                log.warning(f"skipping check {check.check_id}: source table {check.source_table} not in config")
                continue
            if check.reference_table and check.reference_table not in registered_views:
                log.warning(
                    f"skipping check {check.check_id}: reference table {check.reference_table} not in config"
                )
                continue

            try:
                result = _run_check(conn, check, config)
                results.append(result)
            except duckdb.Error as e:
                log.error(f"check {check.check_id} failed with DuckDB error: {e}")
                results.append(
                    StepResult(
                        step_index=-1,
                        assertion_type="cross_table",
                        column=check.source_column or "",
                        description=f"Check {check.check_id} error: {e}",
                        n_passed=0,
                        n_failed=0,
                        failing_rows=None,
                        check_id=check.check_id,
                        severity=check.severity,
                    )
                )
    finally:
        # Close connection
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                log.warning(f"failed to close DuckDB connection", extra={"error": str(e)})

        # Clean up temp parquet files
        for tmp_path in temp_parquet_files:
            try:
                tmp_path.unlink()
            except Exception as e:
                log.warning(f"failed to delete temp parquet file {tmp_path}", extra={"error": str(e)})

    return results


def _convert_sas_to_parquet(sas_path: Path, chunk_size: int = 500_000) -> Path:
    """Convert SAS7BDAT to temp parquet for DuckDB registration using chunked reading.

    Args:
        sas_path: Path to .sas7bdat file
        chunk_size: Chunk size for reading

    Returns:
        Path to temporary parquet file

    Note:
        This still concatenates all chunks in memory before writing. For truly large files
        that don't fit in memory, an incremental parquet writer would be needed.
    """
    from scdm_qa.readers import create_reader

    reader = create_reader(sas_path, chunk_size=chunk_size)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    chunks = list(reader.chunks())
    if chunks:
        combined = pl.concat(chunks)
        combined.write_parquet(tmp_path)
    else:
        pl.DataFrame().write_parquet(tmp_path)

    log.warning(
        "converted SAS file to temp parquet",
        extra={"sas_path": str(sas_path), "tmp_path": str(tmp_path)},
    )
    return tmp_path


def _run_check(
    conn: duckdb.DuckDBPyConnection,
    check: CrossTableCheckDef,
    config: QAConfig,
) -> StepResult:
    """Dispatch to appropriate handler based on check_type.

    Args:
        conn: DuckDB connection
        check: Check definition
        config: QAConfig for access to max_failing_rows

    Returns:
        StepResult with check results
    """
    if check.check_type == "referential_integrity":
        return _handle_referential_integrity(conn, check, config)
    elif check.check_type == "length_consistency":
        return _handle_length_consistency(conn, check, config)
    elif check.check_type == "cross_date_compare":
        return _handle_cross_date_compare(conn, check, config)
    elif check.check_type == "length_excess":
        return _handle_length_excess(conn, check, config)
    elif check.check_type == "column_mismatch":
        return _handle_column_mismatch(conn, check, config)
    else:
        raise ValueError(f"unknown check_type: {check.check_type}")


def _handle_referential_integrity(
    conn: duckdb.DuckDBPyConnection,
    check: CrossTableCheckDef,
    config: QAConfig,
) -> StepResult:
    """Check 201: Referential integrity (PatID in source but not in reference).

    Args:
        conn: DuckDB connection
        check: Check definition (must have source_table, reference_table, source_column, reference_column)
        config: QAConfig for max_failing_rows

    Returns:
        StepResult with n_failed = count of missing references
    """
    source = check.source_table
    reference = check.reference_table
    source_col = check.source_column
    reference_col = check.reference_column

    # Count missing
    count_query = f"""
        SELECT COUNT(DISTINCT s."{source_col}") AS n_missing
        FROM "{source}" s
        WHERE s."{source_col}" IS NOT NULL
          AND s."{source_col}" NOT IN (
            SELECT "{reference_col}" FROM "{reference}"
            WHERE "{reference_col}" IS NOT NULL
        )
    """
    n_missing = conn.execute(count_query).fetchone()[0] or 0

    # Total count
    total_query = f'SELECT COUNT(DISTINCT "{source_col}") FROM "{source}" WHERE "{source_col}" IS NOT NULL'
    n_total = conn.execute(total_query).fetchone()[0] or 0

    # Sample of failing rows
    failing_query = f"""
        SELECT s.*
        FROM "{source}" s
        WHERE s."{source_col}" IS NOT NULL
          AND s."{source_col}" NOT IN (
            SELECT "{reference_col}" FROM "{reference}"
            WHERE "{reference_col}" IS NOT NULL
        )
        LIMIT {config.max_failing_rows}
    """
    failing_df = conn.execute(failing_query).pl()

    n_passed = n_total - n_missing if n_total > n_missing else 0

    return StepResult(
        step_index=-1,
        assertion_type="cross_table",
        column=source_col or "",
        description=check.description,
        n_passed=n_passed,
        n_failed=n_missing,
        failing_rows=failing_df if failing_df.height > 0 else None,
        check_id=check.check_id,
        severity=check.severity,
    )


def _handle_length_consistency(
    conn: duckdb.DuckDBPyConnection,
    check: CrossTableCheckDef,
    config: QAConfig,
) -> StepResult:
    """Check 203: Length consistency for same column across multiple tables.

    Args:
        conn: DuckDB connection
        check: Check definition (must have source_column and table_group)
        config: QAConfig

    Returns:
        StepResult (n_failed > 0 if max lengths differ across tables)
    """
    if not check.table_group or not check.source_column:
        raise ValueError("length_consistency check requires source_column and table_group")

    col = check.source_column
    tables = check.table_group

    # Build union query to get max length per table
    union_parts = []
    for table in tables:
        union_parts.append(
            f"""
            SELECT
                '{col}' AS column_name,
                '{table}' AS table_key,
                MAX(LENGTH(CAST("{col}" AS VARCHAR))) AS max_len
            FROM "{table}"
            """
        )

    union_query = " UNION ALL ".join(union_parts)
    results_df = conn.execute(union_query).pl()

    # Check if all max_len values are the same
    if results_df.height == 0:
        return StepResult(
            step_index=-1,
            assertion_type="cross_table",
            column=col,
            description=check.description,
            n_passed=0,
            n_failed=0,
            failing_rows=None,
            check_id=check.check_id,
            severity=check.severity,
        )

    max_lens = results_df["max_len"].unique().to_list()
    n_failed = 0 if len(max_lens) <= 1 else len(max_lens)

    return StepResult(
        step_index=-1,
        assertion_type="cross_table",
        column=col,
        description=check.description,
        n_passed=0 if n_failed > 0 else 1,
        n_failed=n_failed,
        failing_rows=results_df if n_failed > 0 else None,
        check_id=check.check_id,
        severity=check.severity,
    )


def _handle_cross_date_compare(
    conn: duckdb.DuckDBPyConnection,
    check: CrossTableCheckDef,
    config: QAConfig,
) -> StepResult:
    """Checks 205, 206, 227: Cross-table date comparison (joined on PatID).

    Args:
        conn: DuckDB connection
        check: Check definition (must have source_table, reference_table, source_column, reference_column, target_column)
        config: QAConfig

    Returns:
        StepResult with n_failed = count of violations
    """
    source = check.source_table
    reference = check.reference_table
    source_col = check.source_column
    reference_col = check.reference_column
    target_col = check.target_column

    # Count violations: target_col < Birth_Date
    count_query = f"""
        SELECT COUNT(*) AS n_violations
        FROM "{source}" s
        JOIN "{reference}" r ON s."{source_col}" = r."{reference_col}"
        WHERE s."{target_col}" IS NOT NULL
          AND r."Birth_Date" IS NOT NULL
          AND s."{target_col}" < r."Birth_Date"
    """
    n_violations = conn.execute(count_query).fetchone()[0] or 0

    # Total count
    total_query = f"""
        SELECT COUNT(*) AS n_total
        FROM "{source}" s
        JOIN "{reference}" r ON s."{source_col}" = r."{reference_col}"
        WHERE s."{target_col}" IS NOT NULL
          AND r."Birth_Date" IS NOT NULL
    """
    n_total = conn.execute(total_query).fetchone()[0] or 0

    # Sample failing rows
    failing_query = f"""
        SELECT s.*
        FROM "{source}" s
        JOIN "{reference}" r ON s."{source_col}" = r."{reference_col}"
        WHERE s."{target_col}" IS NOT NULL
          AND r."Birth_Date" IS NOT NULL
          AND s."{target_col}" < r."Birth_Date"
        LIMIT {config.max_failing_rows}
    """
    failing_df = conn.execute(failing_query).pl()

    n_passed = n_total - n_violations if n_total > n_violations else 0

    return StepResult(
        step_index=-1,
        assertion_type="cross_table",
        column=target_col or "",
        description=check.description,
        n_passed=n_passed,
        n_failed=n_violations,
        failing_rows=failing_df if failing_df.height > 0 else None,
        check_id=check.check_id,
        severity=check.severity,
    )


def _handle_length_excess(
    conn: duckdb.DuckDBPyConnection,
    check: CrossTableCheckDef,
    config: QAConfig,
) -> StepResult:
    """Check 209: Actual max column length much smaller than declared schema length.

    Args:
        conn: DuckDB connection
        check: Check definition (must have source_table and source_column)
        config: QAConfig

    Returns:
        StepResult (n_failed > 0 if actual_max < declared_length * 0.5)
    """
    source = check.source_table
    col = check.source_column

    # Get actual max length
    actual_query = f"""
        SELECT MAX(LENGTH(CAST("{col}" AS VARCHAR))) AS actual_max
        FROM "{source}"
        WHERE "{col}" IS NOT NULL
    """
    actual_max = conn.execute(actual_query).fetchone()[0] or 0

    # Get declared length from schema
    try:
        schema = get_schema(source)
        col_def = schema.get_column(col)
    except KeyError as e:
        log.warning(f"could not find schema for table {source}: {e}")
        return StepResult(
            step_index=-1,
            assertion_type="cross_table",
            column=col,
            description=check.description,
            n_passed=1,
            n_failed=0,
            failing_rows=None,
            check_id=check.check_id,
            severity=check.severity,
        )

    if not col_def or col_def.length is None:
        # No length info available, pass
        return StepResult(
            step_index=-1,
            assertion_type="cross_table",
            column=col,
            description=check.description,
            n_passed=1,
            n_failed=0,
            failing_rows=None,
            check_id=check.check_id,
            severity=check.severity,
        )

    declared_length = col_def.length

    # Check if actual < declared * 0.5
    threshold = declared_length * 0.5
    n_failed = 1 if actual_max > 0 and actual_max < threshold else 0

    result_data = {
        "column": [col],
        "declared_length": [declared_length],
        "actual_max": [actual_max],
        "threshold": [int(threshold)],
    }
    failing_rows = pl.DataFrame(result_data) if n_failed > 0 else None

    return StepResult(
        step_index=-1,
        assertion_type="cross_table",
        column=col,
        description=check.description,
        n_passed=1 - n_failed,
        n_failed=n_failed,
        failing_rows=failing_rows,
        check_id=check.check_id,
        severity=check.severity,
    )


def _handle_column_mismatch(
    conn: duckdb.DuckDBPyConnection,
    check: CrossTableCheckDef,
    config: QAConfig,
) -> StepResult:
    """Check 224: Column mismatch (Hispanic != ImputedHispanic when both non-null).

    Args:
        conn: DuckDB connection
        check: Check definition (must have source_table, column_a, column_b)
        config: QAConfig

    Returns:
        StepResult with n_failed = count of mismatches
    """
    source = check.source_table
    col_a = check.column_a
    col_b = check.column_b

    # Count mismatches
    count_query = f"""
        SELECT COUNT(*) AS n_mismatches
        FROM "{source}"
        WHERE "{col_a}" IS NOT NULL
          AND "{col_b}" IS NOT NULL
          AND "{col_a}" != "{col_b}"
    """
    n_mismatches = conn.execute(count_query).fetchone()[0] or 0

    # Total count with both non-null
    total_query = f"""
        SELECT COUNT(*) AS n_total
        FROM "{source}"
        WHERE "{col_a}" IS NOT NULL
          AND "{col_b}" IS NOT NULL
    """
    n_total = conn.execute(total_query).fetchone()[0] or 0

    # Sample failing rows
    failing_query = f"""
        SELECT *
        FROM "{source}"
        WHERE "{col_a}" IS NOT NULL
          AND "{col_b}" IS NOT NULL
          AND "{col_a}" != "{col_b}"
        LIMIT {config.max_failing_rows}
    """
    failing_df = conn.execute(failing_query).pl()

    n_passed = n_total - n_mismatches if n_total > n_mismatches else 0

    return StepResult(
        step_index=-1,
        assertion_type="cross_table",
        column=f"{col_a}, {col_b}",
        description=check.description,
        n_passed=n_passed,
        n_failed=n_mismatches,
        failing_rows=failing_df if failing_df.height > 0 else None,
        check_id=check.check_id,
        severity=check.severity,
    )

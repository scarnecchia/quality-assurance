"""Cross-table validation orchestrator using DuckDB SQL engine."""

# pattern: Imperative Shell

from __future__ import annotations

import structlog
import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa

from scdm_qa.config import QAConfig
from scdm_qa.schemas import get_schema
from scdm_qa.schemas.models import CrossTableCheckDef, TableSchema
from scdm_qa.validation.duckdb_utils import create_connection
from scdm_qa.validation.results import StepResult

log = structlog.get_logger(__name__)

_SCDM_TYPE_MAP: dict[str, pa.DataType] = {
    "Numeric": pa.float64(),
    "Character": pa.utf8(),
}


def build_arrow_schema(
    table_schema: TableSchema,
    *,
    data_columns: tuple[str, ...] | None = None,
) -> pa.Schema:
    """Build a canonical pyarrow.Schema from an SCDM TableSchema.

    Args:
        table_schema: SCDM table definition with column types and nullability.
        data_columns: If provided, only include spec columns present in this
            sequence, ordered to match data column order. Columns in data but
            not in spec are excluded (caller merges inferred types separately).

    Returns:
        pyarrow.Schema with canonical types for SCDM columns.

    Raises:
        ValueError: If a ColumnDef has an unrecognised col_type.
    """
    col_lookup = {c.name: c for c in table_schema.columns}

    if data_columns is not None:
        ordered_names = [n for n in data_columns if n in col_lookup]
    else:
        ordered_names = [c.name for c in table_schema.columns]

    fields: list[pa.Field] = []
    for name in ordered_names:
        col_def = col_lookup[name]
        arrow_type = _SCDM_TYPE_MAP.get(col_def.col_type)
        if arrow_type is None:
            raise ValueError(
                f"unrecognised SCDM col_type {col_def.col_type!r} "
                f"for column {col_def.name!r}"
            )
        fields.append(pa.field(name, arrow_type, nullable=col_def.missing_allowed))

    return pa.schema(fields)


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
        conn = create_connection(
            memory_limit=config.duckdb_memory_limit,
            threads=config.duckdb_threads,
            temp_directory=config.duckdb_temp_directory,
        )

        # Register all tables as views
        for table_key, file_path in config.tables.items():
            try:
                if file_path.suffix.lower() == ".sas7bdat":
                    temp_path = _convert_sas_to_parquet(
                        file_path, chunk_size=config.chunk_size, table_key=table_key
                    )
                    temp_parquet_files.append(temp_path)
                    safe_path = str(temp_path).replace("'", "''")
                else:
                    safe_path = str(file_path).replace("'", "''")

                conn.execute(f'CREATE VIEW "{table_key}" AS SELECT * FROM read_parquet(\'{safe_path}\')')
                registered_views.add(table_key)
                log.info("registered view", table_key=table_key, path=str(file_path))
            except Exception as e:
                log.warning("failed to register table", table_key=table_key, error=str(e))

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
                log.warning("skipping check: source table not in config", check_id=check.check_id, table=check.source_table)
                continue
            if check.reference_table and check.reference_table not in registered_views:
                log.warning("skipping check: reference table not in config", check_id=check.check_id, table=check.reference_table)
                continue

            try:
                result = _run_check(conn, check, config)
                results.append(result)
            except duckdb.Error as e:
                log.error("check failed with DuckDB error", check_id=check.check_id, error=str(e))
                results.append(
                    StepResult(
                        step_index=-1,
                        assertion_type="cross_table",
                        column=check.join_column or "",
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
                log.warning("failed to close DuckDB connection", error=str(e))

        # Clean up temp parquet files
        for tmp_path in temp_parquet_files:
            try:
                tmp_path.unlink()
            except Exception as e:
                log.warning("failed to delete temp parquet file", path=str(tmp_path), error=str(e))

    return results


def _build_write_schema(
    canonical_schema: TableSchema | None,
    data_schema: pa.Schema,
) -> pa.Schema:
    """Build a write schema merging canonical SCDM types with inferred types.

    Canonical types are used for columns defined in the SCDM spec. Columns
    present in data but not in spec keep their inferred types. Column order
    follows the data schema.

    Args:
        canonical_schema: SCDM TableSchema, or None for unknown tables.
        data_schema: Schema inferred from the first data chunk.

    Returns:
        Merged pyarrow.Schema with canonical types where available.
    """
    if canonical_schema is None:
        return data_schema

    data_col_names = tuple(data_schema.names)
    canonical = build_arrow_schema(canonical_schema, data_columns=data_col_names)
    canonical_lookup = {f.name: f for f in canonical}

    merged_fields: list[pa.Field] = []
    for i, name in enumerate(data_col_names):
        if name in canonical_lookup:
            merged_fields.append(canonical_lookup[name])
        else:
            merged_fields.append(data_schema.field(i))

    return pa.schema(merged_fields)


def _convert_sas_to_parquet(
    sas_path: Path,
    chunk_size: int = 500_000,
    *,
    table_key: str,
) -> Path:
    """Convert SAS7BDAT to temp Parquet via streaming writes.

    Each chunk is cast to a canonical schema (from the SCDM spec) and written
    as a separate Parquet row group, keeping memory bounded to one chunk.

    Args:
        sas_path: Path to .sas7bdat file.
        chunk_size: Chunk size for reading.
        table_key: SCDM table key for canonical schema lookup.

    Returns:
        Path to temporary Parquet file.
    """
    import pyarrow.parquet as pq

    from scdm_qa.readers import create_reader

    reader = create_reader(sas_path, chunk_size=chunk_size)

    # Resolve canonical schema (None if table_key unknown)
    canonical_schema: TableSchema | None = None
    try:
        canonical_schema = get_schema(table_key)
    except KeyError:
        log.warning(
            "no SCDM spec for table; schema will be inferred from data",
            table_key=table_key,
        )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    writer: pq.ParquetWriter | None = None
    write_schema: pa.Schema | None = None
    total_rows = 0

    try:
        for chunk_df in reader.chunks():
            arrow_table = chunk_df.to_arrow()

            if write_schema is None:
                write_schema = _build_write_schema(
                    canonical_schema, arrow_table.schema
                )
                writer = pq.ParquetWriter(str(tmp_path), write_schema)

            arrow_table = arrow_table.cast(write_schema)
            writer.write_table(arrow_table)
            total_rows += arrow_table.num_rows

        # Handle zero-chunk case
        if writer is None:
            if canonical_schema is not None:
                write_schema = build_arrow_schema(canonical_schema)
            else:
                write_schema = pa.schema([])
            writer = pq.ParquetWriter(str(tmp_path), write_schema)
    finally:
        if writer is not None:
            writer.close()

    log.info(
        "converted SAS file to temp parquet (streaming)",
        sas_path=str(sas_path),
        tmp_path=str(tmp_path),
        n_rows=total_rows,
        n_columns=len(write_schema) if write_schema else 0,
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
        check: Check definition (must have source_table, reference_table, join_column, join_reference_column)
        config: QAConfig for max_failing_rows

    Returns:
        StepResult with n_failed = count of missing references
    """
    source = check.source_table
    reference = check.reference_table
    source_col = check.join_column
    reference_col = check.join_reference_column

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
        check: Check definition (must have join_column and table_group)
        config: QAConfig

    Returns:
        StepResult (n_failed > 0 if max lengths differ across tables)
    """
    if not check.table_group or not check.join_column:
        raise ValueError("length_consistency check requires join_column and table_group")

    col = check.join_column
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
    """Checks 205, 206, 227: Cross-table date comparison.

    Joins source to reference on join_column/join_reference_column, then
    checks that compare_column (source) >= compare_reference_column (reference).

    Args:
        conn: DuckDB connection
        check: Check definition with join keys and both date columns
        config: QAConfig

    Returns:
        StepResult with n_failed = count of violations
    """
    source = check.source_table
    reference = check.reference_table
    join_col = check.join_column
    join_ref_col = check.join_reference_column
    date_col = check.compare_column
    ref_date_col = check.compare_reference_column

    count_query = f"""
        SELECT COUNT(*) AS n_violations
        FROM "{source}" s
        JOIN "{reference}" r ON s."{join_col}" = r."{join_ref_col}"
        WHERE s."{date_col}" IS NOT NULL
          AND r."{ref_date_col}" IS NOT NULL
          AND s."{date_col}" < r."{ref_date_col}"
    """
    n_violations = conn.execute(count_query).fetchone()[0] or 0

    total_query = f"""
        SELECT COUNT(*) AS n_total
        FROM "{source}" s
        JOIN "{reference}" r ON s."{join_col}" = r."{join_ref_col}"
        WHERE s."{date_col}" IS NOT NULL
          AND r."{ref_date_col}" IS NOT NULL
    """
    n_total = conn.execute(total_query).fetchone()[0] or 0

    failing_query = f"""
        SELECT s.*
        FROM "{source}" s
        JOIN "{reference}" r ON s."{join_col}" = r."{join_ref_col}"
        WHERE s."{date_col}" IS NOT NULL
          AND r."{ref_date_col}" IS NOT NULL
          AND s."{date_col}" < r."{ref_date_col}"
        LIMIT {config.max_failing_rows}
    """
    failing_df = conn.execute(failing_query).pl()

    n_passed = n_total - n_violations if n_total > n_violations else 0

    return StepResult(
        step_index=-1,
        assertion_type="cross_table",
        column=f"{date_col}, {ref_date_col}",
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
        check: Check definition (must have source_table and join_column)
        config: QAConfig

    Returns:
        StepResult (n_failed > 0 if actual_max < declared_length * 0.5)
    """
    source = check.source_table
    col = check.join_column

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
        log.warning("could not find schema for table", table=source, error=str(e))
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

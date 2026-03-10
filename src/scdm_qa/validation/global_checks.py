from __future__ import annotations

from pathlib import Path
from typing import Iterator, TypedDict

import polars as pl
import structlog

from scdm_qa.schemas.checks import get_date_ordering_checks_for_table, get_not_populated_checks_for_table
from scdm_qa.schemas.models import TableSchema
from scdm_qa.validation.results import StepResult

log = structlog.get_logger(__name__)


class SortViolation(TypedDict):
    chunk_boundary: str
    issue: str


def check_uniqueness(
    file_path: Path,
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame] | None = None,
    *,
    max_failing_rows: int = 500,
) -> StepResult | None:
    if not schema.unique_row:
        return None

    key_cols = list(schema.unique_row)
    description = f"Uniqueness on ({', '.join(key_cols)})"

    if file_path.suffix.lower() == ".parquet":
        result = _uniqueness_duckdb(file_path, key_cols, description, max_failing_rows)
        if result is not None:
            return result
        log.info("duckdb not available, falling back to in-memory uniqueness check")

    return _uniqueness_in_memory(key_cols, description, chunks, max_failing_rows)


def _uniqueness_duckdb(
    file_path: Path,
    key_cols: list[str],
    description: str,
    max_failing_rows: int,
) -> StepResult | None:
    try:
        import duckdb
    except ImportError:
        return None

    safe_path = str(file_path).replace("'", "''")
    cols_sql = ", ".join(f'"{c}"' for c in key_cols)
    query = f"""
        SELECT {cols_sql}, COUNT(*) AS _dup_count
        FROM read_parquet('{safe_path}')
        GROUP BY {cols_sql}
        HAVING COUNT(*) > 1
        LIMIT {max_failing_rows}
    """
    total_query = f"SELECT COUNT(*) FROM read_parquet('{safe_path}')"

    dup_rows_query = f"""
        SELECT SUM(_dup_count) FROM (
            SELECT COUNT(*) AS _dup_count
            FROM read_parquet('{safe_path}')
            GROUP BY {cols_sql}
            HAVING COUNT(*) > 1
        )
    """

    conn = duckdb.connect()
    try:
        try:
            total_rows = conn.execute(total_query).fetchone()[0]
            dup_row_total = conn.execute(dup_rows_query).fetchone()[0] or 0
            failing_df = conn.execute(query).pl()
        except Exception as e:
            log.warning("duckdb execution failed", error=str(e))
            return None
    finally:
        conn.close()

    n_failed = dup_row_total
    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    log.info(
        "uniqueness check via duckdb",
        key_cols=key_cols,
        total_rows=total_rows,
        duplicate_rows=dup_row_total,
    )

    return StepResult(
        step_index=-1,  # will be renumbered when appended
        assertion_type="rows_distinct",
        column=", ".join(key_cols),
        description=description,
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=failing_df if failing_df.height > 0 else None,
        check_id=None,
        severity=None,
    )


def _uniqueness_in_memory(
    key_cols: list[str],
    description: str,
    chunks: Iterator[pl.DataFrame] | None,
    max_failing_rows: int,
) -> StepResult | None:
    if chunks is None:
        log.warning("no chunks provided for in-memory uniqueness check")
        return None

    all_keys: list[pl.DataFrame] = []
    total_rows = 0

    for chunk in chunks:
        present_cols = [c for c in key_cols if c in chunk.columns]
        if len(present_cols) != len(key_cols):
            continue
        all_keys.append(chunk.select(present_cols))
        total_rows += chunk.height

    if not all_keys:
        return None

    combined = pl.concat(all_keys)
    duplicates = (
        combined.group_by(key_cols)
        .agg(pl.len().alias("_count"))
        .filter(pl.col("_count") > 1)
    )

    # n_failed = total duplicate rows (not groups)
    n_failed = duplicates["_count"].sum() if duplicates.height > 0 else 0
    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    failing_rows = duplicates.head(max_failing_rows) if duplicates.height > 0 else None

    return StepResult(
        step_index=-1,
        assertion_type="rows_distinct",
        column=", ".join(key_cols),
        description=description,
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=failing_rows,
        check_id=None,
        severity=None,
    )


def check_sort_order(
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame],
) -> StepResult | None:
    if not schema.sort_order:
        return None

    sort_cols = list(schema.sort_order)
    description = f"Sort order on ({', '.join(sort_cols)})"

    prev_last_row: pl.DataFrame | None = None
    violations: list[SortViolation] = []
    total_rows = 0
    chunk_num = 0

    for chunk in chunks:
        chunk_num += 1
        present_cols = [c for c in sort_cols if c in chunk.columns]
        if len(present_cols) != len(sort_cols):
            continue

        total_rows += chunk.height

        if prev_last_row is not None:
            first_row = chunk.select(present_cols).head(1)
            if not _is_sorted_boundary(prev_last_row, first_row, present_cols):
                violations.append({
                    "chunk_boundary": f"{chunk_num - 1}-{chunk_num}",
                    "issue": "sort order break at chunk boundary",
                })

        prev_last_row = chunk.select(present_cols).tail(1)

    n_failed = len(violations)
    n_passed = max(0, chunk_num - 1 - n_failed) if chunk_num > 1 else 0

    failing_rows = None
    if violations:
        failing_rows = pl.DataFrame(violations)

    return StepResult(
        step_index=-1,
        assertion_type="sort_order",
        column=", ".join(sort_cols),
        description=description,
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=failing_rows,
        check_id=None,
        severity=None,
    )


def _is_sorted_boundary(
    last_row: pl.DataFrame,
    first_row: pl.DataFrame,
    sort_cols: list[str],
) -> bool:
    for col in sort_cols:
        last_val = last_row[col][0]
        first_val = first_row[col][0]
        if last_val is None or first_val is None:
            continue
        if last_val < first_val:
            return True
        if last_val > first_val:
            return False
    return True  # equal is OK


def check_not_populated(
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame],
) -> list[StepResult]:
    """Check 111: Detect columns that are entirely null across all chunks.

    Returns one StepResult per target column defined in the check registry.
    """
    check_defs = get_not_populated_checks_for_table(schema.table_key)
    if not check_defs:
        return []

    target_columns = [c.column for c in check_defs]
    non_null_counts: dict[str, int] = {col: 0 for col in target_columns}
    total_rows = 0

    for chunk in chunks:
        total_rows += chunk.height
        for col_name in target_columns:
            if col_name in chunk.columns:
                non_null_counts[col_name] += chunk[col_name].drop_nulls().len()

    results: list[StepResult] = []
    for check_def in check_defs:
        count = non_null_counts.get(check_def.column, 0)
        if count == 0:
            # Column is entirely null — not populated
            n_failed = total_rows
            n_passed = 0
        else:
            n_failed = 0
            n_passed = total_rows

        results.append(
            StepResult(
                step_index=-1,
                assertion_type="not_populated",
                column=check_def.column,
                description=f"{check_def.column} populated (check {check_def.check_id})",
                n_passed=n_passed,
                n_failed=n_failed,
                failing_rows=None,
                check_id=check_def.check_id,
                severity=check_def.severity,
            )
        )

    return results


def check_date_ordering(
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame],
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    """Check 226: Detect rows where date_a > date_b.

    Rows where either date is null are skipped (not flagged).
    Returns one StepResult per configured date pair.
    """
    ordering_defs = get_date_ordering_checks_for_table(schema.table_key)
    if not ordering_defs:
        return []

    # Accumulate per-pair counts across chunks
    pair_failed: dict[str, int] = {}
    pair_passed: dict[str, int] = {}
    pair_failing_rows: dict[str, list[pl.DataFrame]] = {}
    pair_failing_count: dict[str, int] = {}
    total_rows = 0

    for pair_def in ordering_defs:
        key = f"{pair_def.date_a}>{pair_def.date_b}"
        pair_failed[key] = 0
        pair_passed[key] = 0
        pair_failing_rows[key] = []
        pair_failing_count[key] = 0

    for chunk in chunks:
        total_rows += chunk.height
        for pair_def in ordering_defs:
            if pair_def.date_a not in chunk.columns or pair_def.date_b not in chunk.columns:
                continue

            key = f"{pair_def.date_a}>{pair_def.date_b}"

            # Filter to rows where both dates are non-null
            both_present = chunk.filter(
                pl.col(pair_def.date_a).is_not_null() & pl.col(pair_def.date_b).is_not_null()
            )

            violations = both_present.filter(
                pl.col(pair_def.date_a) > pl.col(pair_def.date_b)
            )

            pair_failed[key] += violations.height
            pair_passed[key] += both_present.height - violations.height

            if violations.height > 0 and pair_failing_count[key] < max_failing_rows:
                remaining = max_failing_rows - pair_failing_count[key]
                sample = violations.head(remaining)
                pair_failing_rows[key].append(sample)
                pair_failing_count[key] += sample.height

    results: list[StepResult] = []
    for pair_def in ordering_defs:
        key = f"{pair_def.date_a}>{pair_def.date_b}"
        failing = None
        if pair_failing_rows[key]:
            failing = pl.concat(pair_failing_rows[key])

        results.append(
            StepResult(
                step_index=-1,
                assertion_type="date_ordering",
                column=f"{pair_def.date_a}, {pair_def.date_b}",
                description=f"{pair_def.description} (check {pair_def.check_id})",
                n_passed=pair_passed[key],
                n_failed=pair_failed[key],
                failing_rows=failing,
                check_id=pair_def.check_id,
                severity=pair_def.severity,
            )
        )

    return results

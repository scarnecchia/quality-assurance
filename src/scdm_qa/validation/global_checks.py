from __future__ import annotations

from pathlib import Path
from typing import Iterator, TypedDict

import polars as pl
import structlog

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

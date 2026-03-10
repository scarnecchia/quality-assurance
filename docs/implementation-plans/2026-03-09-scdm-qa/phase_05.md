# SCDM-QA Implementation Plan — Phase 5: Global Validation

**Goal:** Uniqueness and sort order checks that require seeing the full dataset, with DuckDB as optional fast path and in-memory fallback.

**Architecture:** Global checks run after per-chunk validation. For Parquet files with DuckDB available, uniqueness uses SQL `GROUP BY ... HAVING COUNT(*) > 1` directly against the file. Without DuckDB, uniqueness is checked via accumulated key sets during chunk iteration. Sort order is verified by checking cross-chunk boundaries — the last row of chunk N must sort before the first row of chunk N+1. Results are appended to the existing `ValidationResult`.

**Tech Stack:** Python >=3.12, DuckDB (optional), polars 1.38.x

**Scope:** 8 phases from original design (phase 5 of 8)

**Codebase verified:** 2026-03-09

---

## Acceptance Criteria Coverage

This phase implements and tests:

### scdm-qa.AC2: Validation rules cover the full SCDM spec
- **scdm-qa.AC2.4 Success:** Duplicate rows on unique key columns (e.g., PatID in Demographic) produce validation warnings

### scdm-qa.AC5: Handles TB-scale data
- **scdm-qa.AC5.3 Success:** DuckDB used for global checks on Parquet when installed; graceful fallback when not

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Create global checks module

**Files:**
- Create: `src/scdm_qa/validation/global_checks.py`

**Step 1: Create the file**

```python
from __future__ import annotations

from pathlib import Path
from typing import Iterator

import polars as pl
import structlog

from scdm_qa.schemas.models import TableSchema
from scdm_qa.validation.results import StepResult

log = structlog.get_logger(__name__)


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

    cols_sql = ", ".join(f'"{c}"' for c in key_cols)
    query = f"""
        SELECT {cols_sql}, COUNT(*) AS _dup_count
        FROM read_parquet('{file_path}')
        GROUP BY {cols_sql}
        HAVING COUNT(*) > 1
        LIMIT {max_failing_rows}
    """
    total_query = f"SELECT COUNT(*) FROM read_parquet('{file_path}')"

    dup_rows_query = f"""
        SELECT SUM(_dup_count) FROM (
            SELECT COUNT(*) AS _dup_count
            FROM read_parquet('{file_path}')
            GROUP BY {cols_sql}
            HAVING COUNT(*) > 1
        )
    """

    conn = duckdb.connect()
    try:
        total_rows = conn.execute(total_query).fetchone()[0]
        dup_row_total = conn.execute(dup_rows_query).fetchone()[0] or 0
        failing_df = conn.execute(query).pl()
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
    violations: list[dict] = []
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
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.validation.global_checks import check_uniqueness, check_sort_order; print('global checks imported OK')"`
Expected: `global checks imported OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/validation/global_checks.py
git commit -m "feat: add global uniqueness and sort order checks with DuckDB fast path"
```
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update validation __init__.py exports

**Files:**
- Modify: `src/scdm_qa/validation/__init__.py`

**Step 1: Update the file**

Add global check exports:

```python
from scdm_qa.validation.global_checks import check_sort_order, check_uniqueness
from scdm_qa.validation.results import StepResult, ValidationResult

__all__ = [
    "StepResult",
    "ValidationResult",
    "check_sort_order",
    "check_uniqueness",
]
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.validation import check_uniqueness, check_sort_order; print('exports OK')"`
Expected: `exports OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/validation/__init__.py
git commit -m "chore: export global check functions from validation package"
```
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Test global validation checks

**Verifies:** scdm-qa.AC2.4, scdm-qa.AC5.3

**Files:**
- Create: `tests/test_global_checks.py`

**Implementation:**

Tests verify uniqueness detection with both in-memory and DuckDB paths, sort order boundary detection, and graceful handling of tables without uniqueness/sort constraints.

**Testing:**
- scdm-qa.AC2.4: Duplicate composite keys detected (in-memory path)
- scdm-qa.AC2.4: Duplicate keys detected via DuckDB (if installed)
- scdm-qa.AC5.3: DuckDB used when available; test confirms DuckDB import works or gracefully falls back
- Sort order: break at chunk boundary detected
- Tables with empty unique_row return None
- Tables with empty sort_order return None

```python
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from scdm_qa.schemas import get_schema
from scdm_qa.validation.global_checks import check_sort_order, check_uniqueness


class TestUniquenessInMemory:
    def test_detects_duplicate_keys(self) -> None:
        schema = get_schema("demographic")
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
            pl.DataFrame({"PatID": ["P2", "P3"], "Birth_Date": [3, 4], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])

        result = check_uniqueness(
            Path("dummy.sas7bdat"),  # non-parquet forces in-memory path
            schema,
            chunks=chunks,
        )
        assert result is not None
        # P2 appears twice → 2 duplicate rows
        assert result.n_failed == 2

    def test_no_duplicates_passes(self) -> None:
        schema = get_schema("demographic")
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
            pl.DataFrame({"PatID": ["P3", "P4"], "Birth_Date": [3, 4], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])
        result = check_uniqueness(Path("dummy.sas7bdat"), schema, chunks=chunks)
        assert result is not None
        assert result.n_failed == 0

    def test_returns_none_for_table_without_unique_row(self) -> None:
        schema = get_schema("vital_signs")
        result = check_uniqueness(Path("dummy.parquet"), schema)
        assert result is None


class TestUniquenessDuckDB:
    def test_detects_duplicates_via_duckdb(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        df = pl.DataFrame({"PatID": ["P1", "P1", "P3"]})
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        schema = get_schema("demographic")
        result = check_uniqueness(path, schema)
        assert result is not None
        assert result.n_failed > 0


class TestSortOrder:
    def test_detects_sort_break_at_boundary(self) -> None:
        schema = get_schema("demographic")
        # Chunk 1 ends with P3, chunk 2 starts with P1 — sort order break
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P3"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [3, 4], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])
        result = check_sort_order(schema, chunks)
        assert result is not None
        assert result.n_failed > 0

    def test_correctly_sorted_passes(self) -> None:
        schema = get_schema("demographic")
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
            pl.DataFrame({"PatID": ["P3", "P4"], "Birth_Date": [3, 4], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])
        result = check_sort_order(schema, chunks)
        assert result is not None
        assert result.n_failed == 0
```

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: All tests pass (DuckDB test skipped if not installed).

**Commit:** `test: add global uniqueness and sort order check tests`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

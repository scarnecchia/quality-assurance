# L1 & L2 Validation Checks Implementation Plan — Phase 6

**Goal:** Implement L2 enrollment checks 215 (overlapping enrollment spans) and 216 (consecutive non-bridged enrollment gaps).

**Architecture:** Two new functions in `global_checks.py`: `check_overlapping_spans()` and `check_enrollment_gaps()`. Both collect (PatID, Enr_Start, Enr_End) across chunks, sort by PatID + Enr_Start, then detect overlaps (215) or gaps (216). Check 215 has a DuckDB fast path for Parquet files (self-join SQL) with polars fallback, matching the existing `_uniqueness_duckdb()` pattern.

**Tech Stack:** Python 3.12+, polars, duckdb (optional), pytest

**Scope:** 8 phases from original design (phase 6 of 8)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### l1-l2-checks.AC2: L2 checks detect cross-record data quality issues
- **l1-l2-checks.AC2.2 Success:** Check 215 flags overlapping enrollment spans within the same PatID
- **l1-l2-checks.AC2.8 Edge:** Check 216 flags enrollment gaps but adjacent spans (Enr_End + 1 day = next Enr_Start) pass

### l1-l2-checks.AC4: Severity levels match SAS reference
- **l1-l2-checks.AC4.1 Success:** Checks marked Fail in SAS reference produce error-level results
- **l1-l2-checks.AC4.3 Success:** Checks marked Warn in SAS reference produce warning-level results

---

<!-- START_SUBCOMPONENT_A (tasks 1-4) -->

<!-- START_TASK_1 -->
### Task 1: Implement check_overlapping_spans() in global_checks.py

**Verifies:** l1-l2-checks.AC2.2

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py`

**Implementation:**

Add new function. The SCDM spec defines Enr_Start/Enr_End as "Numeric" (SAS date integers), but Parquet files may store them as either integers or `pl.Date` depending on the ETL pipeline. The implementation must handle both:
- For `pl.Date` columns, use `polars.duration(days=1)` for gap arithmetic
- For integer columns, use `+ 1` for gap arithmetic

The overlap check (`Enr_Start < prev_Enr_End`) works with both dtypes since `<` is valid for dates and integers alike. The gap check needs dtype-aware addition. Cast to `pl.Date` at the start if the column is integer, to normalise handling.

```python
def check_overlapping_spans(
    file_path: Path,
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame] | None = None,
    *,
    max_failing_rows: int = 500,
) -> StepResult | None:
    """Check 215: Detect overlapping enrollment spans within the same patient.

    For each patient, sorts spans by Enr_Start and checks if any span's
    Enr_Start is strictly less than the previous span's Enr_End.
    """
    if schema.table_key != "enrollment":
        return None

    # DuckDB fast path for Parquet files
    if file_path.suffix.lower() == ".parquet":
        result = _overlapping_spans_duckdb(file_path, max_failing_rows)
        if result is not None:
            return result
        log.info("duckdb not available, falling back to in-memory overlap check")

    return _overlapping_spans_in_memory(chunks, max_failing_rows)


def _overlapping_spans_duckdb(
    file_path: Path,
    max_failing_rows: int,
) -> StepResult | None:
    try:
        import duckdb
    except ImportError:
        return None

    safe_path = str(file_path).replace("'", "''")
    # Self-join: find pairs where same patient has overlapping spans
    overlap_query = f"""
        WITH spans AS (
            SELECT "PatID", "Enr_Start", "Enr_End",
                   LAG("Enr_End") OVER (PARTITION BY "PatID" ORDER BY "Enr_Start") AS prev_end
            FROM read_parquet('{safe_path}')
        )
        SELECT "PatID", "Enr_Start", "Enr_End", prev_end
        FROM spans
        WHERE "Enr_Start" < prev_end
        LIMIT {max_failing_rows}
    """
    count_query = f"""
        WITH spans AS (
            SELECT "PatID", "Enr_Start", "Enr_End",
                   LAG("Enr_End") OVER (PARTITION BY "PatID" ORDER BY "Enr_Start") AS prev_end
            FROM read_parquet('{safe_path}')
        )
        SELECT COUNT(*) FROM spans WHERE "Enr_Start" < prev_end
    """
    total_query = f"SELECT COUNT(*) FROM read_parquet('{safe_path}')"

    conn = duckdb.connect()
    try:
        try:
            total_rows = conn.execute(total_query).fetchone()[0]
            n_failed = conn.execute(count_query).fetchone()[0] or 0
            failing_df = conn.execute(overlap_query).pl()
        except Exception as e:
            log.warning("duckdb overlap check failed", error=str(e))
            return None
    finally:
        conn.close()

    n_passed = total_rows - n_failed

    return StepResult(
        step_index=-1,
        assertion_type="overlapping_spans",
        column="PatID, Enr_Start, Enr_End",
        description="No overlapping enrollment spans (check 215)",
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=failing_df if failing_df.height > 0 else None,
        check_id="215",
        severity="Fail",
    )


def _overlapping_spans_in_memory(
    chunks: Iterator[pl.DataFrame] | None,
    max_failing_rows: int,
) -> StepResult | None:
    if chunks is None:
        return None

    all_spans: list[pl.DataFrame] = []
    total_rows = 0

    for chunk in chunks:
        cols = ["PatID", "Enr_Start", "Enr_End"]
        if all(c in chunk.columns for c in cols):
            all_spans.append(chunk.select(cols))
            total_rows += chunk.height

    if not all_spans:
        return None

    combined = pl.concat(all_spans).sort("PatID", "Enr_Start")

    # Detect overlaps: within each patient, Enr_Start < prev Enr_End
    with_prev = combined.with_columns(
        pl.col("Enr_End").shift(1).over("PatID").alias("prev_end")
    )
    overlaps = with_prev.filter(
        pl.col("prev_end").is_not_null() & (pl.col("Enr_Start") < pl.col("prev_end"))
    )

    n_failed = overlaps.height
    n_passed = total_rows - n_failed

    return StepResult(
        step_index=-1,
        assertion_type="overlapping_spans",
        column="PatID, Enr_Start, Enr_End",
        description="No overlapping enrollment spans (check 215)",
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=overlaps.head(max_failing_rows) if overlaps.height > 0 else None,
        check_id="215",
        severity="Fail",
    )
```

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: Existing tests pass.

**Commit:** `feat: add check_overlapping_spans for L2 check 215`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement check_enrollment_gaps() in global_checks.py

**Verifies:** l1-l2-checks.AC2.8

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py`

**Implementation:**

```python
def check_enrollment_gaps(
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame],
    *,
    max_failing_rows: int = 500,
) -> StepResult | None:
    """Check 216: Detect non-bridged enrollment gaps.

    For each patient, sorts spans by Enr_Start and checks for gaps where
    previous Enr_End + 1 day < current Enr_Start. Adjacent spans
    (Enr_End + 1 day == next Enr_Start) pass.

    Handles both integer (SAS date) and pl.Date dtypes.
    """
    if schema.table_key != "enrollment":
        return None

    all_spans: list[pl.DataFrame] = []
    total_rows = 0

    for chunk in chunks:
        cols = ["PatID", "Enr_Start", "Enr_End"]
        if all(c in chunk.columns for c in cols):
            all_spans.append(chunk.select(cols))
            total_rows += chunk.height

    if not all_spans:
        return None

    combined = pl.concat(all_spans).sort("PatID", "Enr_Start")

    with_prev = combined.with_columns(
        pl.col("Enr_End").shift(1).over("PatID").alias("prev_end")
    )

    # Gap: prev_end + 1 day < Enr_Start (more than 1 day between spans)
    # Use dtype-aware increment: duration(days=1) for Date, +1 for integers
    enr_dtype = combined["Enr_Start"].dtype
    if enr_dtype == pl.Date or enr_dtype == pl.Datetime:
        one_day = pl.duration(days=1)
    else:
        one_day = 1

    gaps = with_prev.filter(
        pl.col("prev_end").is_not_null() & ((pl.col("prev_end") + one_day) < pl.col("Enr_Start"))
    )

    n_failed = gaps.height
    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    return StepResult(
        step_index=-1,
        assertion_type="enrollment_gaps",
        column="PatID, Enr_Start, Enr_End",
        description="No non-bridged enrollment gaps (check 216)",
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=gaps.head(max_failing_rows) if gaps.height > 0 else None,
        check_id="216",
        severity="Warn",
    )
```

Key decisions:
- Adjacent means `prev_end + 1 day == next_start` (passes)
- Gap means `prev_end + 1 day < next_start` (fails)
- No DuckDB fast path for this check (simpler logic, less benefit from SQL)
- No `file_path` parameter (unlike `check_overlapping_spans` which needs it for its DuckDB fast path). Global check functions only take `file_path` when they have a DuckDB fast path — this matches the existing pattern where `check_uniqueness(file_path, ...)` has one but `check_sort_order(schema, chunks)` does not.

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: Existing tests pass.

**Commit:** `feat: add check_enrollment_gaps for L2 check 216`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Wire enrollment checks into pipeline.py

**Verifies:** l1-l2-checks.AC2.2, l1-l2-checks.AC2.8

**Files:**
- Modify: `src/scdm_qa/pipeline.py:16`

**Implementation:**

Update import:

```python
from scdm_qa.validation.global_checks import (
    check_sort_order, check_uniqueness, check_not_populated,
    check_date_ordering, check_cause_of_death,
    check_overlapping_spans, check_enrollment_gaps,
)
```

In `_process_table()`, after the cause of death block, add:

```python
    # L2 checks: enrollment overlaps and gaps (checks 215, 216)
    if schema.table_key == "enrollment":
        overlap_reader = create_reader(file_path, chunk_size=config.chunk_size)
        overlap_step = check_overlapping_spans(
            file_path, schema, overlap_reader.chunks(),
            max_failing_rows=config.max_failing_rows,
        )
        if overlap_step is not None:
            global_steps.append(overlap_step)

        gaps_reader = create_reader(file_path, chunk_size=config.chunk_size)
        gaps_step = check_enrollment_gaps(
            schema, gaps_reader.chunks(),
            max_failing_rows=config.max_failing_rows,
        )
        if gaps_step is not None:
            global_steps.append(gaps_step)
```

**Verification:**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

**Commit:** `feat: wire enrollment checks 215, 216 into pipeline`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Tests for enrollment checks

**Verifies:** l1-l2-checks.AC2.2, l1-l2-checks.AC2.8, l1-l2-checks.AC4.1, l1-l2-checks.AC4.3

**Files:**
- Modify: `tests/test_global_checks.py`

**Implementation:**

Add new test classes to `tests/test_global_checks.py`.

**Testing:**

Tests must verify each AC listed:

- **l1-l2-checks.AC2.2 (overlapping spans):** Create enrollment data with patient P1 having two overlapping spans (e.g., Enr_Start=100, Enr_End=200 and Enr_Start=150, Enr_End=300). Assert `check_overlapping_spans` returns n_failed > 0 with check_id="215".

- **l1-l2-checks.AC2.2 (non-overlapping passes):** Create non-overlapping spans. Assert n_failed == 0.

- **l1-l2-checks.AC2.8 (gap detected):** Create enrollment data with a gap (e.g., Enr_End=100, next Enr_Start=200). Assert `check_enrollment_gaps` returns n_failed > 0 with check_id="216".

- **l1-l2-checks.AC2.8 (adjacent passes):** Create enrollment data with adjacent spans (Enr_End=100, next Enr_Start=101). Assert n_failed == 0.

- **l1-l2-checks.AC4.1:** Assert check_id="215" on overlapping spans result.

- **l1-l2-checks.AC4.3:** Assert check_id="216" on enrollment gaps result (Warn severity).

- **Non-enrollment table:** Call with a non-enrollment schema. Assert None returned.

- **DuckDB fast path** (if duckdb available): Create parquet file with overlapping spans, test via `check_overlapping_spans` with file_path pointing to parquet. Use `pytest.importorskip("duckdb")` to skip if unavailable.

- **DuckDB fallback:** Mock `_overlapping_spans_duckdb` to return None, verify polars fallback works. Follow existing mock pattern in `tests/test_global_checks.py:73`.

For in-memory tests, use `iter([df])` or `iter([df1, df2])` for chunks. Use `get_schema("enrollment")` for the schema.

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: All tests pass.

Run: `uv run pytest tests/ -v`
Expected: Full suite passes.

**Commit:** `test: add tests for L2 enrollment checks 215, 216`
<!-- END_TASK_4 -->

<!-- END_SUBCOMPONENT_A -->

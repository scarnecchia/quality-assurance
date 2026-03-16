# DuckDB Global Checks Migration - Phase 3: Migrate Sort Order and Not Populated

**Goal:** Convert `check_sort_order()` and `check_not_populated()` to DuckDB SQL. The sort order check is improved from chunk-boundary-only to full row-level LAG window verification.

**Architecture:** `check_sort_order()` switches from chunk boundary comparison to a DuckDB LAG window that checks every consecutive row pair. `check_not_populated()` switches from per-chunk accumulation to a single `COUNT("{col}")` aggregation per target column.

**Tech Stack:** Python 3.12+, DuckDB, polars, pytest

**Scope:** 6 phases from original design (phase 3 of 6)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

### GH-7.AC1: All global checks execute via DuckDB SQL
- **GH-7.AC1.1 Success:** Each of the 7 check functions executes SQL against a DuckDB view and returns a valid StepResult (sort order and not populated in this phase)
- **GH-7.AC1.2 Success:** No `pl.concat()` calls remain in any global check code path (for sort order and not populated)
- **GH-7.AC1.3 Success:** Check functions accept `conn: DuckDBPyConnection` and `view_name: str` instead of chunk iterators (for sort order and not populated)

### GH-7.AC3: Sort order check is strictly more correct
- **GH-7.AC3.1 Success:** Sort order violation within a single logical chunk is detected (previously missed by boundary-only check)
- **GH-7.AC3.2 Success:** Correctly sorted file passes with zero violations
- **GH-7.AC3.3 Edge:** File with equal adjacent rows in sort columns passes (equal is not a violation)

### GH-7.AC7: Results backward compatible
- **GH-7.AC7.1 Success:** All check IDs unchanged (102, 111)
- **GH-7.AC7.2 Success:** All severities unchanged per check
- **GH-7.AC7.3 Success:** StepResult shape unchanged (same fields, same types)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Rewrite check_sort_order() to use DuckDB LAG window

**Verifies:** GH-7.AC1.1, GH-7.AC1.3, GH-7.AC3.1, GH-7.AC3.2, GH-7.AC3.3, GH-7.AC7.1, GH-7.AC7.2, GH-7.AC7.3

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py` (replace `check_sort_order`, delete `_is_sorted_boundary`)
- Modify: `src/scdm_qa/pipeline.py` (update call site)

**Implementation:**

Replace `check_sort_order()` (currently lines 200-251) with:

```python
def check_sort_order(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
) -> StepResult | None:
    if not schema.sort_order:
        return None

    sort_cols = list(schema.sort_order)
    sas_id = _TABLE_KEY_TO_SAS_ID.get(schema.table_key, schema.table_key.upper())
    description = f"{sas_id} table is not sorted by the following variables: {', '.join(sort_cols)}"
    safe_view = view_name.replace('"', '""')

    try:
        total_rows = conn.execute(
            f'SELECT COUNT(*) FROM "{safe_view}"'
        ).fetchone()[0]

        # Build LAG-based comparison for each sort column.
        # A row is a violation if any column is strictly less than the
        # previous row's value AND all higher-priority columns are equal.
        # This mirrors multi-column sort comparison.
        lag_cols = []
        for col in sort_cols:
            safe_col = col.replace('"', '""')
            lag_cols.append(
                f'LAG("{safe_col}") OVER (ORDER BY rowid) AS "_prev_{safe_col}"'
            )

        lag_select = ", ".join(lag_cols)

        # Build violation condition: for multi-column sort, a violation
        # occurs when, scanning left to right, the first column that
        # differs has decreased.
        conditions = []
        for i, col in enumerate(sort_cols):
            safe_col = col.replace('"', '""')
            # All prior columns are equal
            equal_prefix = " AND ".join(
                f'"{sort_cols[j].replace(chr(34), chr(34)+chr(34))}" = "_prev_{sort_cols[j].replace(chr(34), chr(34)+chr(34))}"'
                for j in range(i)
            )
            cond = f'"{safe_col}" < "_prev_{safe_col}"'
            if equal_prefix:
                cond = f"({equal_prefix} AND {cond})"
            conditions.append(cond)

        violation_where = " OR ".join(f"({c})" for c in conditions)

        n_failed = conn.execute(f"""
            WITH lagged AS (
                SELECT *, ROW_NUMBER() OVER () AS rowid,
                       {lag_select}
                FROM "{safe_view}"
            )
            SELECT COUNT(*) FROM lagged
            WHERE {violation_where}
        """).fetchone()[0] or 0

    except duckdb.Error as e:
        log.error("sort order check failed", error=str(e), view=view_name)
        return StepResult(
            step_index=-1,
            assertion_type="sort_order",
            column=", ".join(sort_cols),
            description=f"Sort order check error: {e}",
            n_passed=0,
            n_failed=0,
            failing_rows=None,
            check_id="102",
            severity="Fail",
        )

    # n_passed = rows that are not violations (total - failed).
    # First row can never be a violation (no predecessor).
    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    return StepResult(
        step_index=-1,
        assertion_type="sort_order",
        column=", ".join(sort_cols),
        description=description,
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=None,
        check_id="102",
        severity="Fail",
    )
```

Delete `_is_sorted_boundary()` helper (lines 254-268).

**Row ordering assumption:** The `ROW_NUMBER() OVER ()` (without ORDER BY) relies on DuckDB scanning the Parquet file in physical row order. This is the default behaviour for single-threaded DuckDB connections reading a single Parquet file. Since the pipeline creates connections via `create_connection()` which sets a thread limit, and sort order verification is inherently about physical file order, this is acceptable. The connection is configured per-table, not shared across parallel scans. If future DuckDB versions change parallelism behaviour, consider adding `PRAGMA threads=1` for this specific query as a safety measure.

Note: The sort order check produces `failing_rows=None` (same as the existing chunk-boundary check which returns a DataFrame of SortViolation dicts — this is an acceptable simplification since the check_id and counts carry the diagnostic value). The `SortViolation` TypedDict will be cleaned up in Phase 6.

Update pipeline call site:

```python
if schema.sort_order:
    sort_step = check_sort_order(conn, table_key, schema)
    if sort_step is not None:
        global_steps.append(sort_step)
```

Remove `sort_reader = create_reader(...)`.

**Testing:**

Tests must verify:
- GH-7.AC3.1: Intra-chunk sort violations detected (new test: rows [P3, P1, P2] in a single chunk)
- GH-7.AC3.2: Correctly sorted file passes with zero violations
- GH-7.AC3.3: Equal adjacent rows pass (rows [P1, P1, P2] — equal is not a violation)
- GH-7.AC7.1: check_id is "102"
- GH-7.AC7.2: severity is "Fail"

Replace `TestSortOrder` class with DuckDB view-based tests. Key changes:
- Create Parquet fixtures with `pl.DataFrame.write_parquet()`
- Open DuckDB connection, register view, call `check_sort_order(conn, view_name, schema)`
- Add new test for intra-chunk violation (previously undetectable)
- Add new test for equal adjacent rows (edge case)

**Verification:**
Run: `uv run pytest tests/test_global_checks.py::TestSortOrder -v`
Expected: All sort order tests pass including the new intra-chunk violation test.

**Commit:** `refactor(global-checks): migrate check_sort_order to DuckDB LAG window, detect all row-level violations`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Rewrite check_not_populated() to use DuckDB COUNT

**Verifies:** GH-7.AC1.1, GH-7.AC1.2, GH-7.AC1.3, GH-7.AC7.1, GH-7.AC7.2, GH-7.AC7.3

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py` (replace `check_not_populated`)
- Modify: `src/scdm_qa/pipeline.py` (update call site)

**Implementation:**

Replace `check_not_populated()` (currently lines 271-318) with:

```python
def check_not_populated(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
) -> list[StepResult]:
    check_defs = get_not_populated_checks_for_table(schema.table_key)
    if not check_defs:
        return []

    safe_view = view_name.replace('"', '""')

    try:
        total_rows = conn.execute(
            f'SELECT COUNT(*) FROM "{safe_view}"'
        ).fetchone()[0]
    except duckdb.Error as e:
        log.error("not populated check failed", error=str(e), view=view_name)
        return []

    results: list[StepResult] = []
    for check_def in check_defs:
        safe_col = check_def.column.replace('"', '""')
        try:
            non_null_count = conn.execute(
                f'SELECT COUNT("{safe_col}") FROM "{safe_view}"'
            ).fetchone()[0]
        except duckdb.Error:
            non_null_count = 0

        if non_null_count == 0:
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
```

Update pipeline call site:

```python
if get_not_populated_checks_for_table(schema.table_key):
    not_pop_steps = check_not_populated(conn, table_key, schema)
    global_steps.extend(not_pop_steps)
```

Remove `not_pop_reader = create_reader(...)`.

**Testing:**

Tests must verify:
- GH-7.AC1.1: check_not_populated executes SQL against DuckDB view
- GH-7.AC7.1: check_id is "111"
- GH-7.AC7.2: severity matches registry definitions

Replace `TestNotPopulated` class with DuckDB view-based tests. Key test cases to preserve:
- Entirely null column detected (n_failed = total_rows)
- Populated column passes (n_failed = 0)
- Only registry target columns checked
- No check defs returns empty list
- Multi-chunk accumulation now handled by single SQL query

**Verification:**
Run: `uv run pytest tests/test_global_checks.py::TestNotPopulated -v`
Expected: All not-populated tests pass with DuckDB views.

**Commit:** `refactor(global-checks): migrate check_not_populated to DuckDB COUNT aggregation`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

# DuckDB Global Checks Migration - Phase 2: Migrate Uniqueness and Overlapping Spans

**Goal:** Convert `check_uniqueness()` and `check_overlapping_spans()` to use the view-based DuckDB pattern from Phase 1, and remove all in-memory fallback paths.

**Architecture:** Both functions switch from accepting `file_path` + `chunks` + per-function DuckDB config to accepting `conn: duckdb.DuckDBPyConnection` + `view_name: str`. The SQL queries already exist in `_uniqueness_duckdb()` and `_overlapping_spans_duckdb()` — they are adapted to query the named view instead of calling `read_parquet()` directly. The private helper functions (`_uniqueness_duckdb`, `_uniqueness_in_memory`, `_overlapping_spans_duckdb`, `_overlapping_spans_in_memory`) are deleted.

**Tech Stack:** Python 3.12+, DuckDB, polars, pytest

**Scope:** 6 phases from original design (phase 2 of 6)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

### GH-7.AC1: All global checks execute via DuckDB SQL
- **GH-7.AC1.1 Success:** Each of the 7 check functions executes SQL against a DuckDB view and returns a valid StepResult (uniqueness and overlapping spans in this phase)
- **GH-7.AC1.2 Success:** No `pl.concat()` calls remain in any global check code path (for uniqueness and overlapping spans)
- **GH-7.AC1.3 Success:** Check functions accept `conn: DuckDBPyConnection` and `view_name: str` instead of chunk iterators (for uniqueness and overlapping spans)

### GH-7.AC2: In-memory fallback paths removed
- **GH-7.AC2.1 Success:** `_uniqueness_in_memory()` function is deleted
- **GH-7.AC2.2 Success:** `_overlapping_spans_in_memory()` function is deleted
- **GH-7.AC2.3 Success:** No conditional fallback logic ("if DuckDB unavailable, fall back to...") remains (for these two checks)

### GH-7.AC5: Enrollment and ENC checks via DuckDB
- **GH-7.AC5.1 Success:** Overlapping enrollment spans detected via LAG window (this phase)

### GH-7.AC7: Results backward compatible
- **GH-7.AC7.1 Success:** All check IDs unchanged (211, 215)
- **GH-7.AC7.2 Success:** All severities unchanged per check
- **GH-7.AC7.3 Success:** StepResult shape unchanged (same fields, same types)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Rewrite check_uniqueness() to use conn + view_name

**Verifies:** GH-7.AC1.1, GH-7.AC1.2, GH-7.AC1.3, GH-7.AC2.1, GH-7.AC2.3, GH-7.AC7.1, GH-7.AC7.2, GH-7.AC7.3

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py:47-197` (replace `check_uniqueness`, delete `_uniqueness_duckdb`, delete `_uniqueness_in_memory`)
- Modify: `src/scdm_qa/pipeline.py` (update call site in global checks block)

**Implementation:**

Replace `check_uniqueness()` (currently lines 47-74) with a new signature:

```python
def check_uniqueness(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> StepResult | None:
```

The function body queries the view by name (not `read_parquet()`):

```python
def check_uniqueness(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> StepResult | None:
    if not schema.unique_row:
        return None

    key_cols = list(schema.unique_row)
    description = f"Duplicate record(s) present for unique key variable(s): {', '.join(key_cols)}"
    cols_sql = ", ".join(f'"{c}"' for c in key_cols)
    safe_view = view_name.replace('"', '""')

    try:
        total_rows = conn.execute(
            f'SELECT COUNT(*) FROM "{safe_view}"'
        ).fetchone()[0]

        dup_row_total = conn.execute(f"""
            SELECT COALESCE(SUM(_dup_count), 0) FROM (
                SELECT COUNT(*) AS _dup_count
                FROM "{safe_view}"
                GROUP BY {cols_sql}
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        failing_df = conn.execute(f"""
            SELECT {cols_sql}, COUNT(*) AS _dup_count
            FROM "{safe_view}"
            GROUP BY {cols_sql}
            HAVING COUNT(*) > 1
            LIMIT {max_failing_rows}
        """).pl()
    except duckdb.Error as e:
        log.error("uniqueness check failed", error=str(e), view=view_name)
        return StepResult(
            step_index=-1,
            assertion_type="rows_distinct",
            column=", ".join(key_cols),
            description=f"Uniqueness check error: {e}",
            n_passed=0,
            n_failed=0,
            failing_rows=None,
            check_id="211",
            severity="Fail",
        )

    n_failed = dup_row_total
    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    log.info(
        "uniqueness check via duckdb",
        key_cols=key_cols,
        total_rows=total_rows,
        duplicate_rows=dup_row_total,
    )

    return StepResult(
        step_index=-1,
        assertion_type="rows_distinct",
        column=", ".join(key_cols),
        description=description,
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=failing_df if failing_df.height > 0 else None,
        check_id="211",
        severity="Fail",
    )
```

Add `import duckdb` at the top of `global_checks.py`.

Delete `_uniqueness_duckdb()` (lines 77-148) and `_uniqueness_in_memory()` (lines 151-197) entirely.

Update the pipeline call site in `pipeline.py` to pass `conn` and `table_key`:

```python
if schema.unique_row:
    uniqueness_step = check_uniqueness(
        conn,
        table_key,
        schema,
        max_failing_rows=config.max_failing_rows,
    )
    if uniqueness_step is not None:
        global_steps.append(uniqueness_step)
```

Remove the `uniqueness_reader = create_reader(...)` line and the `duckdb_memory_limit`, `duckdb_threads`, `duckdb_temp_directory` kwargs.

**Testing:**

Tests must verify:
- GH-7.AC1.1: check_uniqueness executes SQL against a DuckDB view and returns a valid StepResult
- GH-7.AC1.3: Function accepts conn + view_name (not file_path + chunks)
- GH-7.AC7.1: check_id is "211"
- GH-7.AC7.2: severity is "Fail"
- GH-7.AC7.3: StepResult shape unchanged

Replace `TestUniquenessInMemory` and `TestUniquenessDuckDB` test classes in `tests/test_global_checks.py` with a single `TestUniqueness` class that creates Parquet temp files, opens a DuckDB connection, registers a view, and calls `check_uniqueness(conn, view_name, schema, ...)`.

Follow the Parquet fixture pattern from `test_cross_table_engine.py`:
- Use `tmp_path` for temp Parquet files
- Use `pytest.importorskip("duckdb")` at the top of each test
- Create connection via `create_connection()`, register view, call check, close in finally

Remove the fallback tests (`test_fallback_to_in_memory_when_duckdb_unavailable`) — there is no fallback anymore.

**Verification:**
Run: `uv run pytest tests/test_global_checks.py -v`
Expected: All uniqueness tests pass with DuckDB views. No in-memory or fallback tests remain.

**Commit:** `refactor(global-checks): migrate check_uniqueness to view-based DuckDB, remove in-memory fallback`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Rewrite check_overlapping_spans() to use conn + view_name

**Verifies:** GH-7.AC1.1, GH-7.AC1.2, GH-7.AC1.3, GH-7.AC2.2, GH-7.AC2.3, GH-7.AC5.1, GH-7.AC7.1, GH-7.AC7.2, GH-7.AC7.3

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py` (replace `check_overlapping_spans`, delete `_overlapping_spans_duckdb`, delete `_overlapping_spans_in_memory`)
- Modify: `src/scdm_qa/pipeline.py` (update call site)

**Implementation:**

Replace `check_overlapping_spans()` with:

```python
def check_overlapping_spans(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> StepResult | None:
    if schema.table_key != "enrollment":
        return None

    safe_view = view_name.replace('"', '""')

    try:
        total_rows = conn.execute(
            f'SELECT COUNT(*) FROM "{safe_view}"'
        ).fetchone()[0]

        n_failed = conn.execute(f"""
            WITH spans AS (
                SELECT "PatID", "Enr_Start", "Enr_End",
                       LAG("Enr_End") OVER (
                           PARTITION BY "PatID" ORDER BY "Enr_Start"
                       ) AS prev_end
                FROM "{safe_view}"
            )
            SELECT COUNT(*) FROM spans WHERE "Enr_Start" < prev_end
        """).fetchone()[0] or 0

        failing_df = conn.execute(f"""
            WITH spans AS (
                SELECT "PatID", "Enr_Start", "Enr_End",
                       LAG("Enr_End") OVER (
                           PARTITION BY "PatID" ORDER BY "Enr_Start"
                       ) AS prev_end
                FROM "{safe_view}"
            )
            SELECT "PatID", "Enr_Start", "Enr_End", prev_end
            FROM spans
            WHERE "Enr_Start" < prev_end
            LIMIT {max_failing_rows}
        """).pl()
    except duckdb.Error as e:
        log.error("overlapping spans check failed", error=str(e), view=view_name)
        return StepResult(
            step_index=-1,
            assertion_type="overlapping_spans",
            column="PatID, Enr_Start, Enr_End",
            description=f"Overlapping spans check error: {e}",
            n_passed=0,
            n_failed=0,
            failing_rows=None,
            check_id="215",
            severity="Fail",
        )

    n_passed = total_rows - n_failed if total_rows > n_failed else 0

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
```

Delete `_overlapping_spans_duckdb()` and `_overlapping_spans_in_memory()` entirely.

Update pipeline call site:

```python
if schema.table_key == "enrollment":
    overlap_step = check_overlapping_spans(
        conn,
        table_key,
        schema,
        max_failing_rows=config.max_failing_rows,
    )
    if overlap_step is not None:
        global_steps.append(overlap_step)
```

Remove `overlap_reader = create_reader(...)` and all `duckdb_memory_limit/threads/temp_directory` kwargs.

**Testing:**

Tests must verify:
- GH-7.AC5.1: Overlapping enrollment spans detected via LAG window
- GH-7.AC1.3: Function accepts conn + view_name
- GH-7.AC7.1: check_id is "215"
- GH-7.AC7.2: severity is "Fail"

Replace `TestOverlappingSpans` test class with a new version using DuckDB views. All tests should create Parquet fixtures, register views, and call `check_overlapping_spans(conn, view_name, schema, ...)`.

Remove fallback tests (`test_duckdb_fallback_to_in_memory`).

Key test cases to preserve:
- Overlapping spans detected (P1: [100,200] and [150,300])
- Non-overlapping spans pass (P1: [100,200] and [201,300])
- Returns None for non-enrollment table
- Multiple patients with mixed overlaps
- check_id "215" and severity "Fail"

**Verification:**
Run: `uv run pytest tests/test_global_checks.py -v`
Expected: All overlapping span tests pass with DuckDB views. No in-memory or fallback tests remain.

**Commit:** `refactor(global-checks): migrate check_overlapping_spans to view-based DuckDB, remove in-memory fallback`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

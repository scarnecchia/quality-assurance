# DuckDB Global Checks Migration - Phase 4: Migrate Date Ordering and Cause of Death

**Goal:** Convert `check_date_ordering()` and `check_cause_of_death()` to DuckDB SQL queries against views.

**Architecture:** `check_date_ordering()` replaces per-chunk accumulation with a single SQL `WHERE date_a > date_b AND both NOT NULL` query per date pair. `check_cause_of_death()` replaces `pl.concat()` accumulation with `GROUP BY PatID HAVING SUM(CASE WHEN CauseType='U'...)` aggregation.

**Tech Stack:** Python 3.12+, DuckDB, polars, pytest

**Scope:** 6 phases from original design (phase 4 of 6)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

### GH-7.AC1: All global checks execute via DuckDB SQL
- **GH-7.AC1.1 Success:** Each of the 7 check functions executes SQL against a DuckDB view and returns a valid StepResult (date ordering and cause of death in this phase)
- **GH-7.AC1.2 Success:** No `pl.concat()` calls remain in any global check code path (for date ordering and cause of death)
- **GH-7.AC1.3 Success:** Check functions accept `conn: DuckDBPyConnection` and `view_name: str` instead of chunk iterators (for date ordering and cause of death)

### GH-7.AC4: Date ordering and cause of death via DuckDB
- **GH-7.AC4.1 Success:** Date ordering violations detected when date_a > date_b
- **GH-7.AC4.2 Success:** Rows where either date is null are skipped (not flagged)
- **GH-7.AC4.3 Success:** Patients missing CauseType='U' detected (check 236)
- **GH-7.AC4.4 Success:** Patients with multiple CauseType='U' detected (check 237)
- **GH-7.AC4.5 Success:** Failing row samples are bounded by max_failing_rows

### GH-7.AC7: Results backward compatible
- **GH-7.AC7.1 Success:** All check IDs unchanged (226, 236, 237)
- **GH-7.AC7.2 Success:** All severities unchanged per check
- **GH-7.AC7.3 Success:** StepResult shape unchanged (same fields, same types)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Rewrite check_date_ordering() to use DuckDB SQL

**Verifies:** GH-7.AC1.1, GH-7.AC1.2, GH-7.AC1.3, GH-7.AC4.1, GH-7.AC4.2, GH-7.AC4.5, GH-7.AC7.1, GH-7.AC7.2, GH-7.AC7.3

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py` (replace `check_date_ordering`)
- Modify: `src/scdm_qa/pipeline.py` (update call site)

**Implementation:**

Replace `check_date_ordering()` (currently lines 321-395) with:

```python
def check_date_ordering(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    ordering_defs = get_date_ordering_checks_for_table(schema.table_key)
    if not ordering_defs:
        return []

    safe_view = view_name.replace('"', '""')
    results: list[StepResult] = []

    for pair_def in ordering_defs:
        safe_a = pair_def.date_a.replace('"', '""')
        safe_b = pair_def.date_b.replace('"', '""')

        try:
            # Count rows where both dates are non-null
            both_present = conn.execute(f"""
                SELECT COUNT(*) FROM "{safe_view}"
                WHERE "{safe_a}" IS NOT NULL AND "{safe_b}" IS NOT NULL
            """).fetchone()[0]

            # Count violations: date_a > date_b (both non-null)
            n_failed = conn.execute(f"""
                SELECT COUNT(*) FROM "{safe_view}"
                WHERE "{safe_a}" IS NOT NULL
                  AND "{safe_b}" IS NOT NULL
                  AND "{safe_a}" > "{safe_b}"
            """).fetchone()[0] or 0

            n_passed = both_present - n_failed

            # Sample failing rows
            failing_df = conn.execute(f"""
                SELECT * FROM "{safe_view}"
                WHERE "{safe_a}" IS NOT NULL
                  AND "{safe_b}" IS NOT NULL
                  AND "{safe_a}" > "{safe_b}"
                LIMIT {max_failing_rows}
            """).pl()

            failing = failing_df if failing_df.height > 0 else None

        except duckdb.Error as e:
            log.error(
                "date ordering check failed",
                error=str(e),
                view=view_name,
                date_a=pair_def.date_a,
                date_b=pair_def.date_b,
            )
            n_passed = 0
            n_failed = 0
            failing = None

        results.append(
            StepResult(
                step_index=-1,
                assertion_type="date_ordering",
                column=f"{pair_def.date_a}, {pair_def.date_b}",
                description=f"{pair_def.description} (check {pair_def.check_id})",
                n_passed=n_passed,
                n_failed=n_failed,
                failing_rows=failing,
                check_id=pair_def.check_id,
                severity=pair_def.severity,
            )
        )

    return results
```

Update pipeline call site:

```python
if get_date_ordering_checks_for_table(schema.table_key):
    date_order_steps = check_date_ordering(
        conn, table_key, schema,
        max_failing_rows=config.max_failing_rows,
    )
    global_steps.extend(date_order_steps)
```

Remove `date_order_reader = create_reader(...)`.

**Testing:**

Tests must verify:
- GH-7.AC4.1: Date ordering violations detected when date_a > date_b
- GH-7.AC4.2: Rows where either date is null are skipped
- GH-7.AC4.5: Failing rows bounded by max_failing_rows
- GH-7.AC7.1: check_id is "226"
- GH-7.AC7.2: severity matches registry

Replace `TestDateOrdering` class with DuckDB view-based tests. Key test cases:
- ADate > DDate flagged as violation
- Null dates skipped
- Clean data passes
- Enrollment date ordering (Enr_Start > Enr_End)
- Failing rows sampled

**Verification:**
Run: `uv run pytest tests/test_global_checks.py::TestDateOrdering -v`
Expected: All date ordering tests pass with DuckDB views.

**Commit:** `refactor(global-checks): migrate check_date_ordering to DuckDB SQL`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Rewrite check_cause_of_death() to use DuckDB SQL

**Verifies:** GH-7.AC1.1, GH-7.AC1.2, GH-7.AC1.3, GH-7.AC4.3, GH-7.AC4.4, GH-7.AC4.5, GH-7.AC7.1, GH-7.AC7.2, GH-7.AC7.3

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py` (replace `check_cause_of_death`)
- Modify: `src/scdm_qa/pipeline.py` (update call site)

**Implementation:**

Replace `check_cause_of_death()` (currently lines 398-475) with:

```python
def check_cause_of_death(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    if schema.table_key != "cause_of_death":
        return []

    safe_view = view_name.replace('"', '""')

    try:
        total_patients = conn.execute(f"""
            SELECT COUNT(DISTINCT "PatID") FROM "{safe_view}"
        """).fetchone()[0]

        # Check 236: patients with zero CauseType='U'
        missing_u_count = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT "PatID"
                FROM "{safe_view}"
                GROUP BY "PatID"
                HAVING SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) = 0
            )
        """).fetchone()[0] or 0

        failing_236 = conn.execute(f"""
            SELECT "PatID", 0 AS u_count
            FROM "{safe_view}"
            GROUP BY "PatID"
            HAVING SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) = 0
            LIMIT {max_failing_rows}
        """).pl()

        # Check 237: patients with more than one CauseType='U'
        multiple_u_count = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT "PatID"
                FROM "{safe_view}"
                GROUP BY "PatID"
                HAVING SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) > 1
            )
        """).fetchone()[0] or 0

        failing_237 = conn.execute(f"""
            SELECT "PatID",
                   SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) AS u_count
            FROM "{safe_view}"
            GROUP BY "PatID"
            HAVING SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) > 1
            LIMIT {max_failing_rows}
        """).pl()

    except duckdb.Error as e:
        log.error("cause of death check failed", error=str(e), view=view_name)
        return [
            StepResult(
                step_index=-1,
                assertion_type="cause_of_death",
                column="CauseType",
                description=f"Cause of death check error: {e}",
                n_passed=0, n_failed=0, failing_rows=None,
                check_id="236", severity="Fail",
            ),
            StepResult(
                step_index=-1,
                assertion_type="cause_of_death",
                column="CauseType",
                description=f"Cause of death check error: {e}",
                n_passed=0, n_failed=0, failing_rows=None,
                check_id="237", severity="Fail",
            ),
        ]

    return [
        StepResult(
            step_index=-1,
            assertion_type="cause_of_death",
            column="CauseType",
            description="Each patient has underlying cause of death (check 236)",
            n_passed=total_patients - missing_u_count,
            n_failed=missing_u_count,
            failing_rows=failing_236 if failing_236.height > 0 else None,
            check_id="236",
            severity="Fail",
        ),
        StepResult(
            step_index=-1,
            assertion_type="cause_of_death",
            column="CauseType",
            description="Each patient has at most one underlying cause of death (check 237)",
            n_passed=total_patients - multiple_u_count,
            n_failed=multiple_u_count,
            failing_rows=failing_237 if failing_237.height > 0 else None,
            check_id="237",
            severity="Fail",
        ),
    ]
```

Update pipeline call site:

```python
if schema.table_key == "cause_of_death":
    cod_steps = check_cause_of_death(
        conn, table_key, schema,
        max_failing_rows=config.max_failing_rows,
    )
    global_steps.extend(cod_steps)
```

Remove `cod_reader = create_reader(...)`.

**Testing:**

Tests must verify:
- GH-7.AC4.3: Patients missing CauseType='U' detected (check 236)
- GH-7.AC4.4: Patients with multiple CauseType='U' detected (check 237)
- GH-7.AC4.5: Failing row samples bounded by max_failing_rows
- GH-7.AC7.1: check_ids are "236" and "237"
- GH-7.AC7.2: severity is "Fail"

Replace `TestCauseOfDeath` class with DuckDB view-based tests. Key test cases:
- Patients missing CauseType='U' flagged
- Patients with multiple CauseType='U' flagged
- Clean data passes (exactly one 'U' per patient)
- Returns empty list for non-cause_of_death tables

**Verification:**
Run: `uv run pytest tests/test_global_checks.py::TestCauseOfDeath -v`
Expected: All cause of death tests pass with DuckDB views.

**Commit:** `refactor(global-checks): migrate check_cause_of_death to DuckDB SQL`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

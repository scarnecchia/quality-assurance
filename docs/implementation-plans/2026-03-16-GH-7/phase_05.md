# DuckDB Global Checks Migration - Phase 5: Migrate Enrollment Gaps and ENC Combinations

**Goal:** Convert `check_enrollment_gaps()` and `check_enc_combinations()` — the final two OOM-risk checks — to DuckDB SQL.

**Architecture:** `check_enrollment_gaps()` uses a LAG window with date arithmetic to detect gaps. `check_enc_combinations()` uses CASE WHEN SQL driven by the `ENC_COMBINATION_RULES` and `ENC_RATE_THRESHOLDS` dicts, with GROUP BY aggregation for rate threshold checks.

**Tech Stack:** Python 3.12+, DuckDB, polars, pytest

**Scope:** 6 phases from original design (phase 5 of 6)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

### GH-7.AC1: All global checks execute via DuckDB SQL
- **GH-7.AC1.1 Success:** Each of the 7 check functions executes SQL against a DuckDB view and returns a valid StepResult (enrollment gaps and ENC combinations in this phase)
- **GH-7.AC1.2 Success:** No `pl.concat()` calls remain in any global check code path (for enrollment gaps and ENC combinations)
- **GH-7.AC1.3 Success:** Check functions accept `conn: DuckDBPyConnection` and `view_name: str` instead of chunk iterators (for enrollment gaps and ENC combinations)

### GH-7.AC5: Enrollment and ENC checks via DuckDB
- **GH-7.AC5.2 Success:** Non-bridged enrollment gaps detected (prev_end + 1 day < Enr_Start)
- **GH-7.AC5.3 Success:** Adjacent spans (Enr_End + 1 day == next Enr_Start) pass
- **GH-7.AC5.4 Success:** Invalid ENC field combinations flagged per combination rules
- **GH-7.AC5.5 Success:** EncType rate threshold violations detected (check 245)
- **GH-7.AC5.6 Edge:** Unknown EncType values are flagged as invalid

### GH-7.AC7: Results backward compatible
- **GH-7.AC7.1 Success:** All check IDs unchanged (216, 244, 245)
- **GH-7.AC7.2 Success:** All severities unchanged per check
- **GH-7.AC7.3 Success:** StepResult shape unchanged (same fields, same types)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Rewrite check_enrollment_gaps() to use DuckDB LAG window

**Verifies:** GH-7.AC1.1, GH-7.AC1.2, GH-7.AC1.3, GH-7.AC5.2, GH-7.AC5.3, GH-7.AC7.1, GH-7.AC7.2, GH-7.AC7.3

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py` (replace `check_enrollment_gaps`)
- Modify: `src/scdm_qa/pipeline.py` (update call site)

**Implementation:**

Replace `check_enrollment_gaps()` (currently lines 623-683) with:

```python
def check_enrollment_gaps(
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

        # Detect gaps: prev_end + 1 < Enr_Start
        # DuckDB handles date arithmetic natively (DATE + INTERVAL '1 day')
        # For integer dates, prev_end + 1 < Enr_Start
        n_failed = conn.execute(f"""
            WITH spans AS (
                SELECT "PatID", "Enr_Start", "Enr_End",
                       LAG("Enr_End") OVER (
                           PARTITION BY "PatID" ORDER BY "Enr_Start"
                       ) AS prev_end
                FROM "{safe_view}"
            )
            SELECT COUNT(*) FROM spans
            WHERE prev_end IS NOT NULL
              AND (prev_end + 1) < "Enr_Start"
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
            WHERE prev_end IS NOT NULL
              AND (prev_end + 1) < "Enr_Start"
            LIMIT {max_failing_rows}
        """).pl()

    except duckdb.Error as e:
        log.error("enrollment gaps check failed", error=str(e), view=view_name)
        return StepResult(
            step_index=-1,
            assertion_type="enrollment_gaps",
            column="PatID, Enr_Start, Enr_End",
            description=f"Enrollment gaps check error: {e}",
            n_passed=0, n_failed=0, failing_rows=None,
            check_id="216", severity="Warn",
        )

    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    return StepResult(
        step_index=-1,
        assertion_type="enrollment_gaps",
        column="PatID, Enr_Start, Enr_End",
        description="No non-bridged enrollment gaps (check 216)",
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=failing_df if failing_df.height > 0 else None,
        check_id="216",
        severity="Warn",
    )
```

**Date arithmetic note:** DuckDB does NOT support `DATE + INTEGER` directly. The `+ 1` arithmetic works for integer-typed date columns (which is the SCDM standard — dates are stored as integer SAS date values). If the Parquet file happens to contain actual `DATE`-typed columns, DuckDB requires `+ INTERVAL '1 day'` instead. The implementor should check the actual column type at execution time. A safe approach: use `TRY_CAST(prev_end AS INTEGER)` to detect the type, or query `typeof("Enr_Start")` from the view and branch accordingly. Alternatively, cast to integer first: `CAST(prev_end AS INTEGER) + 1 < CAST("Enr_Start" AS INTEGER)`. Given that SCDM stores dates as integers, the `+ 1` approach shown above is the primary path. Add a test with integer dates to confirm.

Update pipeline call site:

```python
# Inside the enrollment block (after check_overlapping_spans):
gaps_step = check_enrollment_gaps(
    conn, table_key, schema,
    max_failing_rows=config.max_failing_rows,
)
if gaps_step is not None:
    global_steps.append(gaps_step)
```

Remove `gaps_reader = create_reader(...)`.

**Testing:**

Tests must verify:
- GH-7.AC5.2: Non-bridged gaps detected (prev_end + 1 < Enr_Start)
- GH-7.AC5.3: Adjacent spans pass (prev_end + 1 == Enr_Start)
- GH-7.AC7.1: check_id is "216"
- GH-7.AC7.2: severity is "Warn"

Replace `TestEnrollmentGaps` class with DuckDB view-based tests. Key test cases:
- Gap detected (e.g., spans [100,200] and [300,400] — gap of 99 days)
- Adjacent spans pass (e.g., spans [100,200] and [201,300])
- Correctly bridged spans pass
- Returns None for non-enrollment table
- Integer dates and Date-type dates both work

**Verification:**
Run: `uv run pytest tests/test_global_checks.py::TestEnrollmentGaps -v`
Expected: All enrollment gaps tests pass with DuckDB views.

**Commit:** `refactor(global-checks): migrate check_enrollment_gaps to DuckDB LAG window`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Rewrite check_enc_combinations() to use DuckDB SQL

**Verifies:** GH-7.AC1.1, GH-7.AC1.2, GH-7.AC1.3, GH-7.AC5.4, GH-7.AC5.5, GH-7.AC5.6, GH-7.AC7.1, GH-7.AC7.2, GH-7.AC7.3

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py` (replace `check_enc_combinations`)
- Modify: `src/scdm_qa/pipeline.py` (update call site)

**Implementation:**

Replace `check_enc_combinations()` (currently lines 686-797) with:

```python
def check_enc_combinations(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    if schema.table_key != "encounter":
        return []

    safe_view = view_name.replace('"', '""')

    # Build CASE WHEN conditions from ENC_COMBINATION_RULES
    # A row is invalid if:
    #   1. Its EncType has a rule and a required field is NULL, OR
    #   2. Its EncType is not in the known types
    known_types = list(ENC_COMBINATION_RULES.keys())
    type_list = ", ".join(f"'{t}'" for t in known_types)

    violation_cases = []
    for enc_type, (ddate_req, disp_req, status_req) in ENC_COMBINATION_RULES.items():
        conditions = []
        if ddate_req:
            conditions.append('"DDate" IS NULL')
        if disp_req:
            conditions.append('"Discharge_Disposition" IS NULL')
        if status_req:
            conditions.append('"Discharge_Status" IS NULL')
        if conditions:
            or_clause = " OR ".join(conditions)
            violation_cases.append(
                f"""("EncType" = '{enc_type}' AND ({or_clause}))"""
            )

    # Also flag unknown EncType
    violation_cases.append(f'"EncType" NOT IN ({type_list})')
    invalid_where = " OR ".join(violation_cases)

    try:
        total_rows = conn.execute(
            f'SELECT COUNT(*) FROM "{safe_view}"'
        ).fetchone()[0]

        # Check 244: count invalid rows
        n_failed_244 = conn.execute(f"""
            SELECT COUNT(*) FROM "{safe_view}"
            WHERE {invalid_where}
        """).fetchone()[0] or 0

        n_passed_244 = total_rows - n_failed_244

        failing_244 = None
        if n_failed_244 > 0:
            failing_244 = conn.execute(f"""
                SELECT "EncType", "DDate", "Discharge_Disposition", "Discharge_Status"
                FROM "{safe_view}"
                WHERE {invalid_where}
                LIMIT {max_failing_rows}
            """).pl()

    except duckdb.Error as e:
        log.error("ENC combination check failed", error=str(e), view=view_name)
        return [
            StepResult(
                step_index=-1,
                assertion_type="enc_combinations",
                column="EncType, DDate, Discharge_Disposition, Discharge_Status",
                description=f"ENC combination check error: {e}",
                n_passed=0, n_failed=0, failing_rows=None,
                check_id="244", severity="Fail",
            )
        ]

    results: list[StepResult] = [
        StepResult(
            step_index=-1,
            assertion_type="enc_combinations",
            column="EncType, DDate, Discharge_Disposition, Discharge_Status",
            description="Valid ENC field combination (check 244)",
            n_passed=n_passed_244,
            n_failed=n_failed_244,
            failing_rows=failing_244 if failing_244 is not None and failing_244.height > 0 else None,
            check_id="244",
            severity="Fail",
        )
    ]

    # Check 245: rate threshold per EncType
    for enc_type, threshold in ENC_RATE_THRESHOLDS.items():
        try:
            row = conn.execute(f"""
                SELECT
                    COUNT(*) AS type_total,
                    SUM(CASE WHEN ({invalid_where}) THEN 1 ELSE 0 END) AS type_invalid
                FROM "{safe_view}"
                WHERE "EncType" = '{enc_type}'
            """).fetchone()

            type_total = row[0]
            type_invalid = row[1] or 0

            if type_total == 0:
                continue

            rate = type_invalid / type_total

            if rate > threshold:
                n_failed = type_invalid
                n_passed = type_total - type_invalid
            else:
                n_failed = 0
                n_passed = type_total

            results.append(
                StepResult(
                    step_index=-1,
                    assertion_type="enc_combination_rate",
                    column=f"EncType={enc_type}",
                    description=f"{enc_type} invalid combo rate {'>' if rate > threshold else '<='} {threshold:.0%} (check 245)",
                    n_passed=n_passed,
                    n_failed=n_failed,
                    failing_rows=None,
                    check_id="245",
                    severity="Fail",
                )
            )

        except duckdb.Error as e:
            log.error(
                "ENC rate threshold check failed",
                error=str(e),
                enc_type=enc_type,
            )

    return results
```

Update pipeline call site:

```python
if schema.table_key == "encounter":
    enc_combo_steps = check_enc_combinations(
        conn, table_key, schema,
        max_failing_rows=config.max_failing_rows,
    )
    global_steps.extend(enc_combo_steps)
```

Remove `enc_combo_reader = create_reader(...)`.

**Testing:**

Tests must verify:
- GH-7.AC5.4: Invalid ENC field combinations flagged per rules (IP with null DDate, etc.)
- GH-7.AC5.5: EncType rate threshold violations detected
- GH-7.AC5.6: Unknown EncType values flagged as invalid
- GH-7.AC7.1: check_ids are "244" and "245"
- GH-7.AC7.2: severity is "Fail"

Replace `TestEncCombinations` class with DuckDB view-based tests. Key test cases:
- IP row with null DDate flagged
- ED row with all nulls passes (optional fields)
- Unknown EncType flagged
- Rate threshold exceeded for specific EncType
- Rate threshold not exceeded passes

**Verification:**
Run: `uv run pytest tests/test_global_checks.py::TestEncCombinations -v`
Expected: All ENC combination tests pass with DuckDB views.

**Commit:** `refactor(global-checks): migrate check_enc_combinations to DuckDB SQL`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

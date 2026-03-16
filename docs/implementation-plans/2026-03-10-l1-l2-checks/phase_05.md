# L1 & L2 Validation Checks Implementation Plan — Phase 5

**Goal:** Implement L2 cause of death checks 236 (missing underlying cause) and 237 (multiple underlying causes).

**Architecture:** New `check_cause_of_death()` function in `global_checks.py`. Single scan, accumulates all (PatID, CauseType) tuples, groups by PatID, checks for count of CauseType='U'. Returns two StepResults (one per check). Wired into `pipeline.py` for the cause_of_death table.

**Tech Stack:** Python 3.12+, polars, pytest

**Scope:** 8 phases from original design (phase 5 of 8)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### l1-l2-checks.AC2: L2 checks detect cross-record data quality issues
- **l1-l2-checks.AC2.3 Success:** Check 236 flags patients in COD with no CauseType='U' record
- **l1-l2-checks.AC2.7 Edge:** Check 237 flags patients with >1 CauseType='U' but a patient with exactly 1 'U' passes both 236 and 237

### l1-l2-checks.AC4: Severity levels match SAS reference
- **l1-l2-checks.AC4.1 Success:** Checks marked Fail in SAS reference produce error-level results

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Implement check_cause_of_death() in global_checks.py

**Verifies:** l1-l2-checks.AC2.3, l1-l2-checks.AC2.7

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py`

**Implementation:**

Add new function:

```python
def check_cause_of_death(
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame],
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    """Checks 236 and 237: Validate underlying cause of death records.

    236: Each patient in COD must have at least one CauseType='U' record.
    237: Each patient in COD must have at most one CauseType='U' record.

    Returns two StepResults (236 first, then 237).
    """
    if schema.table_key != "cause_of_death":
        return []

    # Accumulate all PatID + CauseType across chunks
    all_records: list[pl.DataFrame] = []
    for chunk in chunks:
        if "PatID" in chunk.columns and "CauseType" in chunk.columns:
            all_records.append(chunk.select("PatID", "CauseType"))

    if not all_records:
        return []

    combined = pl.concat(all_records)

    # Count CauseType='U' records per patient
    u_counts = (
        combined.filter(pl.col("CauseType") == "U")
        .group_by("PatID")
        .agg(pl.len().alias("u_count"))
    )

    all_patients = combined.select("PatID").unique()
    total_patients = all_patients.height

    # Join to get u_count per patient (patients not in u_counts have 0)
    patient_u = all_patients.join(u_counts, on="PatID", how="left").with_columns(
        pl.col("u_count").fill_null(0)
    )

    # Check 236: patients with zero CauseType='U'
    missing_u = patient_u.filter(pl.col("u_count") == 0)
    n_failed_236 = missing_u.height
    n_passed_236 = total_patients - n_failed_236
    failing_236 = missing_u.head(max_failing_rows) if missing_u.height > 0 else None

    # Check 237: patients with more than one CauseType='U'
    multiple_u = patient_u.filter(pl.col("u_count") > 1)
    n_failed_237 = multiple_u.height
    n_passed_237 = total_patients - n_failed_237
    failing_237 = multiple_u.head(max_failing_rows) if multiple_u.height > 0 else None

    return [
        StepResult(
            step_index=-1,
            assertion_type="cause_of_death",
            column="CauseType",
            description="Each patient has underlying cause of death (check 236)",
            n_passed=n_passed_236,
            n_failed=n_failed_236,
            failing_rows=failing_236,
            check_id="236",
            severity="Fail",
        ),
        StepResult(
            step_index=-1,
            assertion_type="cause_of_death",
            column="CauseType",
            description="Each patient has at most one underlying cause of death (check 237)",
            n_passed=n_passed_237,
            n_failed=n_failed_237,
            failing_rows=failing_237,
            check_id="237",
            severity="Fail",
        ),
    ]
```

Key decisions:
- Collects all records in memory (COD tables are typically small — one row per cause per patient)
- Counts are per-patient, not per-row
- Uses left join to identify patients with zero 'U' records (check 236)

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: Existing tests pass.

**Commit:** `feat: add check_cause_of_death for L2 checks 236, 237`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Wire check_cause_of_death into pipeline.py

**Verifies:** l1-l2-checks.AC2.3

**Files:**
- Modify: `src/scdm_qa/pipeline.py:16`

**Implementation:**

Update import:

```python
from scdm_qa.validation.global_checks import (
    check_sort_order, check_uniqueness, check_not_populated,
    check_date_ordering, check_cause_of_death,
)
```

In `_process_table()`, after the date ordering block, add:

```python
    # L2 check: cause of death (checks 236, 237)
    if schema.table_key == "cause_of_death":
        cod_reader = create_reader(file_path, chunk_size=config.chunk_size)
        cod_steps = check_cause_of_death(schema, cod_reader.chunks(), max_failing_rows=config.max_failing_rows)
        global_steps.extend(cod_steps)
```

The `table_key` guard is explicit here (matching the guard inside the function) to avoid creating unnecessary readers.

**Verification:**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

**Commit:** `feat: wire check_cause_of_death into pipeline`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Tests for check_cause_of_death

**Verifies:** l1-l2-checks.AC2.3, l1-l2-checks.AC2.7, l1-l2-checks.AC4.1

**Files:**
- Modify: `tests/test_global_checks.py`

**Implementation:**

Add new test classes to `tests/test_global_checks.py`.

**Testing:**

Tests must verify each AC listed:

- **l1-l2-checks.AC2.3:** Create COD data with patient P1 having CauseType='C' and 'I' but no 'U'. Call `check_cause_of_death` with a cause_of_death schema. Assert check 236 result has n_failed >= 1.

- **l1-l2-checks.AC2.7 (exactly one U passes both):** Create COD data with patient P2 having exactly one CauseType='U' record (plus others). Assert check 236 has n_failed == 0 for P2, and check 237 has n_failed == 0 for P2.

- **l1-l2-checks.AC2.7 (multiple U fails 237):** Create COD data with patient P3 having two CauseType='U' records. Assert check 237 has n_failed >= 1.

- **l1-l2-checks.AC4.1:** Assert check_id="236" and check_id="237" on the respective StepResults.

- **Non-COD table:** Call with a non-cause_of_death schema. Assert empty list returned.

- **Multiple chunks:** Distribute records across chunks and verify counts accumulate correctly.

Use `get_schema("cause_of_death")` for the schema. Use `iter([df])` or `iter([df1, df2])` for chunks.

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: All tests pass.

Run: `uv run pytest tests/ -v`
Expected: Full suite passes.

**Commit:** `test: add tests for L2 cause of death checks 236, 237`
<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->

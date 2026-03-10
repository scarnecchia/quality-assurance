# L1 & L2 Validation Checks Implementation Plan — Phase 2

**Goal:** Add L1 per-chunk checks 122 (leading spaces), 124 (unexpected zeros), and 128 (non-numeric characters) as pointblank assertions in the existing validation pipeline.

**Architecture:** New loop in `build_validation()` iterates `L1CheckDef` entries for the current table and adds pointblank assertions. Corresponding loop in `_build_step_descriptions()` maintains sync. Both use `get_per_chunk_checks_for_table()` from the check registry.

**Tech Stack:** Python 3.12+, polars, pointblank (col_vals_regex, col_vals_gt), pytest

**Scope:** 8 phases from original design (phase 2 of 8)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### l1-l2-checks.AC1: L1 checks detect column-level data quality issues
- **l1-l2-checks.AC1.2 Success:** Check 122 flags character values with leading whitespace
- **l1-l2-checks.AC1.3 Success:** Check 124 flags numeric columns containing zero values as suspicious
- **l1-l2-checks.AC1.4 Success:** Check 128 flags PostalCode values containing non-numeric characters
- **l1-l2-checks.AC1.6 Edge:** Null values in L1 check columns are not flagged (na_pass=True for 122, 124, 128)

### l1-l2-checks.AC4: Severity levels match SAS reference
- **l1-l2-checks.AC4.1 Success:** Checks marked Fail in SAS reference produce error-level results

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Add L1 per-chunk assertions to build_validation()

**Verifies:** l1-l2-checks.AC1.2, l1-l2-checks.AC1.3, l1-l2-checks.AC1.4, l1-l2-checks.AC1.6

**Files:**
- Modify: `src/scdm_qa/schemas/validation.py:1-71`

**Implementation:**

Add an import for the check registry at the top of the file:

```python
from scdm_qa.schemas.checks import get_per_chunk_checks_for_table
```

After the conditional rules loop (after line 69), add a new loop that iterates per-chunk L1 check definitions for the current table and adds the appropriate pointblank assertion:

```python
    # L1 per-chunk checks (122, 124, 128)
    for check_def in get_per_chunk_checks_for_table(schema.table_key):
        if check_def.column not in present_columns:
            continue

        if check_def.check_type == "leading_spaces":
            # Check 122: Flag values with leading whitespace
            # Pattern matches: non-space first char OR empty string
            validation = validation.col_vals_regex(
                columns=check_def.column,
                pattern=r"^[^ ]|^$",
                na_pass=True,
            )
        elif check_def.check_type == "unexpected_zeros":
            # Check 124: Flag numeric columns containing zero
            validation = validation.col_vals_gt(
                columns=check_def.column,
                value=0,
                na_pass=True,
            )
        elif check_def.check_type == "non_numeric":
            # Check 128: Flag non-numeric characters
            validation = validation.col_vals_regex(
                columns=check_def.column,
                pattern=r"^[0-9]*$",
                na_pass=True,
            )
```

This loop goes AFTER the conditional rules loop so that the step ordering is: nullability → enum → length → conditional → L1 checks.

**Critical invariant:** The `present_columns` guard (`check_def.column not in present_columns`) in `build_validation()` and the identical guard in `_build_step_descriptions()` (Task 2) MUST use the same iteration order and the same skip condition. If one adds an assertion but the other skips the description (or vice versa), the step count mismatch guard in `runner.py:67-76` will fire. Both loops iterate `get_per_chunk_checks_for_table(schema.table_key)` with the same `present_columns` set, so they stay in sync as long as both use the same condition.

**Verification:**

Run: `uv run pytest tests/test_validation.py -v`
Expected: All existing tests pass (new checks only fire for tables with L1 definitions).

**Commit:** `feat: add L1 per-chunk checks 122, 124, 128 to build_validation`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add L1 per-chunk step descriptions to _build_step_descriptions()

**Verifies:** l1-l2-checks.AC1.2, l1-l2-checks.AC1.3, l1-l2-checks.AC1.4

**Files:**
- Modify: `src/scdm_qa/validation/runner.py:110-150`

**Implementation:**

Add an import at the top of runner.py:

```python
from scdm_qa.schemas.checks import get_per_chunk_checks_for_table
```

After the conditional rules loop in `_build_step_descriptions()`, add a matching loop that generates step descriptions for L1 checks. The order and conditions MUST exactly mirror the loop added to `build_validation()` in Task 1 — otherwise the step count mismatch check at line 67-76 will fire.

```python
    # L1 per-chunk checks (122, 124, 128) — must mirror build_validation() order
    for check_def in get_per_chunk_checks_for_table(schema.table_key):
        if check_def.column not in present_columns:
            continue

        if check_def.check_type == "leading_spaces":
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_regex",
                check_def.column,
                f"{check_def.column} no leading spaces (check {check_def.check_id})",
                check_def.check_id,
                check_def.severity,
            ))
        elif check_def.check_type == "unexpected_zeros":
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_gt",
                check_def.column,
                f"{check_def.column} not zero (check {check_def.check_id})",
                check_def.check_id,
                check_def.severity,
            ))
        elif check_def.check_type == "non_numeric":
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_regex",
                check_def.column,
                f"{check_def.column} numeric only (check {check_def.check_id})",
                check_def.check_id,
                check_def.severity,
            ))

    return descriptions
```

**Verification:**

Run: `uv run pytest tests/test_runner.py -v`
Expected: All existing runner tests pass. The step count mismatch guard should NOT fire.

**Commit:** `feat: add L1 per-chunk step descriptions to runner`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Tests for L1 per-chunk checks

**Verifies:** l1-l2-checks.AC1.2, l1-l2-checks.AC1.3, l1-l2-checks.AC1.4, l1-l2-checks.AC1.6, l1-l2-checks.AC4.1

**Files:**
- Create: `tests/test_l1_checks.py`

**Implementation:**

Create a new test file following existing project patterns (class-based grouping, inline Polars DataFrames, tmp_path for parquet files, plain assert).

**Testing:**

Tests must verify each AC listed:

- **l1-l2-checks.AC1.2:** Create an encounter table parquet with DRG values including one with a leading space (e.g., `" X123"`). Run `run_validation`. Assert that a step with check_id="122" has n_failed > 0.

- **l1-l2-checks.AC1.3:** Create a dispensing table parquet with RxSup values including a zero. Run `run_validation`. Assert that a step with check_id="124" has n_failed > 0.

- **l1-l2-checks.AC1.4:** Create a demographic table parquet with PostalCode values including non-numeric characters (e.g., `"K1A0B1"`). Run `run_validation`. Assert that a step with check_id="128" has n_failed > 0.

- **l1-l2-checks.AC1.6:** For each of the above checks, include null values in the target column. Assert null values do NOT contribute to n_failed (na_pass=True behaviour).

- **l1-l2-checks.AC4.1:** Assert that all three checks carry the correct check_id values ("122", "124", "128") in their StepResult.

- **Clean data test:** Create data without violations for each check type. Assert n_failed == 0 for L1 check steps.

Each test class should follow the pattern in `tests/test_runner.py`: write parquet to tmp_path, create reader via `create_reader`, get schema via `get_schema`, run `run_validation`, assert on results.

For encounter/dispensing/demographic tables, include the minimum required columns to avoid nullability check failures (PatID and other required columns). Include only the columns relevant to the check being tested plus required columns.

**Verification:**

Run: `uv run pytest tests/test_l1_checks.py -v`
Expected: All tests pass.

Run: `uv run pytest tests/ -v`
Expected: Full suite passes.

**Commit:** `test: add tests for L1 per-chunk checks 122, 124, 128`
<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->

# L1 & L2 Validation Checks Implementation Plan — Phase 3

**Goal:** Implement L1 global check 111 (variable not populated) to detect columns that are entirely null across all chunks.

**Architecture:** New `check_not_populated()` function in `global_checks.py` following the existing `check_uniqueness()` / `check_sort_order()` pattern. Single pass counting non-null values per target column. Wired into `pipeline.py` after existing global checks.

**Tech Stack:** Python 3.12+, polars, pytest

**Scope:** 8 phases from original design (phase 3 of 8)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### l1-l2-checks.AC1: L1 checks detect column-level data quality issues
- **l1-l2-checks.AC1.1 Success:** Check 111 flags columns with zero non-null records as not populated
- **l1-l2-checks.AC1.5 Edge:** Columns with all nulls EXCEPT the target check-111 columns still pass (only specific columns are checked)

### l1-l2-checks.AC4: Severity levels match SAS reference
- **l1-l2-checks.AC4.1 Success:** Checks marked Fail in SAS reference produce error-level results
- **l1-l2-checks.AC4.2 Success:** Checks marked Note in SAS reference produce informational results

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Implement check_not_populated() in global_checks.py

**Verifies:** l1-l2-checks.AC1.1, l1-l2-checks.AC1.5

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py`

**Implementation:**

Add import at top of file:

```python
from scdm_qa.schemas.checks import get_not_populated_checks_for_table
```

Add new function after `check_sort_order()`:

```python
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
```

Key design decisions:
- Returns `list[StepResult]` (not single StepResult) because there may be multiple check-111 columns per table
- Uses `step_index=-1` consistent with existing global checks (renumbered when appended to results)
- No failing_rows sample since the entire column is the "failure" — there's no meaningful subset to extract
- Only checks columns listed in the registry, not all columns (AC1.5)

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: Existing tests pass.

**Commit:** `feat: add check_not_populated global check for check 111`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Wire check_not_populated into pipeline.py

**Verifies:** l1-l2-checks.AC1.1

**Files:**
- Modify: `src/scdm_qa/pipeline.py:16, 146-176`

**Implementation:**

Add import at line 16:

```python
from scdm_qa.validation.global_checks import check_sort_order, check_uniqueness, check_not_populated
```

In `_process_table()`, after the sort order check block (after line 166), add:

```python
    # L1 global check: not populated (check 111)
    not_pop_reader = create_reader(file_path, chunk_size=config.chunk_size)
    not_pop_steps = check_not_populated(schema, not_pop_reader.chunks())
    global_steps.extend(not_pop_steps)
```

This creates a fresh reader for the scan (consistent with existing global check pattern) and extends `global_steps` with the results. The existing code at lines 168-176 already handles appending `global_steps` to `validation_result`.

**Verification:**

Run: `uv run pytest tests/test_runner.py tests/test_cli.py -v`
Expected: All tests pass.

**Commit:** `feat: wire check_not_populated into pipeline`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Tests for check_not_populated

**Verifies:** l1-l2-checks.AC1.1, l1-l2-checks.AC1.5, l1-l2-checks.AC4.1, l1-l2-checks.AC4.2

**Files:**
- Modify: `tests/test_global_checks.py`

**Implementation:**

Add new test classes to the existing `tests/test_global_checks.py` file following project conventions.

**Testing:**

Tests must verify each AC listed:

- **l1-l2-checks.AC1.1:** Call `check_not_populated` with an encounter schema and chunks where DDate column is entirely null. Assert the StepResult for DDate has n_failed == total_rows and n_passed == 0.

- **l1-l2-checks.AC1.5:** Call `check_not_populated` with a demographic schema and chunks where `Race` column is all null BUT `Race` is NOT a check-111 target. Assert NO StepResult is produced for `Race`. Only `ImputedHispanic` and `ImputedRace` should produce results.

- **l1-l2-checks.AC4.1:** Assert that check_id="111" is present on all returned StepResults.

- **l1-l2-checks.AC4.2:** The severity field is carried on L1CheckDef but check_not_populated doesn't embed severity in StepResult. Verify the check_def severity is "Note" for ImputedHispanic/ImputedRace and "Fail" for PDX/DDate etc. by importing from the registry and asserting.

- **Populated column passes:** Call with chunks where target column has at least one non-null value. Assert n_failed == 0.

- **No check defs for table:** Call with a table that has no check-111 definitions. Assert empty list returned.

Create synthetic DataFrames inline (not parquet files) since `check_not_populated` takes an `Iterator[pl.DataFrame]`. Use a simple generator function or `iter([df])` to provide chunks.

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: All tests pass.

Run: `uv run pytest tests/ -v`
Expected: Full suite passes.

**Commit:** `test: add tests for check 111 (variable not populated)`
<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->

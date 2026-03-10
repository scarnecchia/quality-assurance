# L1 & L2 Validation Checks Implementation Plan — Phase 1

**Goal:** Add check_id and severity tracking to StepResult and propagate through accumulator and runner. Create L1CheckDef data model and check registry.

**Architecture:** Extend existing frozen dataclass `StepResult` with optional `check_id` and `severity` fields. Propagate through `_MutableStepAccum` and the step description tuple (4→6 elements). Create a new `L1CheckDef` dataclass in models and a new `checks.py` registry module. Update `compute_exit_code()` to respect severity levels (Note checks are informational only and don't escalate exit codes).

**Tech Stack:** Python 3.12+, polars, pointblank, pytest

**Scope:** 8 phases from original design (phase 1 of 8)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### l1-l2-checks.AC3: StepResult carries SAS CheckID cross-reference
- **l1-l2-checks.AC3.1 Success:** New checks produce StepResult with check_id set to SAS CheckID string
- **l1-l2-checks.AC3.2 Success:** Existing checks produce StepResult with check_id=None
- **l1-l2-checks.AC3.3 Failure:** check_id field does not break serialisation or reporting of existing results

### l1-l2-checks.AC4: Severity levels match SAS reference
- **l1-l2-checks.AC4.1 Success:** Checks marked Fail in SAS reference produce error-level results
- **l1-l2-checks.AC4.2 Success:** Checks marked Note in SAS reference produce informational results
- **l1-l2-checks.AC4.3 Success:** Checks marked Warn in SAS reference produce warning-level results

### l1-l2-checks.AC5: Backward compatibility
- **l1-l2-checks.AC5.1 Success:** All pre-existing validation tests pass without modification

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Add check_id and severity to StepResult and _MutableStepAccum

**Verifies:** l1-l2-checks.AC3.2, l1-l2-checks.AC3.3, l1-l2-checks.AC4.1, l1-l2-checks.AC4.2, l1-l2-checks.AC4.3, l1-l2-checks.AC5.1

**Files:**
- Modify: `src/scdm_qa/validation/results.py:8-28`
- Modify: `src/scdm_qa/validation/accumulator.py:10-19, 48-86`

**Implementation:**

In `src/scdm_qa/validation/results.py`, add `check_id: str | None = None` and `severity: str | None = None` as the last fields on `StepResult` (after `failing_rows`). Because `StepResult` is a frozen dataclass with default values, the new fields must come after all fields without defaults.

The `severity` field maps to SAS reference levels: `"Fail"` (error-level), `"Warn"` (warning-level), `"Note"` (informational). Existing checks get `None` and are treated as errors by default (preserving existing behaviour).

```python
@dataclass(frozen=True)
class StepResult:
    step_index: int
    assertion_type: str
    column: str
    description: str
    n_passed: int
    n_failed: int
    failing_rows: pl.DataFrame | None
    check_id: str | None = None
    severity: str | None = None  # "Fail" | "Warn" | "Note" | None
```

In `src/scdm_qa/validation/accumulator.py`, add both `check_id` and `severity` to `_MutableStepAccum`:

```python
@dataclass
class _MutableStepAccum:
    step_index: int
    assertion_type: str
    column: str
    description: str
    n_passed: int = 0
    n_failed: int = 0
    failing_rows: list[pl.DataFrame] = field(default_factory=list)
    failing_rows_count: int = 0
    check_id: str | None = None
    severity: str | None = None
```

Update the step description tuple type from `tuple[int, str, str, str]` to `tuple[int, str, str, str, str | None, str | None]` (6 elements: step_index, assertion_type, column, description, check_id, severity) throughout `accumulator.py`:

In `add_chunk_results` (line 37-67), change the signature and unpacking:

```python
def add_chunk_results(
    self,
    chunk_row_count: int,
    step_descriptions: list[tuple[int, str, str, str, str | None, str | None]],
    n_passed: dict[int, int],
    n_failed: dict[int, int],
    extracts: dict[int, pl.DataFrame],
) -> None:
    self._total_rows += chunk_row_count
    self._chunks_processed += 1

    for step_index, assertion_type, column, description, check_id, severity in step_descriptions:
        if step_index not in self._steps:
            self._steps[step_index] = _MutableStepAccum(
                step_index=step_index,
                assertion_type=assertion_type,
                column=column,
                description=description,
                check_id=check_id,
                severity=severity,
            )
        # ... rest unchanged
```

In `result()` (line 69-93), pass both `check_id` and `severity` when constructing `StepResult`:

```python
steps.append(
    StepResult(
        step_index=accum.step_index,
        assertion_type=accum.assertion_type,
        column=accum.column,
        description=accum.description,
        n_passed=accum.n_passed,
        n_failed=accum.n_failed,
        failing_rows=failing,
        check_id=accum.check_id,
        severity=accum.severity,
    )
)
```

**Testing:**

Tests must verify:
- l1-l2-checks.AC3.2: Existing checks produce StepResult with check_id=None — construct a StepResult without check_id kwarg, assert `.check_id is None`
- l1-l2-checks.AC3.3: check_id does not break existing result construction — construct StepResult with all 7 original positional args, assert all fields correct and check_id is None
- l1-l2-checks.AC5.1: Run existing test suite to confirm no breakage

Follow existing test patterns in `tests/test_accumulator.py`: class-based grouping, inline data, plain assert.

**Verification:**

Run: `uv run pytest tests/test_accumulator.py tests/test_runner.py tests/test_reporting.py -v`
Expected: All existing tests pass without modification.

Run: `uv run pytest tests/ -v`
Expected: Full suite passes.

**Commit:** `feat: add check_id field to StepResult and accumulator`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update step description tuple in runner.py

**Verifies:** l1-l2-checks.AC3.2, l1-l2-checks.AC5.1

**Files:**
- Modify: `src/scdm_qa/validation/runner.py:37, 110-150`

**Implementation:**

Update the type annotation at line 37 from `list[tuple[int, str, str, str]]` to `list[tuple[int, str, str, str, str | None, str | None]]`.

Update `_build_step_descriptions` return type and all tuple constructions to include 5th element (check_id) and 6th element (severity), both `None` for existing checks:

Each existing tuple like:
```python
(step_idx, "col_vals_not_null", col.name, f"{col.name} not null")
```
becomes:
```python
(step_idx, "col_vals_not_null", col.name, f"{col.name} not null", None, None)
```

Update the function signature and return type:
```python
def _build_step_descriptions(
    schema: TableSchema,
    present_columns: set[str],
) -> list[tuple[int, str, str, str, str | None, str | None]]:
```

Apply the `None, None` suffix to all 4 tuple construction sites (not_null, in_set, regex, conditional).

The step count mismatch check at lines 67-76 (`len(step_descriptions) != len(n_passed)`) does not depend on tuple width, so no change needed there.

**Note:** These are targeted edits — do NOT rewrite the entire function. Only change: (1) the type annotation, (2) the 4 tuple construction sites (append `, None, None`), and (3) the return type.

**Testing:**

Tests must verify:
- l1-l2-checks.AC3.2: _build_step_descriptions returns tuples with None as 5th element for all existing check types
- l1-l2-checks.AC5.1: Existing runner tests pass without modification

Follow existing test patterns in `tests/test_runner.py`: class-based grouping, parquet files via tmp_path, assert on ValidationResult fields.

**Verification:**

Run: `uv run pytest tests/test_runner.py tests/test_accumulator.py -v`
Expected: All tests pass.

**Commit:** `feat: extend step description tuple with check_id in runner`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Update existing accumulator tests with check_id in tuples

**Verifies:** l1-l2-checks.AC3.2, l1-l2-checks.AC5.1

**Files:**
- Modify: `tests/test_accumulator.py`

**Implementation:**

The existing tests in `tests/test_accumulator.py` pass 4-element tuples as `step_descriptions`. After Task 1, the accumulator expects 5-element tuples. Update all `step_descriptions` in existing tests to append `None` as the 5th element.

For example, in `TestAccumulatorSumsAcrossChunks.test_sums_pass_fail_counts`:

Change:
```python
step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null")],
```
To:
```python
step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null", None, None)],
```

Apply this to all step_descriptions tuples in the file (3 test classes, each with step_descriptions arguments).

Additionally, add a new test class to verify check_id propagation:

```python
class TestAccumulatorPropagatesCheckId:
    def test_check_id_none_for_standard_checks(self) -> None:
        acc = ValidationAccumulator("test", "Test Table")
        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null", None, None)],
            n_passed={1: 100},
            n_failed={1: 0},
            extracts={},
        )
        result = acc.result()
        assert result.steps[0].check_id is None

    def test_check_id_preserved_when_set(self) -> None:
        acc = ValidationAccumulator("test", "Test Table")
        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_regex", "NDC", "NDC leading spaces", "122", "Warn")],
            n_passed={1: 95},
            n_failed={1: 5},
            extracts={},
        )
        result = acc.result()
        assert result.steps[0].check_id == "122"
```

**Testing:**

Tests must verify:
- l1-l2-checks.AC3.1: StepResult with check_id="122" has that value preserved after accumulation
- l1-l2-checks.AC3.2: StepResult with check_id=None when no check_id provided

**Verification:**

Run: `uv run pytest tests/test_accumulator.py -v`
Expected: All tests pass (existing + new).

Run: `uv run pytest tests/ -v`
Expected: Full suite passes.

**Commit:** `test: update accumulator tests for check_id propagation`
<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 4-5) -->

<!-- START_TASK_4 -->
### Task 4: Create L1CheckDef dataclass in models.py

**Verifies:** None (infrastructure for subsequent phases)

**Files:**
- Modify: `src/scdm_qa/schemas/models.py:1-43`

**Implementation:**

Add `L1CheckDef` frozen dataclass after `ColumnDef` (before `TableSchema`). This dataclass represents a single L1 check definition from the SAS reference lookup tables.

```python
@dataclass(frozen=True)
class L1CheckDef:
    check_id: str           # SAS CheckID, e.g. "122"
    table_key: str          # normalised table key, e.g. "dispensing"
    column: str             # target column name, e.g. "NDC"
    check_type: str         # "leading_spaces" | "unexpected_zeros" | "non_numeric" | "not_populated"
    severity: str           # "Fail" | "Warn" | "Note"
```

The `check_type` field maps to the specific validation logic:
- `"leading_spaces"` → Check 122 (col_vals_regex `^[^ ]|^$`)
- `"unexpected_zeros"` → Check 124 (col_vals_gt value=0)
- `"non_numeric"` → Check 128 (col_vals_regex `^[0-9]*$`)
- `"not_populated"` → Check 111 (global: column entirely null)

**Verification:**

Run: `python -c "from scdm_qa.schemas.models import L1CheckDef; print(L1CheckDef('122', 'dispensing', 'NDC', 'leading_spaces', 'Warn'))"`
Expected: Prints the frozen dataclass instance without error.

**Commit:** `feat: add L1CheckDef dataclass to schema models`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Create check registry (checks.py)

**Verifies:** None (infrastructure for subsequent phases)

**Files:**
- Create: `src/scdm_qa/schemas/checks.py`

**Implementation:**

Create `src/scdm_qa/schemas/checks.py` with hardcoded `L1CheckDef` instances derived from the SAS reference lookup tables (`lkp_all_flags`, `lkp_all_l1`). This module is the single source of truth for which columns on which tables receive which L1 checks.

```python
from __future__ import annotations

from scdm_qa.schemas.models import L1CheckDef

# Check 122: Leading spaces in character fields
# Source: SAS lkp_all_l1 where CheckID=122
_CHECK_122_DEFS: tuple[L1CheckDef, ...] = (
    L1CheckDef("122", "cause_of_death", "COD", "leading_spaces", "Warn"),
    L1CheckDef("122", "encounter", "DRG", "leading_spaces", "Warn"),
    L1CheckDef("122", "inpatient_pharmacy", "NDC", "leading_spaces", "Warn"),
    L1CheckDef("122", "inpatient_pharmacy", "RxRoute", "leading_spaces", "Warn"),
    L1CheckDef("122", "inpatient_pharmacy", "RxUOM", "leading_spaces", "Warn"),
    L1CheckDef("122", "lab_result", "LOINC", "leading_spaces", "Warn"),
    L1CheckDef("122", "lab_result", "MS_Result_unit", "leading_spaces", "Warn"),
    L1CheckDef("122", "lab_result", "Norm_Range_low", "leading_spaces", "Warn"),
    L1CheckDef("122", "lab_result", "Norm_Range_high", "leading_spaces", "Warn"),
    L1CheckDef("122", "tranx", "TransCode", "leading_spaces", "Warn"),
)

# Check 124: Unexpected zeros in numeric fields
# Source: SAS lkp_all_l1 where CheckID=124
_CHECK_124_DEFS: tuple[L1CheckDef, ...] = (
    L1CheckDef("124", "dispensing", "RxSup", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "dispensing", "RxAmt", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "prescribing", "RxSup", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "prescribing", "RxAmt", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "vital_signs", "HT", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "vital_signs", "WT", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "vital_signs", "Diastolic", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "vital_signs", "Systolic", "unexpected_zeros", "Warn"),
)

# Check 128: Non-numeric characters in PostalCode
# Source: SAS lkp_all_l1 where CheckID=128
_CHECK_128_DEFS: tuple[L1CheckDef, ...] = (
    L1CheckDef("128", "demographic", "PostalCode", "non_numeric", "Warn"),
)

# Check 111: Variable not populated (entirely null column)
# Source: SAS lkp_all_flags where CheckID=111
_CHECK_111_DEFS: tuple[L1CheckDef, ...] = (
    L1CheckDef("111", "demographic", "ImputedHispanic", "not_populated", "Note"),
    L1CheckDef("111", "demographic", "ImputedRace", "not_populated", "Note"),
    L1CheckDef("111", "diagnosis", "PDX", "not_populated", "Fail"),
    L1CheckDef("111", "encounter", "DDate", "not_populated", "Fail"),
    L1CheckDef("111", "encounter", "Discharge_Disposition", "not_populated", "Fail"),
    L1CheckDef("111", "encounter", "Discharge_Status", "not_populated", "Fail"),
    L1CheckDef("111", "encounter", "Admitting_Source", "not_populated", "Fail"),
    L1CheckDef("111", "enrollment", "PlanType", "not_populated", "Note"),
    L1CheckDef("111", "enrollment", "PayerType", "not_populated", "Note"),
)

ALL_L1_CHECKS: tuple[L1CheckDef, ...] = (
    *_CHECK_122_DEFS,
    *_CHECK_124_DEFS,
    *_CHECK_128_DEFS,
    *_CHECK_111_DEFS,
)


def get_l1_checks_for_table(table_key: str) -> tuple[L1CheckDef, ...]:
    """Return all L1 check definitions for a given table key."""
    return tuple(c for c in ALL_L1_CHECKS if c.table_key == table_key)


def get_per_chunk_checks_for_table(table_key: str) -> tuple[L1CheckDef, ...]:
    """Return L1 checks that run per-chunk (122, 124, 128) for a given table."""
    return tuple(
        c for c in ALL_L1_CHECKS
        if c.table_key == table_key and c.check_type != "not_populated"
    )


def get_not_populated_checks_for_table(table_key: str) -> tuple[L1CheckDef, ...]:
    """Return L1 check-111 definitions for a given table (global check)."""
    return tuple(
        c for c in ALL_L1_CHECKS
        if c.table_key == table_key and c.check_type == "not_populated"
    )
```

**Verification:**

Run: `python -c "from scdm_qa.schemas.checks import get_l1_checks_for_table; print(len(get_l1_checks_for_table('encounter')))"`
Expected: Prints the count of L1 checks for the encounter table (should be 5: DRG leading spaces + DDate/Discharge_Disposition/Discharge_Status/Admitting_Source not populated).

Run: `uv run pytest tests/ -v`
Expected: Full suite passes (new module is imported but doesn't affect existing tests).

**Commit:** `feat: add L1 check registry with SAS reference definitions`
<!-- END_TASK_5 -->

<!-- END_SUBCOMPONENT_B -->

<!-- START_TASK_6 -->
### Task 6: Update global_checks.py StepResult construction to include check_id and severity

**Verifies:** l1-l2-checks.AC3.2, l1-l2-checks.AC4.1

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py:95-103, 142-150, 193-201`

**Implementation:**

All three `StepResult` construction sites in `global_checks.py` need `check_id=None` and `severity=None` added. Adding explicitly makes the intent clear and keeps the codebase consistent.

At lines 95-103 (duckdb uniqueness):
```python
return StepResult(
    step_index=-1,
    assertion_type="rows_distinct",
    column=", ".join(key_cols),
    description=description,
    n_passed=n_passed,
    n_failed=n_failed,
    failing_rows=failing_df if failing_df.height > 0 else None,
    check_id=None,
    severity=None,
)
```

Apply the same `check_id=None, severity=None` addition at lines 142-150 (in-memory uniqueness) and 193-201 (sort order).

**Testing:**

Tests must verify:
- l1-l2-checks.AC3.2: Global check StepResults have check_id=None

Follow existing patterns in `tests/test_global_checks.py`.

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: All existing tests pass.

**Commit:** `feat: add explicit check_id=None to global check StepResults`
<!-- END_TASK_6 -->

<!-- START_TASK_7 -->
### Task 7: Verify reporting backward compatibility

**Verifies:** l1-l2-checks.AC3.3, l1-l2-checks.AC5.1

**Files:**
- No modifications (verification only)

**Implementation:**

The reporting module (`src/scdm_qa/reporting/builder.py`) accesses `step.step_index`, `step.assertion_type`, `step.column`, `step.description`, `step.n_total`, `step.n_passed`, `step.n_failed`, `step.f_passed`, and `step.failing_rows`. It does NOT currently access `check_id`. Adding `check_id` to StepResult does not break any of these accesses.

This task is a verification-only step to confirm the reporting module works correctly with the enhanced StepResult.

**Verification:**

Run: `uv run pytest tests/test_reporting.py -v`
Expected: All reporting tests pass without modification.

Run: `uv run pytest tests/ -v`
Expected: Full test suite passes — confirming backward compatibility across all modules.

**Commit:** No commit needed (verification only).
<!-- END_TASK_7 -->

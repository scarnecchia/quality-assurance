# QA Dashboard Implementation Plan — Phase 1: Data Serialisation Layer

**Goal:** Convert validation and profiling results to a versioned JSON-serialisable schema.

**Architecture:** Pure-function serialisation module (`serialise.py`) that converts frozen dataclasses (`StepResult`, `ValidationResult`, `ColumnProfile`, `ProfilingResult`) to plain dicts. A top-level `serialise_run()` function aggregates all table results into a single dict with `schema_version`, `generated_at`, per-table data, and summary counts.

**Tech Stack:** Python 3.12+, Polars (DataFrame → list-of-dicts), datetime (ISO timestamps)

**Scope:** 6 phases from original design (phase 1 of 6)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### qa-dashboard.AC4: JSON serialisation
- **qa-dashboard.AC4.1 Success:** JSON output includes schema_version field set to "1.0"
- **qa-dashboard.AC4.2 Success:** Failing rows in JSON are truncated to max_failing_rows limit
- **qa-dashboard.AC4.3 Edge:** Serialisation handles null check_id and null severity gracefully (renders as empty/dash)

### qa-dashboard.AC5: Self-contained HTML (partial — data embedding only)
- **qa-dashboard.AC5.1 Success:** JSON data embedded in `<script type="application/json">` blocks, not external files

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->

<!-- START_TASK_1 -->
### Task 1: Create serialise.py with step and validation serialisers

**Verifies:** qa-dashboard.AC4.1, qa-dashboard.AC4.3

**Files:**
- Create: `src/scdm_qa/reporting/serialise.py`

**Implementation:**

Create `src/scdm_qa/reporting/serialise.py` with four public functions:

1. `serialise_step(step: StepResult, max_failing_rows: int) -> dict` — converts a single StepResult to a dict. Fields: `check_id` (str or None), `step_index`, `assertion_type`, `column`, `description`, `n_passed`, `n_failed`, `pass_rate` (computed from `step.f_passed`), `severity` (str or None), `failing_rows` (list of dicts, truncated to `max_failing_rows`, or empty list if None).

   For `failing_rows`: if `step.failing_rows` is not None, call `step.failing_rows.head(max_failing_rows).to_dicts()`. Otherwise, empty list.

2. `serialise_validation(result: ValidationResult, max_failing_rows: int) -> dict` — converts a ValidationResult. Fields: `table_key`, `table_name`, `total_rows`, `chunks_processed`, `steps` (list of serialised steps via `serialise_step`).

3. `serialise_profiling(result: ProfilingResult) -> dict` — converts a ProfilingResult. Fields: `table_key`, `table_name`, `total_rows`, `columns` (list of column dicts). Each column dict: `name`, `col_type`, `total_count`, `null_count`, `distinct_count`, `min_value`, `max_value`, `completeness` (from `col.completeness`), `completeness_pct` (from `col.completeness_pct`).

   Omit `value_frequencies` from serialisation — it's not needed in the dashboard.

4. `serialise_run(results: list[tuple[ValidationResult, ProfilingResult]], *, max_failing_rows: int = 500) -> dict` — top-level function. Produces:
   ```python
   {
       "schema_version": "1.0",
       "generated_at": datetime.now(UTC).isoformat(),
       "tables": {
           result.table_key: {
               "validation": serialise_validation(vr, max_failing_rows),
               "profiling": serialise_profiling(pr),
           }
           for vr, pr in results
       },
       "summary": {
           "total_checks": <sum of len(vr.steps) across all results>,
           "total_failures": <sum of vr.total_failures across all results>,
           "by_severity": {
               "Fail": <count of steps with n_failed > 0 AND severity=="Fail">,
               "Warn": <count of steps with n_failed > 0 AND severity=="Warn">,
               "Note": <count of steps with n_failed > 0 AND severity=="Note">,
               "pass": <count of steps with n_failed==0>,
           },
       },
   }
   ```

Imports needed:
```python
from __future__ import annotations

from datetime import UTC, datetime

from scdm_qa.profiling.results import ProfilingResult
from scdm_qa.validation.results import StepResult, ValidationResult
```

All functions are pure (no side effects). `serialise_run` is the only function that calls `datetime.now()`.

**Verification:**
Run: `python -c "from scdm_qa.reporting.serialise import serialise_run; print('ok')"`
Expected: `ok`

**Commit:** `feat: add serialise module for dashboard JSON schema`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Export serialise functions from reporting package

**Files:**
- Modify: `src/scdm_qa/reporting/__init__.py`

**Implementation:**

Add `serialise_run` to the reporting package's public API. Read the current `__init__.py` and add the import alongside existing exports (`save_table_report`, `save_index`, `make_report_summary`):

```python
from scdm_qa.reporting.serialise import serialise_run
```

And add `"serialise_run"` to `__all__` if one exists, or just add the import.

**Verification:**
Run: `python -c "from scdm_qa.reporting import serialise_run; print('ok')"`
Expected: `ok`

**Commit:** `feat: export serialise_run from reporting package`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Tests for serialisation layer

**Verifies:** qa-dashboard.AC4.1, qa-dashboard.AC4.2, qa-dashboard.AC4.3, qa-dashboard.AC5.1

**Files:**
- Create: `tests/test_serialise.py`

**Testing:**

Follow the existing test pattern from `tests/test_reporting.py` — module-level `_make_*` helper factories, class-based test organisation, direct assertions.

Reuse the same factory pattern: `_make_validation_result(*, with_failures=False)` and `_make_profiling_result()` that create `StepResult`, `ValidationResult`, `ColumnProfile`, `ProfilingResult` instances. Add `check_id` and `severity` fields to the factory to cover AC4.3.

Tests must verify each AC listed above:

- **qa-dashboard.AC4.1** — `serialise_run` output contains `"schema_version": "1.0"` and a valid ISO `generated_at` timestamp.
- **qa-dashboard.AC4.2** — When a StepResult has `failing_rows` with more rows than `max_failing_rows`, the serialised output truncates to exactly `max_failing_rows` rows. Test with a DataFrame of 20 rows and `max_failing_rows=5`.
- **qa-dashboard.AC4.3** — When `check_id` is None and `severity` is None, serialised step dict contains `None` for both fields (not KeyError, not omitted).
- **qa-dashboard.AC5.1** — `serialise_run` output is JSON-serialisable: `json.dumps(serialise_run(...))` succeeds without TypeError.

Additional test cases to cover:

- `serialise_step` with zero failures (no failing_rows): `failing_rows` in output is empty list.
- `serialise_step` pass_rate calculation: step with n_passed=98, n_failed=2 produces `pass_rate` close to 0.98.
- `serialise_validation` produces correct `table_key`, `table_name`, `total_rows`, `chunks_processed`.
- `serialise_profiling` with empty columns tuple (cross-table case): `columns` list is empty.
- `serialise_profiling` correctly computes `completeness` and `completeness_pct` from ColumnProfile properties.
- `serialise_run` summary counts: correct `total_checks`, `total_failures`, and `by_severity` breakdown.
- `serialise_run` with empty results list: produces valid structure with zero counts.
- `serialise_run` with multiple tables: all table keys present in `tables` dict.

Follow project testing patterns. Task-implementor generates actual test code at execution time.

**Verification:**
Run: `uv run pytest tests/test_serialise.py -v`
Expected: All tests pass

**Commit:** `test: add serialisation layer tests`

<!-- END_TASK_3 -->

<!-- END_SUBCOMPONENT_A -->

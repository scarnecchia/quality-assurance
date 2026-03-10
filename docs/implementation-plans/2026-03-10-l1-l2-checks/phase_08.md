# L1 & L2 Validation Checks Implementation Plan — Phase 8

**Goal:** Final integration pass ensuring all new checks are wired correctly, render in HTML reports, and exit codes reflect new check outcomes. Integration tests with multi-table configs exercising all new checks.

**Architecture:** Verification and integration testing phase. No new modules — validates that Phases 1-7 integrate correctly end-to-end.

**Tech Stack:** Python 3.12+, polars, pytest, typer.testing

**Scope:** 8 phases from original design (phase 8 of 8)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### l1-l2-checks.AC3: StepResult carries SAS CheckID cross-reference
- **l1-l2-checks.AC3.3 Failure:** check_id field does not break serialisation or reporting of existing results

### l1-l2-checks.AC4: Severity levels match SAS reference
- **l1-l2-checks.AC4.1 Success:** Checks marked Fail in SAS reference produce error-level results
- **l1-l2-checks.AC4.2 Success:** Checks marked Note in SAS reference produce informational results (do not escalate exit code)
- **l1-l2-checks.AC4.3 Success:** Checks marked Warn in SAS reference produce warning-level results

### l1-l2-checks.AC5: Backward compatibility
- **l1-l2-checks.AC5.1 Success:** All pre-existing validation tests pass without modification
- **l1-l2-checks.AC5.2 Success:** Pipeline exit codes correctly reflect new check outcomes alongside existing checks

### l1-l2-checks.AC6: Test coverage
- **l1-l2-checks.AC6.1 Success:** Each of the 11 new checks has at least one passing-data and one failing-data test case

---

<!-- START_TASK_1 -->
### Task 1: Verify check_id renders in HTML reports

**Verifies:** l1-l2-checks.AC3.3

**Files:**
- Modify: `src/scdm_qa/reporting/builder.py:12-24`

**Implementation:**

The current `build_validation_table()` function creates rows with columns: Step, Check, Column, Description, Total, Passed, Failed, Pass Rate. The new `check_id` field should be rendered in the report so users can cross-reference with SAS output.

Add `check_id` to the row dict in `build_validation_table()`:

```python
def build_validation_table(result: ValidationResult) -> GT:
    rows = []
    for step in result.steps:
        rows.append({
            "Step": step.step_index,
            "Check": step.assertion_type,
            "Column": step.column,
            "Description": step.description,
            "CheckID": step.check_id or "—",
            "Total": step.n_total,
            "Passed": step.n_passed,
            "Failed": step.n_failed,
            "Pass Rate": step.f_passed,
        })
    # ... rest unchanged (empty-rows fallback adds CheckID: "—")
```

Update the empty-row fallback to include `"CheckID": "—"`.

**Testing:**

Tests must verify:
- l1-l2-checks.AC3.3: Report renders correctly with check_id column. Create a StepResult with check_id="122" and one with check_id=None. Build the validation table. Assert the output HTML contains "122" and "—".

Follow existing patterns in `tests/test_reporting.py`.

**Verification:**

Run: `uv run pytest tests/test_reporting.py -v`
Expected: All tests pass (existing + new).

**Commit:** `feat: add CheckID column to HTML validation report`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Make compute_exit_code severity-aware and verify exit codes

**Verifies:** l1-l2-checks.AC5.2, l1-l2-checks.AC4.1, l1-l2-checks.AC4.2, l1-l2-checks.AC4.3

**Files:**
- Modify: `src/scdm_qa/pipeline.py:186-218`

**Implementation:**

Update `compute_exit_code()` to respect the `severity` field on `StepResult`. Steps with `severity="Note"` should never escalate the exit code — they are informational only. Steps with `severity="Warn"` contribute to exit code 1 (warnings). Steps with `severity="Fail"` (or `severity=None` for backward compat) contribute to exit code 2 when exceeding threshold.

```python
def compute_exit_code(
    outcomes: list[TableOutcome],
    *,
    error_threshold: float = 0.05,
) -> int:
    """Compute CLI exit code from pipeline outcomes.

    Severity-aware:
        - Note: informational only, never escalates exit code
        - Warn: failures contribute to exit code 1
        - Fail (or None): failures contribute to exit code 1; threshold exceedance → 2

    Returns:
        0: all checks pass (no failures in non-Note checks)
        1: some failures exist but all within threshold (warnings)
        2: processing errors or at least one Fail/None step exceeds error threshold
    """
    has_errors = any(not o.success for o in outcomes)
    if has_errors:
        return 2

    has_failures = False
    has_threshold_exceedance = False

    for o in outcomes:
        if o.validation_result is None:
            continue
        for step in o.validation_result.steps:
            # Note-severity checks are informational — skip for exit code
            if step.severity == "Note":
                continue
            if step.n_failed > 0:
                has_failures = True
                if step.f_failed > error_threshold:
                    has_threshold_exceedance = True

    if has_threshold_exceedance:
        return 2
    if has_failures:
        return 1
    return 0
```

**Testing:**

Tests must verify:
- l1-l2-checks.AC5.2: Create a `TableOutcome` with a `ValidationResult` containing a new check StepResult (check_id="226", severity="Fail") that has failures. Pass to `compute_exit_code`. Assert exit code is 1 (warnings) or 2 (threshold exceeded) depending on f_failed value.

- l1-l2-checks.AC4.2 (Note severity): Create a StepResult with severity="Note" and n_failed > 0. Assert exit code is 0 (Note checks do not escalate).

- l1-l2-checks.AC4.3 (Warn severity): Create a StepResult with severity="Warn" and n_failed > 0 but below threshold. Assert exit code is 1.

- l1-l2-checks.AC4.1 (Fail severity): Create a StepResult with severity="Fail" and f_failed > threshold. Assert exit code is 2.

- Backward compat: Create a StepResult with severity=None and n_failed > 0. Assert it behaves like Fail (exit code 1 or 2).

- Clean data: All new checks passing → exit code 0.

Follow existing test patterns. Import `compute_exit_code` and `TableOutcome` from `scdm_qa.pipeline`.

**Verification:**

Run: `uv run pytest tests/test_cli.py -v`
Expected: All CLI tests pass.

**Commit:** `feat: make compute_exit_code severity-aware; test exit code behavior`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Integration test with multi-table config

**Verifies:** l1-l2-checks.AC5.1, l1-l2-checks.AC5.2, l1-l2-checks.AC6.1

**Files:**
- Create: `tests/test_l1_l2_integration.py`

**Implementation:**

Create an integration test that exercises the full pipeline with synthetic data triggering all 11 new checks. Follow the pattern in `tests/test_cli.py` for creating temp parquet files and TOML configs.

**Testing:**

Tests must verify:

- **l1-l2-checks.AC6.1 (all 11 checks have test coverage):** Create a multi-table config with:
  - `encounter.parquet` — triggers checks 122 (DRG leading space), 111 (DDate not populated), 226 (ADate > DDate), 244 (invalid ENC combo), 245 (rate threshold)
  - `enrollment.parquet` — triggers checks 215 (overlapping spans), 216 (enrollment gaps), 226 (Enr_Start > Enr_End)
  - `demographic.parquet` — triggers checks 128 (non-numeric PostalCode), 111 (ImputedHispanic not populated)
  - `dispensing.parquet` — triggers check 124 (unexpected zeros in RxSup)
  - `cause_of_death.parquet` — triggers checks 236 (missing underlying cause), 237 (multiple underlying causes)

  Run `run_pipeline(config)`. Assert each of the 11 check IDs appears in at least one StepResult across all outcomes:
  - L1: `"111"`, `"122"`, `"124"`, `"128"`
  - L2: `"215"`, `"216"`, `"226"`, `"236"`, `"237"`, `"244"`, `"245"`

- **l1-l2-checks.AC5.1 (backward compat):** In the same integration test, assert that pre-existing checks (col_vals_not_null, col_vals_in_set, etc.) still produce results alongside new checks.

- **l1-l2-checks.AC5.2 (exit codes):** Run `compute_exit_code(outcomes)` and assert a non-zero exit code (since we're injecting violations).

- **Clean data pass:** Create a separate config with clean data (no violations). Assert all new checks pass (n_failed == 0) and exit code is 0.

Each test creates its own temporary directory with parquet files and a TOML config, following the `_make_config_and_data` pattern from `tests/test_cli.py`.

**Verification:**

Run: `uv run pytest tests/test_l1_l2_integration.py -v`
Expected: All integration tests pass.

Run: `uv run pytest tests/ -v`
Expected: Full suite passes.

**Commit:** `test: add integration tests for all 11 L1/L2 checks`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Verify full test suite passes

**Verifies:** l1-l2-checks.AC5.1

**Files:**
- No modifications

**Implementation:**

Run the complete test suite to confirm no regressions.

**Verification:**

Run: `uv run pytest tests/ -v`
Expected: ALL tests pass with zero failures.

**Commit:** No commit (verification only).
<!-- END_TASK_4 -->

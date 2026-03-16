# Cross-Table Checks & Code/CodeType Validation — Implementation Plan

**Goal:** Extend the SCDM QA pipeline with L1 code/codetype checks and L2 cross-table validation via DuckDB.

**Architecture:** Two-level validation pipeline. L1 adds code format/length checks (223, 228) to the existing per-chunk pointblank chain. L2 adds a new DuckDB-based cross-table phase that runs after all L1 processing. Both levels independently controllable via CLI flags and TOML config.

**Tech Stack:** Python 3.12+, Polars, pointblank, DuckDB, Typer, pytest

**Scope:** 7 phases from original design (phases 1–7)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### cross-table-code-checks.AC3: CLI + config phase isolation
- **cross-table-code-checks.AC3.1 Success:** `--l1-only` runs only per-table validation, skips cross-table
- **cross-table-code-checks.AC3.2 Success:** `--l2-only` runs only cross-table validation, skips per-table
- **cross-table-code-checks.AC3.3 Success:** Default (no flags) runs both L1 and L2
- **cross-table-code-checks.AC3.4 Failure:** `--l1-only --l2-only` together raises error
- **cross-table-code-checks.AC3.7 Success:** `--table` filter with L2 only runs cross-table checks involving that table
- **cross-table-code-checks.AC3.8 Success:** Exit code reflects failures from both L1 and L2 results

---

## Phase 6: Pipeline Two-Level Orchestration

This phase wires the L1 (existing per-table loop) and L2 (cross-table engine from Phase 5) into `run_pipeline()` with conditional execution based on `QAConfig.run_l1` and `QAConfig.run_l2`.

**Existing patterns to follow:**
- `run_pipeline()` in `src/scdm_qa/pipeline.py:42-117` — per-table loop producing `TableOutcome` list
- `TableOutcome` frozen dataclass at `pipeline.py:33-39` — wraps validation + profiling results
- `compute_exit_code()` at `pipeline.py:241-283` — processes outcomes for exit code, severity-aware

**Key design decision:** Cross-table results are wrapped in a synthetic `TableOutcome` with `table_key="cross_table"`. This keeps the existing data model intact — `compute_exit_code()` and reporting work without modification.

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Add conditional L1/L2 execution to run_pipeline()

**Verifies:** cross-table-code-checks.AC3.1, cross-table-code-checks.AC3.2, cross-table-code-checks.AC3.3, cross-table-code-checks.AC3.7

**Files:**
- Modify: `src/scdm_qa/pipeline.py:42-117`
- Test: `tests/test_pipeline_phases.py` (new file)

**Implementation:**

Modify `run_pipeline()` to conditionally execute L1 and L2 based on config:

```python
def run_pipeline(
    config: QAConfig,
    *,
    table_filter: str | None = None,
    profile_only: bool = False,
) -> list[TableOutcome]:
    tables = config.tables
    if table_filter:
        if table_filter not in tables:
            log.error("table not found in config", table=table_filter, available=list(tables.keys()))
            return [TableOutcome(table_key=table_filter, success=False, error=f"table {table_filter!r} not in config")]
        tables = {table_filter: tables[table_filter]}

    outcomes: list[TableOutcome] = []
    report_summaries: list[ReportSummary] = []

    # L1: Per-table validation (existing loop)
    if config.run_l1:
        for table_key, file_path in tables.items():
            # ... existing per-table processing (unchanged) ...
            pass

    # L2: Cross-table validation (new)
    if config.run_l2 and not profile_only:
        from scdm_qa.schemas.cross_table_checks import get_cross_table_checks
        from scdm_qa.validation.cross_table import run_cross_table_checks

        all_checks = get_cross_table_checks()

        # Filter checks by table if --table is specified
        if table_filter:
            from scdm_qa.schemas.cross_table_checks import get_checks_for_table
            all_checks = get_checks_for_table(table_filter)

        if all_checks:
            cross_table_steps = run_cross_table_checks(
                config, all_checks, table_filter=table_filter,
            )

            if cross_table_steps:
                cross_table_vr = ValidationResult(
                    table_key="cross_table",
                    table_name="Cross-Table Checks",
                    steps=tuple(cross_table_steps),
                    total_rows=0,
                    chunks_processed=0,
                )
                outcomes.append(TableOutcome(
                    table_key="cross_table",
                    success=True,
                    validation_result=cross_table_vr,
                ))
                # NOTE: Cross-table reporting (HTML report page + index entry) is
                # NOT wired here. The cross_table outcome has no profiling_result,
                # so the existing save_table_report() guard will skip it.
                # Phase 7 adds the reporting integration for cross-table results.

    if report_summaries:
        save_index(config.output_dir, report_summaries)

    return outcomes
```

The existing per-table loop (lines 58-112) stays intact but is wrapped in `if config.run_l1:`.

The L2 block runs after all L1 processing. It:
1. Loads cross-table check definitions
2. Filters by table if `--table` is specified (AC3.7)
3. Calls `run_cross_table_checks()` from Phase 5
4. Wraps results in a synthetic `TableOutcome` with `table_key="cross_table"`

**Why `--table` filter interaction works (AC3.7):** When `--table` is used with L2, `get_checks_for_table(table_filter)` returns only checks where the filtered table is either source or reference. If the table isn't in any cross-table check, the tuple is empty and L2 is silently skipped.

**Testing:**

Tests must verify:
- AC3.1: Config with `run_l1=True, run_l2=False` → only per-table outcomes, no "cross_table" outcome
- AC3.2: Config with `run_l1=False, run_l2=True` → only "cross_table" outcome, no per-table outcomes
- AC3.3: Config with both True → both per-table and "cross_table" outcomes present
- AC3.7: With `table_filter="diagnosis"` and `run_l2=True` → cross-table checks only include those involving diagnosis table

Use mock/monkeypatch on `run_cross_table_checks` and `_process_table` to isolate L1/L2 execution without requiring actual data files.

**Verification:**

Run: `uv run pytest tests/test_pipeline_phases.py -v`
Expected: All tests pass.

**Commit:** `feat: add conditional L1/L2 execution to run_pipeline()`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Verify exit code reflects L2 failures

**Verifies:** cross-table-code-checks.AC3.8

**Files:**
- Test: `tests/test_pipeline_phases.py` (extend)

**Implementation:**

`compute_exit_code()` already processes all `TableOutcome` objects in the outcomes list, including the synthetic `"cross_table"` outcome. No code changes needed — just verify the existing logic handles L2 results correctly.

**Testing:**

Tests must verify:
- AC3.8: Create a synthetic `TableOutcome` with `table_key="cross_table"` containing `StepResult` objects with failures. Verify `compute_exit_code()` returns appropriate exit code (1 for warnings, 2 for threshold exceedance).
- Verify that Note-severity cross-table checks don't affect exit code.
- Verify that Warn-severity cross-table checks cap at exit 1.
- Verify that Fail-severity cross-table checks with threshold exceedance return exit 2.

**Verification:**

Run: `uv run pytest tests/test_pipeline_phases.py -v`
Expected: All tests pass.

**Commit:** `test: verify exit code handles cross-table results`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Full test suite verification

**Verification:**

Run: `uv run pytest`
Expected: All tests pass. No regressions from pipeline changes.

<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

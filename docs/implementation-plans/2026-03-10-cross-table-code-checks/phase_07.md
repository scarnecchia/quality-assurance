# Cross-Table Checks & Code/CodeType Validation — Implementation Plan

**Goal:** Extend the SCDM QA pipeline with L1 code/codetype checks and L2 cross-table validation via DuckDB.

**Architecture:** Two-level validation pipeline. L1 adds code format/length checks (223, 228) to the existing per-chunk pointblank chain. L2 adds a new DuckDB-based cross-table phase that runs after all L1 processing. Both levels independently controllable via CLI flags and TOML config.

**Tech Stack:** Python 3.12+, Polars, pointblank, DuckDB, Typer, pytest

**Scope:** 7 phases from original design (phases 1–7)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### cross-table-code-checks.AC1: Cross-table validation phase
- **cross-table-code-checks.AC1.9 Success:** Cross-table results produce HTML report page and appear in index summary

---

## Phase 7: Reporting Integration

This phase integrates cross-table validation results into the HTML reporting system. Cross-table results need their own report page and an entry in the index summary.

**Existing patterns to follow:**
- `src/scdm_qa/reporting/builder.py:107-172` — `save_table_report()` assembles HTML with great-tables, writes to `{table_key}.html`
- `src/scdm_qa/reporting/index.py:69-80` — `save_index()` renders Jinja2 template with `ReportSummary` list
- `src/scdm_qa/reporting/index.py:83-106` — `make_report_summary()` factory creates `ReportSummary` TypedDict
- `src/scdm_qa/pipeline.py:69-115` — report generation happens inside the per-table loop

**Key design decision:** The `"cross_table"` `TableOutcome` from Phase 6 has `validation_result` but no `profiling_result` (cross-table checks don't profile data). `save_table_report()` needs to handle this — either by making `profiling_result` optional or by passing an empty profiling result.

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Handle cross-table outcome in pipeline reporting

**Verifies:** cross-table-code-checks.AC1.9

**Files:**
- Modify: `src/scdm_qa/pipeline.py:42-117` (the `run_pipeline()` function)
- Test: `tests/test_pipeline_phases.py` (extend from Phase 6)

**Implementation:**

After the L2 block in `run_pipeline()` (added in Phase 6), add reporting for the cross-table outcome. The cross-table outcome has `validation_result` but no `profiling_result`, so reporting needs slightly different handling.

Two approaches:
1. Create an empty `ProfilingResult` and pass it to `save_table_report()`
2. Make `save_table_report()` accept `profiling_result` as optional

Recommend approach 1 (minimal changes to existing code):

```python
# After creating cross_table outcome in L2 block:
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

    # Create empty profiling result for report generation
    empty_profiling = ProfilingResult(
        table_key="cross_table",
        table_name="Cross-Table Checks",
        total_rows=0,
        columns=(),
    )
    save_table_report(
        config.output_dir,
        "cross_table",
        cross_table_vr,
        empty_profiling,
    )
    report_summaries.append(
        make_report_summary(
            "cross_table",
            "Cross-Table Checks",
            0,  # total_rows
            len(cross_table_steps),
            sum(s.n_failed for s in cross_table_steps),
        )
    )
```

The implementor should verify whether `ProfilingResult` can be constructed with empty `columns=()` — check the `ProfilingResult` dataclass fields. If it requires non-empty data, adjust accordingly.

**Testing:**

- AC1.9: Run pipeline with `run_l2=True`, verify `cross_table.html` report file is created in output_dir. Verify index.html includes a "Cross-Table Checks" entry with link to `cross_table.html`.

**Verification:**

Run: `uv run pytest tests/test_pipeline_phases.py -v`
Expected: All tests pass.

**Commit:** `feat: generate cross-table report page and index entry`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Handle empty profiling section in cross-table report

**Verifies:** cross-table-code-checks.AC1.9

**Files:**
- Modify: `src/scdm_qa/reporting/builder.py:107-172` (potentially)
- Test: `tests/test_reporting.py` (extend)

**Implementation:**

Verify that `save_table_report()` handles the cross-table case gracefully — the profiling section should either show "No profiling data" or be omitted entirely when `profiling_result` has empty `columns`.

Check if `build_profiling_table()` handles empty columns. If it crashes or produces a bad table, add a guard:

```python
# In save_table_report(), before building profiling table:
if profiling_result.columns:
    profiling_gt = build_profiling_table(profiling_result)
    # ... render profiling HTML ...
else:
    # No profiling section for cross-table reports
    pass
```

The cross-table report should focus on the validation results (check outcomes) and failing rows sections only.

**Testing:**

- AC1.9: Call `save_table_report()` with empty profiling result → report generates without error. HTML contains validation steps but no profiling table (or shows placeholder).

**Verification:**

Run: `uv run pytest tests/test_reporting.py -v`
Expected: All tests pass.

**Commit:** `fix: handle empty profiling in cross-table report generation`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Full test suite verification

**Verification:**

Run: `uv run pytest`
Expected: All tests pass. No regressions.

<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

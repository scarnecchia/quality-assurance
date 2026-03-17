# Single-Pass Chunk-Consumer Architecture — Phase 4

**Goal:** Rewire `pipeline.py` to delegate to `TableValidator`, reducing `_process_table()` to a thin orchestrator.

**Architecture:** `_process_table()` is rewritten to construct accumulators, pass them to `TableValidator`, and unpack the result into `TableOutcome`. All DuckDB connection management, chunk iteration, and SAS-vs-Parquet branching are removed from pipeline.py — `TableValidator` owns that logic now. `run_validation()` is no longer called from production code but remains available for direct use and testing.

**Tech Stack:** Python 3.12+, structlog

**Scope:** 5 phases from original design (phase 4 of 5)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### GH-8.AC1: TableValidator encapsulates L1 lifecycle
- **GH-8.AC1.4 Success:** `_process_table()` delegates entirely to `TableValidator` and is <=30 lines

### GH-8.AC5: No regression
- **GH-8.AC5.1 Success:** Full pipeline output (`uv run scdm-qa run`) is identical pre- and post-refactor for Parquet inputs
- **GH-8.AC5.2 Success:** Full pipeline output is identical pre- and post-refactor for SAS inputs (excluding new global check results)
- **GH-8.AC5.3 Success:** Reporting pipeline produces correct HTML dashboards from refactored output
- **GH-8.AC5.4 Success:** Exit code logic is unchanged (severity-aware: 0=pass, 1=warn, 2=error)

---

<!-- START_TASK_1 -->
### Task 1: Rewrite `_process_table()` to delegate to `TableValidator`

**Verifies:** GH-8.AC1.4, GH-8.AC5.1, GH-8.AC5.2, GH-8.AC5.3, GH-8.AC5.4

**Files:**
- Modify: `src/scdm_qa/pipeline.py` (lines 1-31 imports, lines 142-283 `_process_table()`)

**Implementation:**

Rewrite `_process_table()` (currently lines 142-283, ~141 lines) to delegate to `TableValidator`. The new function should be ~20-25 lines.

**New `_process_table()` logic:**

1. File existence check (same as before)
2. Schema lookup: `get_schema(table_key)`
3. Load custom rules: `load_custom_rules(table_key, config.custom_rules_dir)`
4. Create accumulators:
   - `ProfilingAccumulator(schema)` — keyed as `"profiling"`
   - `ValidationChunkAccumulator(schema, max_failing_rows=config.max_failing_rows, custom_extend_fn=custom_extend_fn)` — keyed as `"validation"`
5. Handle `profile_only` mode: if `profile_only`, only register the profiling accumulator (skip validation)
6. Create `TableValidator(table_key, file_path, schema, config, accumulators)` and call `.run()`
7. Extract results:
   - `validation_result` from `accumulator_results["validation"]` (if present)
   - `profiling_result` from `accumulator_results["profiling"]`
   - If `global_check_steps` is non-empty, merge into `validation_result`
8. Return `TableOutcome`

**Updated imports for pipeline.py:**

Remove these imports (no longer needed):
- `duckdb`
- `create_reader` (from `scdm_qa.readers`)
- `get_date_ordering_checks_for_table`, `get_not_populated_checks_for_table` (from `scdm_qa.schemas.checks`)
- `create_connection` (from `scdm_qa.validation.duckdb_utils`)
- `check_cause_of_death`, `check_date_ordering`, `check_enc_combinations`, `check_enrollment_gaps`, `check_not_populated`, `check_overlapping_spans`, `check_sort_order`, `check_uniqueness` (from `scdm_qa.validation.global_checks`)
- `run_validation` (from `scdm_qa.validation.runner`)

Add these imports:
- `ProfilingAccumulator` (already imported)
- `ValidationChunkAccumulator` from `scdm_qa.validation.validation_chunk_accumulator`
- `TableValidator` from `scdm_qa.validation.table_validator`

**New `_process_table()` code:**

```python
def _process_table(
    table_key: str,
    file_path: Path,
    config: QAConfig,
    *,
    profile_only: bool = False,
) -> TableOutcome:
    if not file_path.exists():
        return TableOutcome(table_key=table_key, success=False, error=f"file not found: {file_path}")

    schema = get_schema(table_key)
    custom_extend_fn = load_custom_rules(table_key, config.custom_rules_dir)

    accumulators: dict[str, Any] = {
        "profiling": ProfilingAccumulator(schema),
    }
    if not profile_only:
        accumulators["validation"] = ValidationChunkAccumulator(
            schema,
            max_failing_rows=config.max_failing_rows,
            custom_extend_fn=custom_extend_fn,
        )

    tv_result = TableValidator(
        table_key, file_path, schema, config, accumulators,
        run_global_checks=not profile_only,
    ).run()

    profiling_result = tv_result.accumulator_results["profiling"]
    validation_result = tv_result.accumulator_results.get("validation")

    if validation_result is not None and tv_result.global_check_steps:
        all_steps = list(validation_result.steps) + list(tv_result.global_check_steps)
        validation_result = ValidationResult(
            table_key=validation_result.table_key,
            table_name=validation_result.table_name,
            steps=tuple(all_steps),
            total_rows=validation_result.total_rows,
            chunks_processed=validation_result.chunks_processed,
        )

    return TableOutcome(
        table_key=table_key,
        success=True,
        validation_result=validation_result,
        profiling_result=profiling_result,
    )
```

This is ~30 lines including the file-exists guard and the global-steps merge. The function's public contract (`_process_table(table_key, file_path, config, *, profile_only) -> TableOutcome`) is unchanged, so all tests that mock or call it directly continue to work.

**Testing:**

No new tests — existing tests in `test_pipeline_phases.py` and `test_l1_l2_integration.py` validate the pipeline behaviour. The function signature and return type are identical.

**Verification:**
Run: `uv run pytest tests/test_pipeline_phases.py tests/test_l1_l2_integration.py tests/test_cli.py -v`
Expected: All existing tests pass.

**Commit:** `refactor(pipeline): delegate _process_table to TableValidator (GH-8)`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Full regression check

**Verifies:** GH-8.AC5.1, GH-8.AC5.2, GH-8.AC5.3, GH-8.AC5.4

**Files:** None (no changes)

**Verification:**
Run: `uv run pytest`
Expected: Full test suite passes — no regressions. This validates that:
- Parquet pipeline output is identical (AC5.1)
- SAS pipeline output is identical, now with additional global check results (AC5.2)
- Dashboard reports generate correctly (AC5.3)
- Exit codes are unchanged (AC5.4)

**Commit:** None (no changes to commit)

<!-- END_TASK_2 -->

# GH-8 Test Requirements

Maps each acceptance criterion to automated tests or documented human verification.

---

## GH-8.AC1: TableValidator encapsulates L1 lifecycle

### GH-8.AC1.1 — `TableValidator.run()` on a Parquet file produces identical `ValidationResult` steps to the current `_process_table()` output

- **Test type:** integration
- **Test file:** `tests/test_table_validator.py`
- **Description:** Creates a `demographic` Parquet file with test data (including nulls to trigger failures), runs `TableValidator.run()` with `ValidationChunkAccumulator` and `ProfilingAccumulator`, and compares the validation result (step count, `check_id` per step, `n_passed`/`n_failed` totals) against baseline output from `run_validation()` on the same data.
- **Phase:** 3 (Task 3), with partial coverage in Phase 2 (Task 3) via `ValidationChunkAccumulator` equivalence test

### GH-8.AC1.2 — `TableValidator.run()` on a Parquet file produces identical `ProfilingResult` to the current pipeline

- **Test type:** integration
- **Test file:** `tests/test_table_validator.py`
- **Description:** Same Parquet happy-path test as AC1.1. Extracts `accumulator_results["profiling"]` from the `TableValidatorResult` and verifies it matches the profiling output from the pre-refactor pipeline.

### GH-8.AC1.3 — `TableValidator.run()` includes DuckDB global check results (uniqueness, sort order, etc.) in its output

- **Test type:** integration
- **Test file:** `tests/test_table_validator.py`
- **Description:** Parquet happy-path test verifies `global_check_steps` in the `TableValidatorResult` is non-empty and contains expected DuckDB check types (e.g., uniqueness for `demographic`).

### GH-8.AC1.4 — `_process_table()` delegates entirely to `TableValidator` and is <=30 lines

- **Test type:** human verification
- **Justification:** Line count is a code-style constraint, not a behavioural property. Automated line counting is brittle and low value.
- **Verification approach:** After Phase 4, inspect `src/scdm_qa/pipeline.py` and confirm `_process_table()` is <= 30 lines. Verify it contains no inline DuckDB connection management, chunk iteration, or SAS-vs-Parquet branching — all of that should live in `TableValidator`.

### GH-8.AC1.5 — `TableValidator.run()` propagates exceptions from DuckDB global checks without swallowing them

- **Test type:** unit
- **Test file:** `tests/test_table_validator.py`
- **Description:** Provides a schema that triggers global checks with intentionally broken data or mocks `_execute_global_checks` to raise. Asserts the exception propagates to the caller rather than being caught and silenced.

---

## GH-8.AC2: ChunkAccumulator protocol

### GH-8.AC2.1 — `ProfilingAccumulator` satisfies `ChunkAccumulator` protocol (`isinstance()` returns `True`)

- **Test type:** unit
- **Test file:** `tests/test_accumulator_protocol.py`
- **Description:** Instantiates `ProfilingAccumulator` and asserts `isinstance(acc, ChunkAccumulator)` is `True`. Also verifies a minimal conforming class passes and a non-conforming class (missing `add_chunk`) fails the `isinstance` check.
- **Phase:** 1 (Task 1)

### GH-8.AC2.2 — `ValidationChunkAccumulator` satisfies `ChunkAccumulator` protocol

- **Test type:** unit
- **Test file:** `tests/test_validation_chunk_accumulator.py`
- **Description:** Instantiates `ValidationChunkAccumulator` with a known schema and asserts `isinstance(acc, ChunkAccumulator)` is `True`.
- **Phase:** 2 (Task 3)

### GH-8.AC2.3 — A custom accumulator implementing `add_chunk()` and `result()` can be registered with `TableValidator` and receives every chunk

- **Test type:** integration
- **Test file:** `tests/test_table_validator.py`
- **Description:** Defines a `ChunkCounter` class with `add_chunk()` (records chunk heights) and `result()` (returns the list). Registers it alongside standard accumulators with `TableValidator`. After `run()`, asserts `sum(ChunkCounter.result()) == total_rows` and the list is non-empty. Repeated as a dedicated extensibility integration test in Phase 5 (Task 3).
- **Phase:** 3 (Task 3), reinforced in Phase 5 (Task 3)

### GH-8.AC2.4 — Adding a new accumulator requires zero modifications to `TableValidator` or the read loop

- **Test type:** integration
- **Test file:** `tests/test_table_validator.py`
- **Description:** Same test as AC2.3. The custom accumulator is registered via the `accumulators` dict constructor parameter — no code changes to `TableValidator` are needed. The test itself serves as proof: if it passes without modifying `TableValidator`, the criterion is met.
- **Phase:** 3 (Task 3), reinforced in Phase 5 (Task 3)

---

## GH-8.AC3: Async-capable broadcast

### GH-8.AC3.1 — Multiple accumulators process the same chunk concurrently (via `ThreadPoolExecutor`)

- **Test type:** integration
- **Test file:** `tests/test_table_validator.py`
- **Description:** Registers multiple accumulators (e.g., `ProfilingAccumulator` + custom `ChunkCounter`) and runs `TableValidator.run()`. Verifies both accumulators received all chunks. Concurrency is structural (the implementation uses `ThreadPoolExecutor` with one worker per accumulator) rather than timing-observable, so the test validates correctness of the broadcast, not thread scheduling.
- **Phase:** 3 (Task 3)

### GH-8.AC3.2 — Chunk N+1 is not dispatched until all accumulators finish chunk N

- **Test type:** human verification (partial), integration (partial)
- **Justification:** Ordering guarantees are enforced by the `as_completed` barrier in `_broadcast_chunks()`. A timing-based test would be flaky. The structural guarantee is verified by code inspection.
- **Verification approach:** Inspect `TableValidator._broadcast_chunks()` and confirm it calls `future.result()` for all futures from chunk N before entering the next iteration of the chunk loop. The integration tests in AC3.1 provide indirect evidence (correct results imply correct ordering for deterministic accumulators).
- **Test file (partial):** `tests/test_table_validator.py` — the multiple-accumulators test provides indirect validation that chunk ordering is preserved (accumulated results match expected totals).
- **Phase:** 3 (Task 3)

### GH-8.AC3.3 — If an accumulator raises during `add_chunk()`, the exception propagates to the caller (fail fast)

- **Test type:** unit
- **Test file:** `tests/test_table_validator.py`
- **Description:** Defines an accumulator whose `add_chunk()` raises `RuntimeError`. Registers it with `TableValidator` and asserts `run()` raises `RuntimeError`.
- **Phase:** 3 (Task 3)

---

## GH-8.AC4: SAS global checks enabled

### GH-8.AC4.1 — `TableValidator.run()` on a SAS file converts to temp Parquet and runs DuckDB global checks

- **Test type:** integration
- **Test file:** `tests/test_table_validator.py`
- **Description:** Uses a SAS test file (or mocks `converted_parquet` if no SAS fixture is available) to exercise the SAS branch in `_execute_all_global_checks()`. Verifies `global_check_steps` is non-empty for SAS input — unlike the pre-refactor pipeline which skips global checks for SAS.
- **Phase:** 3 (Task 3)

### GH-8.AC4.2 — Temp Parquet file is cleaned up after global checks complete (including on error)

- **Test type:** unit
- **Test file:** `tests/test_conversion.py`, `tests/test_table_validator.py`
- **Description:** Two angles: (1) `tests/test_conversion.py` verifies `converted_parquet()` context manager cleans up the temp file on normal exit and when the body raises an exception. (2) `tests/test_table_validator.py` mocks `_execute_global_checks` to raise after SAS conversion and asserts the temp Parquet file no longer exists.
- **Phase:** 1 (Task 3) for `converted_parquet` tests, Phase 3 (Task 3) for `TableValidator` error-path cleanup

### GH-8.AC4.3 — SAS global check results are identical in structure to Parquet global check results

- **Test type:** integration
- **Test file:** `tests/test_table_validator.py`
- **Description:** Runs `TableValidator` on equivalent SAS and Parquet inputs for the same table schema. Compares the structure of `global_check_steps` (same `check_id` values, same `StepResult` fields populated) between the two runs.
- **Phase:** 3 (Task 3)

### GH-8.AC4.4 — `cross_table.py` imports `convert_sas_to_parquet` from `readers/conversion.py` (shared location)

- **Test type:** unit
- **Test file:** `tests/test_conversion.py`, `tests/test_cross_table_engine.py`
- **Description:** `tests/test_conversion.py` verifies `convert_sas_to_parquet` is importable from `scdm_qa.readers.conversion`. `tests/test_cross_table_engine.py` (existing, updated imports) verifies `cross_table.py` functions still work correctly after the import path change.
- **Phase:** 1 (Tasks 3-4)

---

## GH-8.AC5: No regression

### GH-8.AC5.1 — Full pipeline output (`uv run scdm-qa run`) is identical pre- and post-refactor for Parquet inputs

- **Test type:** e2e (automated via existing test suite)
- **Test file:** `tests/test_pipeline_phases.py`, `tests/test_l1_l2_integration.py`, `tests/test_cli.py`
- **Description:** Existing pipeline and integration tests exercise the full `run_pipeline` path with Parquet inputs. If all pass after Phase 4, output is identical. Full regression check runs `uv run pytest` at the end of every phase.
- **Phase:** 4 (Task 2), 5 (Task 4)

### GH-8.AC5.2 — Full pipeline output is identical pre- and post-refactor for SAS inputs (excluding new global check results)

- **Test type:** e2e (automated via existing test suite)
- **Test file:** `tests/test_pipeline_phases.py`, `tests/test_l1_l2_integration.py`
- **Description:** Existing tests that exercise SAS input paths verify output equivalence. New global check results for SAS are additive (not replacing existing output), so existing assertions remain valid.
- **Phase:** 4 (Task 2), 5 (Task 4)

### GH-8.AC5.3 — Reporting pipeline produces correct HTML dashboards from refactored output

- **Test type:** e2e (automated via existing test suite)
- **Test file:** `tests/test_pipeline_phases.py`, `tests/test_cli.py`
- **Description:** Existing tests that verify HTML dashboard generation from pipeline output. If the pipeline produces identical `ValidationResult` and `ProfilingResult` structures, reporting is unaffected.
- **Phase:** 4 (Task 2)

### GH-8.AC5.4 — Exit code logic is unchanged (severity-aware: 0=pass, 1=warn, 2=error)

- **Test type:** e2e (automated via existing test suite)
- **Test file:** `tests/test_pipeline_phases.py`, `tests/test_cli.py`
- **Description:** Existing tests verify exit code behaviour (0 for pass, 1 for warnings within threshold, 2 for errors or threshold exceeded). The exit code logic lives in `pipeline.py` above `_process_table()` and is untouched by this refactor.
- **Phase:** 4 (Task 2)

---

## Summary

| AC | Test Type | Test File(s) | Automated? |
|---|---|---|---|
| AC1.1 | integration | `tests/test_table_validator.py`, `tests/test_validation_chunk_accumulator.py` | Yes |
| AC1.2 | integration | `tests/test_table_validator.py` | Yes |
| AC1.3 | integration | `tests/test_table_validator.py` | Yes |
| AC1.4 | human verification | `src/scdm_qa/pipeline.py` (inspect) | No |
| AC1.5 | unit | `tests/test_table_validator.py` | Yes |
| AC2.1 | unit | `tests/test_accumulator_protocol.py` | Yes |
| AC2.2 | unit | `tests/test_validation_chunk_accumulator.py` | Yes |
| AC2.3 | integration | `tests/test_table_validator.py` | Yes |
| AC2.4 | integration | `tests/test_table_validator.py` | Yes |
| AC3.1 | integration | `tests/test_table_validator.py` | Yes |
| AC3.2 | human verification + integration | `tests/test_table_validator.py` (partial), code inspection | Partial |
| AC3.3 | unit | `tests/test_table_validator.py` | Yes |
| AC4.1 | integration | `tests/test_table_validator.py` | Yes |
| AC4.2 | unit | `tests/test_conversion.py`, `tests/test_table_validator.py` | Yes |
| AC4.3 | integration | `tests/test_table_validator.py` | Yes |
| AC4.4 | unit | `tests/test_conversion.py`, `tests/test_cross_table_engine.py` | Yes |
| AC5.1 | e2e | `tests/test_pipeline_phases.py`, `tests/test_l1_l2_integration.py`, `tests/test_cli.py` | Yes |
| AC5.2 | e2e | `tests/test_pipeline_phases.py`, `tests/test_l1_l2_integration.py` | Yes |
| AC5.3 | e2e | `tests/test_pipeline_phases.py`, `tests/test_cli.py` | Yes |
| AC5.4 | e2e | `tests/test_pipeline_phases.py`, `tests/test_cli.py` | Yes |

**New test files introduced:** 4
- `tests/test_accumulator_protocol.py` (Phase 1)
- `tests/test_conversion.py` (Phase 1)
- `tests/test_validation_chunk_accumulator.py` (Phase 2)
- `tests/test_table_validator.py` (Phase 3, extended in Phase 5)

**Existing test files with updated imports:** 1
- `tests/test_cross_table_engine.py` (Phase 1, Task 4)

**Human verification required:** 2 criteria
- AC1.4 (line count / delegation structure)
- AC3.2 (chunk ordering barrier — partial, structural guarantee verified by code inspection)

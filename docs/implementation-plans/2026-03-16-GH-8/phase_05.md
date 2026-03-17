# Single-Pass Chunk-Consumer Architecture — Phase 5

**Goal:** Remove dead code from `pipeline.py`, verify no regressions, and add an extensibility integration test.

**Architecture:** After Phase 4 rewired `_process_table()` to delegate to `TableValidator`, several imports in `pipeline.py` are now orphaned. `run_validation()` in `runner.py` is retained (used by `tests/test_runner.py`, `tests/test_l1_checks.py`, and `ValidationChunkAccumulator` imports `build_step_descriptions` from the same module). The cleanup is limited to `pipeline.py` imports and the validation CLAUDE.md contract documentation.

**Tech Stack:** Python 3.12+, pytest

**Scope:** 5 phases from original design (phase 5 of 5)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### GH-8.AC2: ChunkAccumulator protocol
- **GH-8.AC2.3 Success:** A custom accumulator implementing `add_chunk()` and `result()` can be registered with `TableValidator` and receives every chunk
- **GH-8.AC2.4 Success:** Adding a new accumulator requires zero modifications to `TableValidator` or the read loop

### GH-8.AC5: No regression
- **GH-8.AC5.1 Success:** Full pipeline output (`uv run scdm-qa run`) is identical pre- and post-refactor for Parquet inputs
- **GH-8.AC5.2 Success:** Full pipeline output is identical pre- and post-refactor for SAS inputs (excluding new global check results)

---

<!-- START_TASK_1 -->
### Task 1: Remove orphaned imports from `pipeline.py`

**Verifies:** None (cleanup)

**Files:**
- Modify: `src/scdm_qa/pipeline.py` (top-level imports)

**Implementation:**

After Phase 4 rewired `_process_table()`, the following imports in `pipeline.py` are now orphaned and should be removed:

- `import duckdb` (line 6)
- `from scdm_qa.readers import create_reader` (line 12)
- `from scdm_qa.schemas.checks import get_date_ordering_checks_for_table, get_not_populated_checks_for_table` (line 15)
- `from scdm_qa.validation.duckdb_utils import create_connection` (line 17)
- All individual global check function imports from `scdm_qa.validation.global_checks` (lines 18-27)
- `from scdm_qa.validation.runner import run_validation` (line 29)

Retain:
- `from scdm_qa.validation.results import StepResult, ValidationResult` — `StepResult` may no longer be needed (check if used in the rewritten `_process_table`), but `ValidationResult` is still used. Remove `StepResult` from the import if unused.

After cleanup, verify no `F401` (unused import) warnings remain.

**Verification:**
Run: `uv run python -c "from scdm_qa.pipeline import run_pipeline, _process_table; print('imports clean')"`
Expected: Prints "imports clean" — no import errors.

Run: `uv run pytest tests/test_pipeline_phases.py -v`
Expected: All tests pass.

**Commit:** `refactor(pipeline): remove orphaned imports after TableValidator migration (GH-8)`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Update CLAUDE.md contracts

**Verifies:** None (documentation)

**Files:**
- Modify: `src/scdm_qa/validation/CLAUDE.md`

**Implementation:**

Update the validation domain CLAUDE.md to reflect the new architecture:

1. Add to **Exposes**: `ChunkAccumulator` protocol, `ValidationChunkAccumulator`, `TableValidator`, `TableValidatorResult`
2. Update **Key Decisions**:
   - Add: "TableValidator orchestrates L1 lifecycle — chunk broadcasting via ThreadPoolExecutor, DuckDB global checks, result assembly"
   - Add: "ChunkAccumulator protocol enables extensible per-chunk consumers without modifying the read loop"
   - Update: "Pipeline owns connection lifecycle" → "TableValidator owns DuckDB connection lifecycle for global checks"
   - Add: "SAS files participate in DuckDB global checks via converted_parquet() context manager"
3. Update **Key Files**:
   - Add: `accumulator_protocol.py` — `ChunkAccumulator` runtime-checkable Protocol
   - Add: `validation_chunk_accumulator.py` — `ValidationChunkAccumulator` wrapping per-chunk validation behind the protocol
   - Add: `table_validator.py` — `TableValidator` with chunk broadcasting and global checks
4. Update **Dependencies / Used by**: pipeline now delegates to TableValidator rather than calling run_validation directly
5. Update `Last verified` date to current date

**Verification:**
Read through the updated CLAUDE.md and verify it accurately reflects the new architecture.

**Commit:** `docs(validation): update CLAUDE.md contracts for TableValidator architecture (GH-8)`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add extensibility integration test

**Verifies:** GH-8.AC2.3, GH-8.AC2.4

**Files:**
- Modify: `tests/test_table_validator.py` (add test to existing file from Phase 3)

**Testing:**

Add an integration test that demonstrates accumulator extensibility by registering a trivial custom `ChunkAccumulator` alongside the standard validation + profiling accumulators.

Test must verify:
- GH-8.AC2.3: A custom accumulator with `add_chunk()` and `result()` can be registered with `TableValidator` and receives every chunk
- GH-8.AC2.4: The custom accumulator was registered without any modifications to `TableValidator` or the read loop

Test pattern:
1. Define a `ChunkCounter` class with `add_chunk(self, chunk: pl.DataFrame) -> None` (appends `chunk.height` to internal list) and `result(self) -> list[int]` (returns the list)
2. Create a `demographic` Parquet file with test data (e.g., 100 rows)
3. Register `ProfilingAccumulator`, `ValidationChunkAccumulator`, and `ChunkCounter` with `TableValidator`
4. Run `TableValidator.run()`
5. Assert `ChunkCounter.result()` returns a non-empty list
6. Assert `sum(ChunkCounter.result()) == 100` (total rows)
7. Assert `"profiling"` and `"validation"` keys also present in accumulator results

This test also serves as documentation of the extensibility pattern for future developers.

**Verification:**
Run: `uv run pytest tests/test_table_validator.py -v -k extensib`
Expected: The extensibility test passes.

**Commit:** `test(validation): add extensibility integration test for custom accumulators (GH-8)`

<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Full regression check

**Verifies:** GH-8.AC5.1, GH-8.AC5.2

**Files:** None (no changes)

**Verification:**
Run: `uv run pytest`
Expected: Full test suite passes — no regressions. All dead code removed. Extensibility demonstrated.

**Commit:** None (no changes to commit)

<!-- END_TASK_4 -->

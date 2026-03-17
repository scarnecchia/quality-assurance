# Single-Pass Chunk-Consumer Architecture — Phase 2

**Goal:** Wrap the per-chunk validation pipeline (`build_validation -> interrogate -> accumulate`) behind the `ChunkAccumulator` protocol via a new `ValidationChunkAccumulator` class.

**Architecture:** `ValidationChunkAccumulator` internalises the logic currently split between `run_validation()` in `runner.py` and `ValidationAccumulator` in `accumulator.py`. It owns `build_validation`, `interrogate`, result extraction, and accumulation — exposing only the `add_chunk(chunk)` / `result()` protocol surface. The existing `build_step_descriptions()` and `ValidationAccumulator` are composed internally, not replaced.

**Tech Stack:** Python 3.12+, pointblank, polars, structlog

**Scope:** 5 phases from original design (phase 2 of 5)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### GH-8.AC2: ChunkAccumulator protocol
- **GH-8.AC2.2 Success:** `ValidationChunkAccumulator` satisfies `ChunkAccumulator` protocol

### GH-8.AC1: TableValidator encapsulates L1 lifecycle
- **GH-8.AC1.1 Success:** `TableValidator.run()` on a Parquet file produces identical `ValidationResult` steps to the current `_process_table()` output (partial — this phase ensures the validation accumulator produces identical results to `run_validation()`)

---

<!-- START_TASK_0 -->
### Task 0: Rename `_build_step_descriptions` to `build_step_descriptions` in `runner.py`

**Verifies:** None (preparation for cross-module import)

**Files:**
- Modify: `src/scdm_qa/validation/runner.py` (lines 53, 70, 112)

**Implementation:**

Rename the private function `_build_step_descriptions` to `build_step_descriptions` (drop leading underscore) to make it a public API suitable for cross-module import. This function is used by `ValidationChunkAccumulator` (created in the next task) and should not be a private implementation detail of `runner.py`.

In `src/scdm_qa/validation/runner.py`:
- Line 53: Change `step_descriptions = _build_step_descriptions(schema, set(chunk.columns))` to `step_descriptions = build_step_descriptions(schema, set(chunk.columns))`
- Line 70: Change `num_steps_in_descriptions` variable name reference if needed (it references len, not the function)
- Line 112: Change `def _build_step_descriptions(` to `def build_step_descriptions(`

**Verification:**
Run: `uv run pytest tests/test_runner.py -v`
Expected: All tests pass (function is called internally, no tests import it by name).

**Commit:** `refactor(validation): make build_step_descriptions public for cross-module use (GH-8)`

<!-- END_TASK_0 -->

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Create `ValidationChunkAccumulator` class

**Verifies:** GH-8.AC2.2

**Files:**
- Create: `src/scdm_qa/validation/validation_chunk_accumulator.py`

**Implementation:**

Create `src/scdm_qa/validation/validation_chunk_accumulator.py` that wraps the per-chunk validation pipeline behind the `ChunkAccumulator` protocol.

The class internalises the logic from `runner.py` lines 41-94:
1. On first `add_chunk()`: build step descriptions via `build_step_descriptions(schema, present_columns)` (imported from `runner.py`)
2. Each `add_chunk()`: call `build_validation(chunk, schema, thresholds=thresholds)`, optionally `apply_custom_rules()`, then `interrogate()`, extract results, and feed to an internal `ValidationAccumulator`
3. `result()` returns `ValidationAccumulator.result()` → `ValidationResult`

```python
"""Per-chunk validation accumulator conforming to ChunkAccumulator protocol."""

from __future__ import annotations

import pointblank as pb
import polars as pl
import structlog

from scdm_qa.schemas.custom_rules import ExtendFn
from scdm_qa.schemas.models import TableSchema
from scdm_qa.schemas.validation import build_validation
from scdm_qa.validation.accumulator import ValidationAccumulator
from scdm_qa.validation.results import ValidationResult
from scdm_qa.validation.runner import build_step_descriptions

log = structlog.get_logger(__name__)


class ValidationChunkAccumulator:
    """Wraps build_validation -> interrogate -> accumulate behind ChunkAccumulator."""

    def __init__(
        self,
        schema: TableSchema,
        *,
        thresholds: pb.Thresholds | None = None,
        max_failing_rows: int = 500,
        custom_extend_fn: ExtendFn | None = None,
    ) -> None:
        self._schema = schema
        self._thresholds = thresholds
        self._max_failing_rows = max_failing_rows
        self._custom_extend_fn = custom_extend_fn
        self._accumulator = ValidationAccumulator(
            table_key=schema.table_key,
            table_name=schema.table_name,
            max_failing_rows=max_failing_rows,
        )
        self._step_descriptions: list[tuple[int, str, str, str, str | None, str | None]] = []
        self._chunk_num = 0

    def add_chunk(self, chunk: pl.DataFrame) -> None:
        self._chunk_num += 1

        log.info(
            "validating chunk",
            table=self._schema.table_key,
            chunk=self._chunk_num,
            rows=chunk.height,
        )

        if self._chunk_num == 1:
            self._step_descriptions = build_step_descriptions(
                self._schema, set(chunk.columns)
            )

        validation = build_validation(
            chunk, self._schema, thresholds=self._thresholds
        )
        if self._custom_extend_fn is not None:
            from scdm_qa.schemas.custom_rules import apply_custom_rules
            validation = apply_custom_rules(
                validation, chunk, self._custom_extend_fn
            )

        result = validation.interrogate(
            collect_extracts=True,
            extract_limit=self._max_failing_rows,
        )

        n_passed = result.n_passed()
        n_failed = result.n_failed()

        if self._chunk_num == 1:
            num_descs = len(self._step_descriptions)
            num_results = len(n_passed)
            if num_descs != num_results:
                raise ValueError(
                    f"Step count mismatch for table '{self._schema.table_key}': "
                    f"build_step_descriptions generated {num_descs} steps, "
                    f"but build_validation produced {num_results} steps in pointblank results. "
                    f"This indicates the two code paths have drifted. "
                    f"Both must iterate columns and rules in the same order."
                )

        extracts: dict[int, pl.DataFrame] = {}
        for step_idx in n_failed:
            failed_count = n_failed[step_idx]
            if failed_count is not None and failed_count > 0:
                extract = result.get_data_extracts(i=step_idx, frame=True)
                if (
                    extract is not None
                    and hasattr(extract, "height")
                    and extract.height > 0
                ):
                    extracts[step_idx] = extract

        self._accumulator.add_chunk_results(
            chunk_row_count=chunk.height,
            step_descriptions=self._step_descriptions,
            n_passed=n_passed,
            n_failed=n_failed,
            extracts=extracts,
        )

    def result(self) -> ValidationResult:
        final = self._accumulator.result()
        if final.chunks_processed == 0:
            log.warning(
                "validation found no chunks to process",
                table=self._schema.table_key,
            )
        log.info(
            "validation complete",
            table=self._schema.table_key,
            total_rows=final.total_rows,
            chunks=final.chunks_processed,
            total_failures=final.total_failures,
        )
        return final
```

**Testing:**

No tests yet — Task 3 covers all testing for this subcomponent.

**Verification:**
Run: `python -c "from scdm_qa.validation.validation_chunk_accumulator import ValidationChunkAccumulator; print('import OK')"`
Expected: Prints "import OK"

**Commit:** `feat(validation): add ValidationChunkAccumulator wrapping per-chunk pipeline (GH-8)`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Export `ValidationChunkAccumulator` from validation package

**Verifies:** GH-8.AC2.2

**Files:**
- Modify: `src/scdm_qa/validation/__init__.py`

**Implementation:**

Add the new class to `src/scdm_qa/validation/__init__.py`:

```python
from scdm_qa.validation.accumulator_protocol import ChunkAccumulator
from scdm_qa.validation.global_checks import check_sort_order, check_uniqueness
from scdm_qa.validation.results import StepResult, ValidationResult
from scdm_qa.validation.runner import run_validation
from scdm_qa.validation.validation_chunk_accumulator import ValidationChunkAccumulator

__all__ = [
    "ChunkAccumulator",
    "StepResult",
    "ValidationChunkAccumulator",
    "ValidationResult",
    "run_validation",
    "check_sort_order",
    "check_uniqueness",
]
```

**Verification:**
Run: `python -c "from scdm_qa.validation import ValidationChunkAccumulator; print('export OK')"`
Expected: Prints "export OK"

**Commit:** `feat(validation): export ValidationChunkAccumulator from package (GH-8)`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Test `ValidationChunkAccumulator` protocol conformance and result equivalence

**Verifies:** GH-8.AC2.2, GH-8.AC1.1 (partial)

**Files:**
- Create: `tests/test_validation_chunk_accumulator.py`

**Testing:**

Tests must verify:
- GH-8.AC2.2: `ValidationChunkAccumulator` satisfies `ChunkAccumulator` via `isinstance()` check
- Result equivalence: For a known table schema and test data, `ValidationChunkAccumulator` produces the same `ValidationResult` (same step count, same pass/fail counts, same check_ids, same severities) as `run_validation()` using the same reader/schema

The equivalence test pattern:
1. Create a Polars DataFrame with test data for a known table (e.g., `demographic` with some null `PatID` values to trigger failures)
2. Write to a temp Parquet file, create a reader
3. Run `run_validation(reader, schema)` → get baseline `ValidationResult`
4. Create a second reader on the same file
5. Create `ValidationChunkAccumulator(schema)`, feed it the same chunks via `add_chunk()`, call `result()` → get new `ValidationResult`
6. Compare: same number of steps, same `check_id` per step, same `n_passed`/`n_failed` per step, same `total_rows`

Test file: `tests/test_validation_chunk_accumulator.py` (new file — new module)

Additional tests:
- Zero-chunk case: accumulator with no `add_chunk()` calls returns zero-row result
- Step count mismatch detection: verify the ValueError propagates (this validates the drift detection inherited from `runner.py`)

**Verification:**
Run: `uv run pytest tests/test_validation_chunk_accumulator.py -v`
Expected: All tests pass

**Commit:** `test(validation): add ValidationChunkAccumulator conformance and equivalence tests (GH-8)`

<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_TASK_4 -->
### Task 4: Full regression check

**Verifies:** None (regression check)

**Files:** None (no changes)

**Verification:**
Run: `uv run pytest`
Expected: Full test suite passes — no regressions.

**Commit:** None (no changes to commit)

<!-- END_TASK_4 -->

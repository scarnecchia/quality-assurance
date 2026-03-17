# Single-Pass Chunk-Consumer Architecture — Phase 3

**Goal:** Implement `TableValidator` with chunk broadcasting via `ThreadPoolExecutor` and DuckDB global checks, enabling SAS files to participate in global checks.

**Architecture:** `TableValidator` encapsulates the full L1 lifecycle: reader creation, chunk broadcasting to registered `ChunkAccumulator` instances, Parquet path resolution (native or converted from SAS), DuckDB global check execution, and result assembly. Chunks are broadcast concurrently to all accumulators (one thread per accumulator), with a synchronization barrier between chunks.

**Tech Stack:** Python 3.12+, concurrent.futures.ThreadPoolExecutor, duckdb, polars, structlog

**Scope:** 5 phases from original design (phase 3 of 5)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### GH-8.AC1: TableValidator encapsulates L1 lifecycle
- **GH-8.AC1.1 Success:** `TableValidator.run()` on a Parquet file produces identical `ValidationResult` steps to the current `_process_table()` output
- **GH-8.AC1.2 Success:** `TableValidator.run()` on a Parquet file produces identical `ProfilingResult` to the current pipeline
- **GH-8.AC1.3 Success:** `TableValidator.run()` includes DuckDB global check results (uniqueness, sort order, etc.) in its output
- **GH-8.AC1.5 Failure:** `TableValidator.run()` propagates exceptions from DuckDB global checks without swallowing them

### GH-8.AC2: ChunkAccumulator protocol
- **GH-8.AC2.3 Success:** A custom accumulator implementing `add_chunk()` and `result()` can be registered with `TableValidator` and receives every chunk
- **GH-8.AC2.4 Success:** Adding a new accumulator requires zero modifications to `TableValidator` or the read loop

### GH-8.AC3: Async-capable broadcast
- **GH-8.AC3.1 Success:** Multiple accumulators process the same chunk concurrently (via `ThreadPoolExecutor`)
- **GH-8.AC3.2 Success:** Chunk N+1 is not dispatched until all accumulators finish chunk N
- **GH-8.AC3.3 Failure:** If an accumulator raises during `add_chunk()`, the exception propagates to the caller (fail fast)

### GH-8.AC4: SAS global checks enabled
- **GH-8.AC4.1 Success:** `TableValidator.run()` on a SAS file converts to temp Parquet and runs DuckDB global checks
- **GH-8.AC4.2 Success:** Temp Parquet file is cleaned up after global checks complete (including on error)
- **GH-8.AC4.3 Success:** SAS global check results are identical in structure to Parquet global check results

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Create `TableValidator` class

**Verifies:** GH-8.AC1.1, GH-8.AC1.2, GH-8.AC1.3, GH-8.AC1.5, GH-8.AC2.3, GH-8.AC2.4, GH-8.AC3.1, GH-8.AC3.2, GH-8.AC3.3, GH-8.AC4.1, GH-8.AC4.2, GH-8.AC4.3

**Files:**
- Create: `src/scdm_qa/validation/table_validator.py`

**Implementation:**

Create `src/scdm_qa/validation/table_validator.py`. The class must:

1. Accept `table_key`, `file_path`, `schema`, `config`, and a sequence of `ChunkAccumulator` instances
2. Create a `TableReader` internally via `create_reader()`
3. Broadcast chunks to all accumulators via `ThreadPoolExecutor` (one worker per accumulator)
4. Wait for all accumulators to complete each chunk before dispatching the next (barrier pattern)
5. After chunk pass, resolve Parquet path:
   - For `.parquet` files: use the file path directly
   - For `.sas7bdat` files: convert via `converted_parquet()` context manager from `readers/conversion.py`
6. Run all applicable DuckDB global checks against the resolved Parquet path
7. Assemble final results from accumulator outputs and global check step results

The global checks logic is extracted from `pipeline.py` `_process_table()` lines 179-266. The dispatch logic (which checks to run based on schema properties and table key) is identical.

```python
"""Table-level L1 validation orchestrator with concurrent chunk broadcasting."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import structlog

from scdm_qa.config import QAConfig
from scdm_qa.readers import create_reader
from scdm_qa.readers.conversion import converted_parquet
from scdm_qa.schemas.checks import (
    get_date_ordering_checks_for_table,
    get_not_populated_checks_for_table,
)
from scdm_qa.schemas.models import TableSchema
from scdm_qa.validation.accumulator_protocol import ChunkAccumulator
from scdm_qa.validation.duckdb_utils import create_connection
from scdm_qa.validation.global_checks import (
    check_cause_of_death,
    check_date_ordering,
    check_enc_combinations,
    check_enrollment_gaps,
    check_not_populated,
    check_overlapping_spans,
    check_sort_order,
    check_uniqueness,
)
from scdm_qa.validation.results import StepResult, ValidationResult

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class TableValidatorResult:
    """Combined output from all accumulators plus global checks."""

    accumulator_results: dict[str, Any]
    global_check_steps: tuple[StepResult, ...]


class TableValidator:
    """Orchestrates L1 per-table validation: chunk broadcasting + DuckDB global checks."""

    def __init__(
        self,
        table_key: str,
        file_path: Path,
        schema: TableSchema,
        config: QAConfig,
        accumulators: dict[str, ChunkAccumulator],
        *,
        run_global_checks: bool = True,
    ) -> None:
        self._table_key = table_key
        self._file_path = file_path
        self._schema = schema
        self._config = config
        self._accumulators = accumulators
        self._run_global_checks = run_global_checks

    def run(self) -> TableValidatorResult:
        reader = create_reader(self._file_path, chunk_size=self._config.chunk_size)

        self._broadcast_chunks(reader)

        global_steps = self._execute_all_global_checks() if self._run_global_checks else []

        accumulator_results = {
            name: acc.result() for name, acc in self._accumulators.items()
        }

        return TableValidatorResult(
            accumulator_results=accumulator_results,
            global_check_steps=tuple(global_steps),
        )

    def _broadcast_chunks(self, reader: Any) -> None:
        """Fan out each chunk to all accumulators concurrently."""
        acc_list = list(self._accumulators.values())
        if not acc_list:
            for _ in reader.chunks():
                pass
            return

        with ThreadPoolExecutor(max_workers=len(acc_list)) as executor:
            for chunk_num, chunk in enumerate(reader.chunks(), start=1):
                futures = {
                    executor.submit(acc.add_chunk, chunk): name
                    for name, acc in self._accumulators.items()
                }
                for future in as_completed(futures):
                    future.result()

    def _execute_all_global_checks(self) -> list[StepResult]:
        """Execute DuckDB global checks against a Parquet view of the full table."""
        global_steps: list[StepResult] = []
        is_parquet = self._file_path.suffix.lower() == ".parquet"
        is_sas = self._file_path.suffix.lower() == ".sas7bdat"

        if is_parquet:
            self._execute_global_checks(self._file_path, global_steps)
        elif is_sas:
            with converted_parquet(
                self._file_path,
                self._config.chunk_size,
                table_key=self._table_key,
            ) as parquet_path:
                self._execute_global_checks(parquet_path, global_steps)
        else:
            log.warning(
                "skipping global checks for unsupported format",
                table=self._table_key,
                file=str(self._file_path),
            )

        return global_steps

    def _execute_global_checks(
        self, parquet_path: Path, global_steps: list[StepResult]
    ) -> None:
        """Register Parquet as DuckDB view and run all applicable checks."""
        conn: duckdb.DuckDBPyConnection | None = None
        try:
            conn = create_connection(
                memory_limit=self._config.duckdb_memory_limit,
                threads=self._config.duckdb_threads,
                temp_directory=self._config.duckdb_temp_directory,
            )
            safe_path = str(parquet_path).replace("'", "''")
            conn.execute(
                f'CREATE VIEW "{self._table_key}" AS SELECT * FROM '
                f"read_parquet('{safe_path}')"
            )
            log.debug("registered global check view", table=self._table_key)

            schema = self._schema
            table_key = self._table_key
            max_rows = self._config.max_failing_rows

            if schema.unique_row:
                step = check_uniqueness(conn, table_key, schema, max_failing_rows=max_rows)
                if step is not None:
                    global_steps.append(step)

            if schema.sort_order:
                step = check_sort_order(conn, table_key, schema)
                if step is not None:
                    global_steps.append(step)

            if get_not_populated_checks_for_table(schema.table_key):
                global_steps.extend(check_not_populated(conn, table_key, schema))

            if get_date_ordering_checks_for_table(schema.table_key):
                global_steps.extend(
                    check_date_ordering(conn, table_key, schema, max_failing_rows=max_rows)
                )

            if schema.table_key == "cause_of_death":
                global_steps.extend(
                    check_cause_of_death(conn, table_key, schema, max_failing_rows=max_rows)
                )

            if schema.table_key == "enrollment":
                step = check_overlapping_spans(conn, table_key, schema, max_failing_rows=max_rows)
                if step is not None:
                    global_steps.append(step)
                step = check_enrollment_gaps(conn, table_key, schema, max_failing_rows=max_rows)
                if step is not None:
                    global_steps.append(step)

            if schema.table_key == "encounter":
                global_steps.extend(
                    check_enc_combinations(conn, table_key, schema, max_failing_rows=max_rows)
                )

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as e:
                    log.warning("failed to close DuckDB connection", error=str(e))
```

**Testing:**

No tests yet — Task 3 covers all testing for this subcomponent.

**Verification:**
Run: `python -c "from scdm_qa.validation.table_validator import TableValidator; print('import OK')"`
Expected: Prints "import OK"

**Commit:** `feat(validation): add TableValidator with chunk broadcasting and global checks (GH-8)`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Export `TableValidator` and `TableValidatorResult` from validation package

**Verifies:** None (export convenience)

**Files:**
- Modify: `src/scdm_qa/validation/__init__.py`

**Implementation:**

Add to `src/scdm_qa/validation/__init__.py`:

```python
from scdm_qa.validation.accumulator_protocol import ChunkAccumulator
from scdm_qa.validation.global_checks import check_sort_order, check_uniqueness
from scdm_qa.validation.results import StepResult, ValidationResult
from scdm_qa.validation.runner import run_validation
from scdm_qa.validation.table_validator import TableValidator, TableValidatorResult
from scdm_qa.validation.validation_chunk_accumulator import ValidationChunkAccumulator

__all__ = [
    "ChunkAccumulator",
    "StepResult",
    "TableValidator",
    "TableValidatorResult",
    "ValidationChunkAccumulator",
    "ValidationResult",
    "run_validation",
    "check_sort_order",
    "check_uniqueness",
]
```

**Verification:**
Run: `python -c "from scdm_qa.validation import TableValidator, TableValidatorResult; print('export OK')"`
Expected: Prints "export OK"

**Commit:** `feat(validation): export TableValidator from package (GH-8)`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Test `TableValidator`

**Verifies:** GH-8.AC1.1, GH-8.AC1.2, GH-8.AC1.3, GH-8.AC1.5, GH-8.AC2.3, GH-8.AC2.4, GH-8.AC3.1, GH-8.AC3.2, GH-8.AC3.3, GH-8.AC4.1, GH-8.AC4.2, GH-8.AC4.3

**Files:**
- Create: `tests/test_table_validator.py`

**Testing:**

Tests must verify all the ACs listed. Use the project's existing patterns: construct Polars DataFrames, write to `tmp_path` as Parquet, create real schemas via `get_schema()`, run `TableValidator` and compare results.

Test cases:

**Parquet happy path (GH-8.AC1.1, AC1.2, AC1.3):**
- Create a `demographic` Parquet file with test data (some nulls to trigger validation failures)
- Build `ValidationChunkAccumulator` and `ProfilingAccumulator` as accumulators
- Run `TableValidator.run()`
- Compare `accumulator_results["validation"]` against `run_validation()` output for the same data: same step count, same `check_id` per step, same `n_passed`/`n_failed` totals
- Verify profiling result matches
- Verify `global_check_steps` contains expected DuckDB checks (uniqueness for demographic)

**Multiple accumulators receive all chunks (GH-8.AC2.3, AC2.4, AC3.1, AC3.2):**
- Create a custom accumulator that records chunk heights in a list
- Register alongside `ProfilingAccumulator`
- Run `TableValidator.run()`
- Verify custom accumulator received every chunk (sum of heights == total rows)
- This also proves AC2.4: custom accumulator was registered without modifying TableValidator

**Exception propagation from accumulator (GH-8.AC3.3):**
- Create an accumulator whose `add_chunk()` raises `RuntimeError`
- Run `TableValidator.run()` and verify it raises `RuntimeError`

**DuckDB global check exception propagation (GH-8.AC1.5):**
- This is naturally tested by providing a schema that triggers global checks with intentionally bad data; verify errors propagate rather than being swallowed

**SAS happy path with global checks (GH-8.AC4.1, AC4.2, AC4.3):**
- This test requires a SAS file. If SAS test data is not available, create a Parquet file and test with a mocked `converted_parquet` to verify the SAS branch is exercised and temp cleanup occurs. Alternatively, check if `pyreadstat` is available and create a minimal SAS file programmatically.
- Verify that `global_check_steps` is populated (not empty) for SAS input — unlike current pipeline which skips global checks for SAS
- Verify temp file is cleaned up (assert the temp path no longer exists)

**Temp file cleanup on error (GH-8.AC4.2):**
- Mock `_execute_global_checks` to raise after conversion
- Verify the temp Parquet file is still cleaned up

Test file: `tests/test_table_validator.py` (new file — new module)

**Verification:**
Run: `uv run pytest tests/test_table_validator.py -v`
Expected: All tests pass

**Commit:** `test(validation): add TableValidator tests for broadcasting, global checks, SAS support (GH-8)`

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

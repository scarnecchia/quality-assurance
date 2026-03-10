# SCDM-QA Implementation Plan — Phase 4: Per-Chunk Validation

**Goal:** Pointblank validation running per-chunk with results accumulation across chunks into a single merged result set per table.

**Architecture:** A `ValidationRunner` orchestrates per-chunk validation by building a pointblank `Validate` chain from the schema's `build_validation()` and calling `interrogate()` on each chunk. A `ValidationAccumulator` collects pass/fail counts per step and captures a bounded sample of failing rows across all chunks. Because pointblank does not support constructing a `Validate` from pre-computed results, the accumulator stores its own data structures and produces a `ValidationResult` for reporting.

**Tech Stack:** Python >=3.12, pointblank 0.6.3, polars 1.38.x

**Scope:** 8 phases from original design (phase 4 of 8)

**Codebase verified:** 2026-03-09

---

## Acceptance Criteria Coverage

This phase implements and tests:

### scdm-qa.AC2: Validation rules cover the full SCDM spec
- **scdm-qa.AC2.1 Success:** Non-nullable columns with null values produce validation warnings
- **scdm-qa.AC2.2 Success:** Values outside defined enums (e.g., EncType not in {AV, ED, IP, IS, OA}) produce validation warnings
- **scdm-qa.AC2.3 Success:** Character columns exceeding spec-defined string lengths produce validation warnings
- **scdm-qa.AC2.5 Success:** Conditional rules fire correctly (e.g., DDate required when EncType ∈ {IP, IS})
- **scdm-qa.AC2.6 Success:** Generated schemas cover all 19 SCDM tables from `tables_documentation.json`

---

## Investigation Findings

**pointblank result internals (v0.6.3):**
- `interrogate()` returns same `Validate` object enriched with results
- `n_passed()` / `n_failed()` return `dict[int, int]` (step index → count)
- `f_passed()` / `f_failed()` return `dict[int, float]`
- `get_data_extracts(i=N)` returns failing rows as DataFrame for step N
- `get_tabular_report()` returns a GT (great_tables) object — can call `.as_raw_html()`
- **Cannot construct Validate from pre-computed results** — accumulator must store its own metrics
- No direct access to step metadata list (assertion type, column name, etc.)

**Design implication:** The accumulator stores `StepResult` dataclasses with accumulated counts and failing row samples. The reporting phase (Phase 7) builds HTML from these, not from pointblank's report generator.

**Codebase state:** `src/scdm_qa/schemas/` has models, parser, validation builder from Phase 2. `src/scdm_qa/readers/` has Parquet and SAS readers from Phase 3. No `validation/` directory exists yet.

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Create validation result data model

**Files:**
- Create: `src/scdm_qa/validation/__init__.py`
- Create: `src/scdm_qa/validation/results.py`

**Step 1: Create the files**

Create `src/scdm_qa/validation/results.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl


@dataclass(frozen=True)
class StepResult:
    step_index: int
    assertion_type: str  # e.g. "col_vals_not_null", "col_vals_in_set"
    column: str
    description: str
    n_passed: int
    n_failed: int
    failing_rows: pl.DataFrame | None  # bounded sample

    @property
    def n_total(self) -> int:
        return self.n_passed + self.n_failed

    @property
    def f_passed(self) -> float:
        return self.n_passed / self.n_total if self.n_total > 0 else 1.0

    @property
    def f_failed(self) -> float:
        return self.n_failed / self.n_total if self.n_total > 0 else 0.0


@dataclass(frozen=True)
class ValidationResult:
    table_key: str
    table_name: str
    steps: tuple[StepResult, ...]
    total_rows: int
    chunks_processed: int

    @property
    def all_passed(self) -> bool:
        return all(s.n_failed == 0 for s in self.steps)

    @property
    def total_failures(self) -> int:
        return sum(s.n_failed for s in self.steps)
```

Create `src/scdm_qa/validation/__init__.py`:

```python
from scdm_qa.validation.results import StepResult, ValidationResult

__all__ = ["StepResult", "ValidationResult"]
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.validation import StepResult, ValidationResult; print('validation models imported OK')"`
Expected: `validation models imported OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/validation/__init__.py src/scdm_qa/validation/results.py
git commit -m "feat: add validation result data model for accumulating per-chunk results"
```
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create ValidationAccumulator

**Files:**
- Create: `src/scdm_qa/validation/accumulator.py`

**Step 1: Create the file**

The accumulator merges pass/fail counts across chunks and captures a bounded sample of failing rows per validation step.

```python
from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from scdm_qa.validation.results import StepResult, ValidationResult


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


class ValidationAccumulator:
    def __init__(
        self,
        table_key: str,
        table_name: str,
        *,
        max_failing_rows: int = 500,
    ) -> None:
        self._table_key = table_key
        self._table_name = table_name
        self._max_failing_rows = max_failing_rows
        self._steps: dict[int, _MutableStepAccum] = {}
        self._total_rows = 0
        self._chunks_processed = 0

    def add_chunk_results(
        self,
        chunk_row_count: int,
        step_descriptions: list[tuple[int, str, str, str]],
        n_passed: dict[int, int],
        n_failed: dict[int, int],
        extracts: dict[int, pl.DataFrame],
    ) -> None:
        self._total_rows += chunk_row_count
        self._chunks_processed += 1

        for step_index, assertion_type, column, description in step_descriptions:
            if step_index not in self._steps:
                self._steps[step_index] = _MutableStepAccum(
                    step_index=step_index,
                    assertion_type=assertion_type,
                    column=column,
                    description=description,
                )

            accum = self._steps[step_index]
            accum.n_passed += n_passed.get(step_index, 0)
            accum.n_failed += n_failed.get(step_index, 0)

            if step_index in extracts and accum.failing_rows_count < self._max_failing_rows:
                extract = extracts[step_index]
                remaining = self._max_failing_rows - accum.failing_rows_count
                if extract.height > remaining:
                    extract = extract.head(remaining)
                accum.failing_rows.append(extract)
                accum.failing_rows_count += extract.height

    def result(self) -> ValidationResult:
        steps: list[StepResult] = []
        for idx in sorted(self._steps.keys()):
            accum = self._steps[idx]
            failing = None
            if accum.failing_rows:
                failing = pl.concat(accum.failing_rows)
            steps.append(
                StepResult(
                    step_index=accum.step_index,
                    assertion_type=accum.assertion_type,
                    column=accum.column,
                    description=accum.description,
                    n_passed=accum.n_passed,
                    n_failed=accum.n_failed,
                    failing_rows=failing,
                )
            )
        return ValidationResult(
            table_key=self._table_key,
            table_name=self._table_name,
            steps=tuple(steps),
            total_rows=self._total_rows,
            chunks_processed=self._chunks_processed,
        )
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.validation.accumulator import ValidationAccumulator; print('accumulator imported OK')"`
Expected: `accumulator imported OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/validation/accumulator.py
git commit -m "feat: add ValidationAccumulator for merging per-chunk validation results"
```
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Create validation runner

**Files:**
- Create: `src/scdm_qa/validation/runner.py`

**Step 1: Create the file**

The runner orchestrates per-chunk validation: reads chunks from a TableReader, runs pointblank validation on each, and accumulates results.

```python
from __future__ import annotations

from typing import TYPE_CHECKING

import pointblank as pb
import polars as pl
import structlog

from scdm_qa.readers.base import TableReader
from scdm_qa.schemas.custom_rules import ExtendFn
from scdm_qa.schemas.models import TableSchema
from scdm_qa.schemas.validation import build_validation
from scdm_qa.validation.accumulator import ValidationAccumulator
from scdm_qa.validation.results import ValidationResult

if TYPE_CHECKING:
    from scdm_qa.profiling.accumulator import ProfilingAccumulator

log = structlog.get_logger(__name__)


def run_validation(
    reader: TableReader,
    schema: TableSchema,
    *,
    thresholds: pb.Thresholds | None = None,
    max_failing_rows: int = 500,
    profiling_accumulator: "ProfilingAccumulator | None" = None,
    custom_extend_fn: ExtendFn | None = None,
) -> ValidationResult:
    accumulator = ValidationAccumulator(
        table_key=schema.table_key,
        table_name=schema.table_name,
        max_failing_rows=max_failing_rows,
    )

    for chunk_num, chunk in enumerate(reader.chunks(), start=1):
        if profiling_accumulator is not None:
            profiling_accumulator.add_chunk(chunk)

        log.info(
            "validating chunk",
            table=schema.table_key,
            chunk=chunk_num,
            rows=chunk.height,
        )

        # Pre-compute step descriptions from schema (order matches build_validation)
        if chunk_num == 1:
            step_descriptions = _build_step_descriptions(schema, set(chunk.columns))

        validation = build_validation(chunk, schema, thresholds=thresholds)
        if custom_extend_fn is not None:
            from scdm_qa.schemas.custom_rules import apply_custom_rules
            validation = apply_custom_rules(validation, chunk, custom_extend_fn)
        result = validation.interrogate(
            collect_extracts=True,
            extract_limit=max_failing_rows,
        )

        n_passed = result.n_passed()
        n_failed = result.n_failed()

        extracts: dict[int, pl.DataFrame] = {}
        for step_idx in n_failed:
            if n_failed[step_idx] > 0:
                extract = result.get_data_extracts(i=step_idx, frame=True)
                if extract is not None and hasattr(extract, "height") and extract.height > 0:
                    extracts[step_idx] = extract

        accumulator.add_chunk_results(
            chunk_row_count=chunk.height,
            step_descriptions=step_descriptions,
            n_passed=n_passed,
            n_failed=n_failed,
            extracts=extracts,
        )

    final = accumulator.result()
    log.info(
        "validation complete",
        table=schema.table_key,
        total_rows=final.total_rows,
        chunks=final.chunks_processed,
        total_failures=final.total_failures,
    )
    return final


def _build_step_descriptions(
    schema: TableSchema,
    present_columns: set[str],
) -> list[tuple[int, str, str, str]]:
    """Build step descriptions matching the order of steps in build_validation().

    Returns list of (step_index, assertion_type, column, description).
    Step indices are 1-based to match pointblank's convention.
    """
    descriptions: list[tuple[int, str, str, str]] = []
    step_idx = 0

    for col in schema.columns:
        if col.name not in present_columns:
            continue
        if not col.missing_allowed:
            step_idx += 1
            descriptions.append((step_idx, "col_vals_not_null", col.name, f"{col.name} not null"))
        if col.allowed_values is not None:
            step_idx += 1
            descriptions.append((step_idx, "col_vals_in_set", col.name, f"{col.name} in allowed values"))
        if col.col_type == "Character" and col.length is not None:
            step_idx += 1
            descriptions.append((step_idx, "col_vals_regex", col.name, f"{col.name} length <= {col.length}"))

    for rule in schema.conditional_rules:
        if rule.target_column not in present_columns:
            continue
        if rule.condition_column not in present_columns:
            continue
        if not rule.condition_values:
            continue
        step_idx += 1
        descriptions.append((
            step_idx,
            "col_vals_not_null (conditional)",
            rule.target_column,
            f"{rule.target_column} not null when {rule.condition_column} in {sorted(rule.condition_values)}",
        ))

    return descriptions
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.validation.runner import run_validation; print('runner imported OK')"`
Expected: `runner imported OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/validation/runner.py
git commit -m "feat: add validation runner orchestrating per-chunk validation"
```
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 4-5) -->
<!-- START_TASK_4 -->
### Task 4: Test ValidationAccumulator

**Verifies:** scdm-qa.AC2.1, scdm-qa.AC2.2 (accumulation correctness)

**Files:**
- Create: `tests/test_accumulator.py`

**Implementation:**

Tests verify the accumulator correctly sums pass/fail counts across multiple chunks, captures bounded failing row extracts, and produces correct ValidationResult objects.

**Testing:**
- scdm-qa.AC2.1: Accumulator sums null-check failures across chunks
- scdm-qa.AC2.2: Accumulator sums enum-check failures across chunks
- Failing row extracts are bounded by max_failing_rows
- Result properties (all_passed, total_failures) are correct

```python
from __future__ import annotations

import polars as pl

from scdm_qa.validation.accumulator import ValidationAccumulator


class TestAccumulatorSumsAcrossChunks:
    def test_sums_pass_fail_counts(self) -> None:
        acc = ValidationAccumulator("test", "Test Table")

        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null")],
            n_passed={1: 90},
            n_failed={1: 10},
            extracts={},
        )
        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null")],
            n_passed={1: 95},
            n_failed={1: 5},
            extracts={},
        )

        result = acc.result()
        assert result.total_rows == 200
        assert result.chunks_processed == 2
        assert result.steps[0].n_passed == 185
        assert result.steps[0].n_failed == 15


class TestAccumulatorBoundsFailingRows:
    def test_caps_failing_rows_at_limit(self) -> None:
        acc = ValidationAccumulator("test", "Test Table", max_failing_rows=5)

        large_extract = pl.DataFrame({"PatID": [f"P{i}" for i in range(10)]})
        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null")],
            n_passed={1: 90},
            n_failed={1: 10},
            extracts={1: large_extract},
        )

        result = acc.result()
        assert result.steps[0].failing_rows is not None
        assert result.steps[0].failing_rows.height <= 5


class TestAccumulatorMultipleSteps:
    def test_tracks_independent_steps(self) -> None:
        acc = ValidationAccumulator("test", "Test Table")

        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[
                (1, "col_vals_not_null", "PatID", "PatID not null"),
                (2, "col_vals_in_set", "Sex", "Sex in set"),
            ],
            n_passed={1: 100, 2: 95},
            n_failed={1: 0, 2: 5},
            extracts={},
        )

        result = acc.result()
        assert len(result.steps) == 2
        assert result.steps[0].n_failed == 0
        assert result.steps[1].n_failed == 5
        assert not result.all_passed
        assert result.total_failures == 5
```

**Verification:**

Run: `uv run pytest tests/test_accumulator.py -v`
Expected: All tests pass.

**Commit:** `test: add ValidationAccumulator tests for cross-chunk accumulation`
<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Test validation runner end-to-end

**Verifies:** scdm-qa.AC2.1, scdm-qa.AC2.2, scdm-qa.AC2.3, scdm-qa.AC2.5

**Files:**
- Create: `tests/test_runner.py`

**Implementation:**

Integration tests that create Parquet files with known validation issues, run them through the full runner pipeline (reader → schema → validation → accumulation), and verify that failures are detected.

**Testing:**
- scdm-qa.AC2.1: File with null PatID values in demographic table → failures detected
- scdm-qa.AC2.2: File with invalid EncType → failures detected
- scdm-qa.AC2.3: File with over-length character values → failures detected
- scdm-qa.AC2.5: File with DDate null when EncType=IP → failures detected
- Clean data file → all_passed is True

```python
from __future__ import annotations

from pathlib import Path

import polars as pl

from scdm_qa.readers import create_reader
from scdm_qa.schemas import get_schema
from scdm_qa.validation.runner import run_validation


class TestRunnerDetectsNullViolation:
    def test_null_patid_in_demographic(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "PatID": ["P1", None, "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        result = run_validation(reader, schema)

        assert not result.all_passed
        assert result.total_failures > 0


class TestRunnerDetectsInvalidEnum:
    def test_invalid_enctype(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "EncounterID": ["E1", "E2"],
            "ADate": [1000, 2000],
            "EncType": ["IP", "XX"],  # XX is invalid
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("encounter")
        result = run_validation(reader, schema)

        assert not result.all_passed


class TestRunnerMultipleChunks:
    def test_accumulates_across_chunks(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "PatID": [f"P{i}" for i in range(50)] + [None] * 5,
            "Birth_Date": list(range(55)),
            "Sex": ["F"] * 55,
            "Hispanic": ["Y"] * 55,
            "Race": ["1"] * 55,
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=20)
        schema = get_schema("demographic")
        result = run_validation(reader, schema)

        assert result.chunks_processed > 1
        assert result.total_rows == 55
        assert result.total_failures > 0
```

**Verification:**

Run: `uv run pytest tests/test_runner.py -v`
Expected: All tests pass.

**Commit:** `test: add validation runner integration tests`
<!-- END_TASK_5 -->
<!-- END_SUBCOMPONENT_B -->

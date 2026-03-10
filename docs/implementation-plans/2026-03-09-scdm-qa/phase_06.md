# SCDM-QA Implementation Plan — Phase 6: Profiling

**Goal:** Basic data profiling collected during the chunk iteration pass, producing per-column statistics alongside validation results.

**Architecture:** A `ProfilingAccumulator` collects running statistics during chunk iteration — null counts, value frequency counters, min/max for date/numeric columns, and HyperLogLog-style cardinality estimates (or exact counts for reasonable cardinality). Each chunk's stats are merged into the accumulator. The final `ProfilingResult` contains per-column profiles ready for report rendering.

**Tech Stack:** Python >=3.12, polars 1.38.x

**Scope:** 8 phases from original design (phase 6 of 8)

**Codebase verified:** 2026-03-09

---

## Acceptance Criteria Coverage

This phase implements and tests:

### scdm-qa.AC3: Basic data profiling
- **scdm-qa.AC3.1 Success:** Per-column completeness rates (% non-null) reported for all columns
- **scdm-qa.AC3.2 Success:** Value frequency distributions reported for enumerated columns
- **scdm-qa.AC3.3 Success:** Date columns show min/max range
- **scdm-qa.AC3.4 Success:** Cardinality counts reported for identifier columns

---

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Create profiling result data model

**Files:**
- Create: `src/scdm_qa/profiling/__init__.py`
- Create: `src/scdm_qa/profiling/results.py`

**Step 1: Create the files**

Create `src/scdm_qa/profiling/results.py`:

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnProfile:
    name: str
    col_type: str  # "Numeric" or "Character"
    total_count: int
    null_count: int
    distinct_count: int
    min_value: str | None  # string representation for display
    max_value: str | None
    value_frequencies: dict[str, int] | None  # for enumerated columns only

    @property
    def completeness(self) -> float:
        if self.total_count == 0:
            return 0.0
        return (self.total_count - self.null_count) / self.total_count

    @property
    def completeness_pct(self) -> float:
        return self.completeness * 100


@dataclass(frozen=True)
class ProfilingResult:
    table_key: str
    table_name: str
    total_rows: int
    columns: tuple[ColumnProfile, ...]
```

Create `src/scdm_qa/profiling/__init__.py`:

```python
from scdm_qa.profiling.results import ColumnProfile, ProfilingResult

__all__ = ["ColumnProfile", "ProfilingResult"]
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.profiling import ColumnProfile, ProfilingResult; print('profiling models imported OK')"`
Expected: `profiling models imported OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/profiling/__init__.py src/scdm_qa/profiling/results.py
git commit -m "feat: add profiling result data model"
```
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create ProfilingAccumulator

**Files:**
- Create: `src/scdm_qa/profiling/accumulator.py`

**Step 1: Create the file**

```python
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

import polars as pl

from scdm_qa.profiling.results import ColumnProfile, ProfilingResult
from scdm_qa.schemas.models import TableSchema


@dataclass
class _ColumnAccum:
    name: str
    col_type: str
    total_count: int = 0
    null_count: int = 0
    distinct_values: set[str] = field(default_factory=set)
    min_value: object = None
    max_value: object = None
    value_counter: Counter = field(default_factory=Counter)
    is_enumerated: bool = False
    max_distinct_track: int = 10_000


class ProfilingAccumulator:
    def __init__(
        self,
        schema: TableSchema,
        *,
        max_distinct_track: int = 10_000,
    ) -> None:
        self._schema = schema
        self._max_distinct_track = max_distinct_track
        self._total_rows = 0
        self._columns: dict[str, _ColumnAccum] = {}

        for col_def in schema.columns:
            is_enum = col_def.allowed_values is not None
            self._columns[col_def.name] = _ColumnAccum(
                name=col_def.name,
                col_type=col_def.col_type,
                is_enumerated=is_enum,
                max_distinct_track=max_distinct_track,
            )

    def add_chunk(self, chunk: pl.DataFrame) -> None:
        self._total_rows += chunk.height

        for col_name, accum in self._columns.items():
            if col_name not in chunk.columns:
                accum.total_count += chunk.height
                accum.null_count += chunk.height
                continue

            series = chunk[col_name]
            accum.total_count += len(series)
            accum.null_count += series.null_count()

            non_null = series.drop_nulls()
            if len(non_null) == 0:
                continue

            if accum.is_enumerated:
                for val, count in non_null.value_counts().iter_rows():
                    accum.value_counter[str(val)] += count

            if len(accum.distinct_values) < accum.max_distinct_track:
                new_vals = {str(v) for v in non_null.unique().to_list()}
                accum.distinct_values.update(new_vals)

            chunk_min = non_null.min()
            chunk_max = non_null.max()

            if accum.min_value is None or (chunk_min is not None and chunk_min < accum.min_value):
                accum.min_value = chunk_min
            if accum.max_value is None or (chunk_max is not None and chunk_max > accum.max_value):
                accum.max_value = chunk_max

    def result(self) -> ProfilingResult:
        profiles: list[ColumnProfile] = []
        for col_def in self._schema.columns:
            accum = self._columns.get(col_def.name)
            if accum is None:
                continue

            value_freqs = dict(accum.value_counter) if accum.is_enumerated and accum.value_counter else None

            profiles.append(
                ColumnProfile(
                    name=accum.name,
                    col_type=accum.col_type,
                    total_count=accum.total_count,
                    null_count=accum.null_count,
                    distinct_count=len(accum.distinct_values),
                    min_value=str(accum.min_value) if accum.min_value is not None else None,
                    max_value=str(accum.max_value) if accum.max_value is not None else None,
                    value_frequencies=value_freqs,
                )
            )

        return ProfilingResult(
            table_key=self._schema.table_key,
            table_name=self._schema.table_name,
            total_rows=self._total_rows,
            columns=tuple(profiles),
        )
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.profiling.accumulator import ProfilingAccumulator; print('profiling accumulator imported OK')"`
Expected: `profiling accumulator imported OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/profiling/accumulator.py
git commit -m "feat: add ProfilingAccumulator for streaming column statistics"
```
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Test profiling accumulator

**Verifies:** scdm-qa.AC3.1, scdm-qa.AC3.2, scdm-qa.AC3.3, scdm-qa.AC3.4

**Files:**
- Create: `tests/test_profiling.py`

**Implementation:**

Tests verify the accumulator correctly computes completeness rates, value distributions, date ranges, and cardinality across multiple chunks.

**Testing:**
- scdm-qa.AC3.1: Completeness rate correctly computed (e.g., 2/3 non-null = 66.7%)
- scdm-qa.AC3.2: Value frequency distribution for enumerated column matches actual counts
- scdm-qa.AC3.3: Min/max values tracked across chunks
- scdm-qa.AC3.4: Cardinality (distinct count) correctly accumulated across chunks

```python
from __future__ import annotations

import polars as pl

from scdm_qa.profiling.accumulator import ProfilingAccumulator
from scdm_qa.schemas import get_schema


class TestCompletenessRate:
    def test_computes_completeness_across_chunks(self) -> None:
        schema = get_schema("demographic")
        acc = ProfilingAccumulator(schema)

        acc.add_chunk(pl.DataFrame({
            "PatID": ["P1", None, "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
        }))

        result = acc.result()
        patid_profile = next(c for c in result.columns if c.name == "PatID")
        assert patid_profile.null_count == 1
        assert patid_profile.total_count == 3
        assert abs(patid_profile.completeness_pct - 66.67) < 1.0


class TestValueDistribution:
    def test_tracks_enum_frequencies(self) -> None:
        schema = get_schema("demographic")
        acc = ProfilingAccumulator(schema)

        acc.add_chunk(pl.DataFrame({
            "PatID": ["P1", "P2", "P3", "P4"],
            "Birth_Date": [1, 2, 3, 4],
            "Sex": ["F", "F", "M", "F"],
            "Hispanic": ["Y", "N", "Y", "Y"],
            "Race": ["1", "2", "1", "3"],
        }))

        result = acc.result()
        sex_profile = next(c for c in result.columns if c.name == "Sex")
        assert sex_profile.value_frequencies is not None
        assert sex_profile.value_frequencies["F"] == 3
        assert sex_profile.value_frequencies["M"] == 1


class TestDateRange:
    def test_tracks_min_max_across_chunks(self) -> None:
        schema = get_schema("demographic")
        acc = ProfilingAccumulator(schema)

        acc.add_chunk(pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }))
        acc.add_chunk(pl.DataFrame({
            "PatID": ["P3", "P4"],
            "Birth_Date": [500, 3000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }))

        result = acc.result()
        bdate = next(c for c in result.columns if c.name == "Birth_Date")
        assert bdate.min_value == "500"
        assert bdate.max_value == "3000"


class TestCardinality:
    def test_counts_distinct_across_chunks(self) -> None:
        schema = get_schema("demographic")
        acc = ProfilingAccumulator(schema)

        acc.add_chunk(pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1, 2],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }))
        acc.add_chunk(pl.DataFrame({
            "PatID": ["P2", "P3"],  # P2 is duplicate
            "Birth_Date": [3, 4],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }))

        result = acc.result()
        patid = next(c for c in result.columns if c.name == "PatID")
        assert patid.distinct_count == 3  # P1, P2, P3
```

**Verification:**

Run: `uv run pytest tests/test_profiling.py -v`
Expected: All tests pass.

**Commit:** `test: add profiling accumulator tests for completeness, distributions, ranges, and cardinality`
<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

# Single-Pass Chunk-Consumer Architecture — Phase 1

**Goal:** Establish the `ChunkAccumulator` protocol and extract SAS-to-Parquet conversion to a shared `readers/conversion.py` module.

**Architecture:** Define a `typing.Protocol` for chunk consumers following the same `@runtime_checkable` pattern used by `TableReader` in `readers/base.py`. Extract conversion functions from `validation/cross_table.py` into `readers/conversion.py` with a `converted_parquet()` context manager for automatic temp file cleanup.

**Tech Stack:** Python 3.12+, typing.Protocol, pyarrow, pyreadstat, polars, tempfile, contextlib

**Scope:** 5 phases from original design (phase 1 of 5)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### GH-8.AC2: ChunkAccumulator protocol
- **GH-8.AC2.1 Success:** `ProfilingAccumulator` satisfies `ChunkAccumulator` protocol (`isinstance()` returns `True`)

### GH-8.AC4: SAS global checks enabled
- **GH-8.AC4.4 Success:** `cross_table.py` imports `convert_sas_to_parquet` from `readers/conversion.py` (shared location)

---

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Create `ChunkAccumulator` protocol

**Verifies:** GH-8.AC2.1

**Files:**
- Create: `src/scdm_qa/validation/accumulator_protocol.py`
- Modify: `src/scdm_qa/validation/__init__.py`

**Implementation:**

Create the `ChunkAccumulator` protocol at `src/scdm_qa/validation/accumulator_protocol.py`:

```python
"""Chunk accumulator protocol for the single-pass pipeline."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class ChunkAccumulator(Protocol):
    def add_chunk(self, chunk: pl.DataFrame) -> None: ...
    def result(self) -> Any: ...
```

This mirrors the `TableReader` protocol in `readers/base.py` (lines 18-21): `@runtime_checkable`, simple method signatures, `...` bodies.

Add the export to `src/scdm_qa/validation/__init__.py`:

```python
from scdm_qa.validation.accumulator_protocol import ChunkAccumulator
from scdm_qa.validation.global_checks import check_sort_order, check_uniqueness
from scdm_qa.validation.results import StepResult, ValidationResult
from scdm_qa.validation.runner import run_validation

__all__ = [
    "ChunkAccumulator",
    "StepResult",
    "ValidationResult",
    "run_validation",
    "check_sort_order",
    "check_uniqueness",
]
```

**Testing:**

Tests must verify:
- GH-8.AC2.1: `ProfilingAccumulator` satisfies `ChunkAccumulator` via `isinstance()` check
- `ChunkAccumulator` is importable from `scdm_qa.validation`
- A minimal class with `add_chunk(self, chunk: pl.DataFrame) -> None` and `result(self) -> Any` satisfies the protocol
- A class missing `add_chunk` does NOT satisfy the protocol

Test file: `tests/test_accumulator_protocol.py` (new file — new module, one test file per module convention)

**Verification:**
Run: `uv run pytest tests/test_accumulator_protocol.py -v`
Expected: All tests pass

**Commit:** `feat(validation): add ChunkAccumulator runtime-checkable protocol (GH-8)`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Verify existing tests still pass after protocol addition

**Verifies:** None (regression check)

**Files:** None (no changes)

**Verification:**
Run: `uv run pytest`
Expected: Full test suite passes — no regressions from the new module or `__init__.py` change.

**Commit:** None (no changes to commit)

<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_SUBCOMPONENT_B (tasks 3-5) -->
<!-- START_TASK_3 -->
### Task 3: Create `readers/conversion.py` with extracted conversion functions

**Verifies:** GH-8.AC4.4

**Files:**
- Create: `src/scdm_qa/readers/conversion.py`
- Modify: `src/scdm_qa/readers/__init__.py`

**Implementation:**

Create `src/scdm_qa/readers/conversion.py` by extracting these items from `src/scdm_qa/validation/cross_table.py`:

1. `_SCDM_TYPE_MAP` (cross_table.py lines 23-26) — module-level constant
2. `build_arrow_schema()` (cross_table.py lines 29-66) — public function, keep signature identical
3. `_build_write_schema()` (cross_table.py lines 170-201) — keep private, keep signature identical
4. `_convert_sas_to_parquet()` (cross_table.py lines 204-278) → rename to `convert_sas_to_parquet()` (drop leading underscore, now public)
5. New: `converted_parquet()` context manager wrapping `convert_sas_to_parquet()` with automatic temp file cleanup

The file should have these imports (derived from what the extracted functions currently use):

```python
"""SAS-to-Parquet conversion utilities for DuckDB-based checks."""

from __future__ import annotations

import contextlib
import structlog
import tempfile
from collections.abc import Generator
from pathlib import Path

import pyarrow as pa

from scdm_qa.schemas import get_schema
from scdm_qa.schemas.models import TableSchema

log = structlog.get_logger(__name__)

_SCDM_TYPE_MAP: dict[str, pa.DataType] = {
    "Numeric": pa.float64(),
    "Character": pa.utf8(),
}
```

The `build_arrow_schema`, `_build_write_schema`, and `convert_sas_to_parquet` functions are moved verbatim from `cross_table.py`, except:
- `_convert_sas_to_parquet` → `convert_sas_to_parquet` (public name)
- No other signature or logic changes

Add the `converted_parquet()` context manager:

```python
@contextlib.contextmanager
def converted_parquet(
    sas_path: Path,
    chunk_size: int = 500_000,
    *,
    table_key: str,
) -> Generator[Path, None, None]:
    """Context manager that converts SAS to temp Parquet and cleans up on exit.

    Yields:
        Path to temporary Parquet file.
    """
    tmp_path = convert_sas_to_parquet(sas_path, chunk_size, table_key=table_key)
    try:
        yield tmp_path
    finally:
        try:
            tmp_path.unlink()
        except OSError as e:
            log.warning("failed to delete temp parquet file", path=str(tmp_path), error=str(e))
```

Update `src/scdm_qa/readers/__init__.py` to import and export the new public functions:

```python
from __future__ import annotations

from pathlib import Path

from scdm_qa.readers.base import TableMetadata, TableReader
from scdm_qa.readers.conversion import (
    build_arrow_schema,
    convert_sas_to_parquet,
    converted_parquet,
)


class UnsupportedFormatError(Exception):
    pass


def create_reader(file_path: Path, chunk_size: int = 500_000) -> TableReader:
    suffix = file_path.suffix.lower()
    if suffix == ".parquet":
        from scdm_qa.readers.parquet import ParquetReader
        return ParquetReader(file_path, chunk_size=chunk_size)
    elif suffix == ".sas7bdat":
        from scdm_qa.readers.sas import SasReader
        return SasReader(file_path, chunk_size=chunk_size)
    else:
        raise UnsupportedFormatError(
            f"unsupported file format: {suffix!r}. Expected .parquet or .sas7bdat"
        )


__all__ = [
    "TableMetadata",
    "TableReader",
    "UnsupportedFormatError",
    "build_arrow_schema",
    "convert_sas_to_parquet",
    "converted_parquet",
    "create_reader",
]
```

**Testing:**

Tests must verify:
- GH-8.AC4.4: `convert_sas_to_parquet` is importable from `scdm_qa.readers.conversion`
- `build_arrow_schema` produces identical output to the old location (use existing test data patterns from `tests/test_cross_table_engine.py`)
- `converted_parquet()` context manager yields a valid Parquet path and cleans it up on exit
- `converted_parquet()` cleans up even when the body raises an exception

Test file: `tests/test_conversion.py` (new file — new module)

**Verification:**
Run: `uv run pytest tests/test_conversion.py -v`
Expected: All tests pass

**Commit:** `feat(readers): extract SAS-to-Parquet conversion to readers/conversion.py (GH-8)`

<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Rewire `cross_table.py` to import from `readers/conversion.py`

**Verifies:** GH-8.AC4.4

**Files:**
- Modify: `src/scdm_qa/validation/cross_table.py` (lines 1-27, 100-105, 170-278)
- Modify: `tests/test_cross_table_engine.py` (lines 17-22)

**Implementation:**

In `src/scdm_qa/validation/cross_table.py`:

1. Remove the `_SCDM_TYPE_MAP` constant (lines 23-26)
2. Remove `build_arrow_schema()` function (lines 29-66)
3. Remove `_build_write_schema()` function (lines 170-201)
4. Remove `_convert_sas_to_parquet()` function (lines 204-278)
5. Add import at top: `from scdm_qa.readers.conversion import build_arrow_schema, convert_sas_to_parquet`
6. Update the call site in `run_cross_table_checks()` (currently line 101-102):
   - Change `_convert_sas_to_parquet(file_path, chunk_size=config.chunk_size, table_key=table_key)` to `convert_sas_to_parquet(file_path, config.chunk_size, table_key=table_key)`
7. Remove `pyarrow as pa` import if no longer used (check — `build_arrow_schema` was the only pa user)
8. Remove `tempfile` import (no longer used)

In `tests/test_cross_table_engine.py`, update imports (lines 17-22):
- Change: `from scdm_qa.validation.cross_table import (run_cross_table_checks, build_arrow_schema, _convert_sas_to_parquet, _build_write_schema,)`
- To: `from scdm_qa.validation.cross_table import run_cross_table_checks` and `from scdm_qa.readers.conversion import build_arrow_schema, convert_sas_to_parquet, _build_write_schema`
- Update any test references from `_convert_sas_to_parquet` to `convert_sas_to_parquet`

**Testing:**

No new tests — existing tests in `tests/test_cross_table_engine.py` validate that the functions work identically from the new import location.

**Verification:**
Run: `uv run pytest tests/test_cross_table_engine.py -v`
Expected: All tests pass — functions work identically from new location.

**Commit:** `refactor(validation): rewire cross_table.py to use readers/conversion.py (GH-8)`

<!-- END_TASK_4 -->

<!-- START_TASK_5 -->
### Task 5: Full regression check

**Verifies:** None (regression check)

**Files:** None (no changes)

**Verification:**
Run: `uv run pytest`
Expected: Full test suite passes — no regressions from extraction and rewiring.

**Commit:** None (no changes to commit)

<!-- END_TASK_5 -->
<!-- END_SUBCOMPONENT_B -->

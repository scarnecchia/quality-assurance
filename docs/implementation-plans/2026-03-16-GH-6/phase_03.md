# Streaming SAS-to-Parquet Conversion Implementation Plan

**Goal:** Replace the in-memory SAS-to-Parquet concat with streaming writes via `pyarrow.parquet.ParquetWriter`, keeping memory bounded to one chunk at a time.

**Architecture:** A canonical `pyarrow.Schema` is built from SCDM `TableSchema`/`ColumnDef` definitions (Numeric -> float64, Character -> utf8). Each SAS chunk is cast to this schema and written as a Parquet row group. Unknown tables fall back to first-chunk inference.

**Tech Stack:** Python 3.12+, pyarrow (ParquetWriter, Schema), polars (DataFrame chunks), pyreadstat (SAS reading), pytest

**Scope:** 3 phases from original design (phases 1-3)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### GH-6.AC5: Integration with real SAS data
- **GH-6.AC5.1 Success:** Real SCDM SAS file converts correctly with small chunk_size forcing multi-chunk writes
- **GH-6.AC5.2 Success:** Output row count matches source SAS row count
- **GH-6.AC5.3 Success:** Integration test skips cleanly when data directory is unavailable

### GH-6.AC4: Backward compatibility
- **GH-6.AC4.1 Success:** All existing cross-table validation tests pass unchanged
- **GH-6.AC4.2 Success:** Cross-table checks produce identical results for Parquet inputs (no conversion path exercised)

---

## Phase 3: Integration Testing with Real SAS Data

<!-- START_TASK_1 -->
### Task 1: Add integration test for real SAS conversion

**Verifies:** GH-6.AC5.1, GH-6.AC5.2, GH-6.AC5.3

**Files:**
- Create: `tests/test_sas_streaming_integration.py`

**Implementation:**

Create a new test file for integration tests that exercise the full streaming conversion path against real SCDM SAS data files. The tests use an environment variable `SCDM_SAS_DATA_DIR` to locate a directory containing SAS7BDAT files named by SCDM table key (e.g., `demographic.sas7bdat`, `enrollment.sas7bdat`).

The test module uses `pytest.mark.skipif` to skip cleanly when:
1. The `SCDM_SAS_DATA_DIR` environment variable is not set
2. The directory it points to does not exist
3. No `.sas7bdat` files are found in the directory

Each test:
1. Picks a known SCDM table (e.g., `demographic`)
2. Calls `_convert_sas_to_parquet(path, chunk_size=100, table_key=table_key)` with a very small chunk_size to force multi-chunk writes
3. Reads back the output Parquet and verifies:
   - Row count matches the source (read via pyreadstat to get source row count)
   - Schema has canonical types for SCDM spec columns
   - Parquet has multiple row groups (given small chunk_size)
4. Cleans up the temp parquet file

The skip condition and data discovery:

```python
import os
import pytest
from pathlib import Path

_DATA_DIR = os.environ.get("SCDM_SAS_DATA_DIR", "")
_DATA_PATH = Path(_DATA_DIR) if _DATA_DIR else None
_HAS_DATA = _DATA_PATH is not None and _DATA_PATH.is_dir() and any(_DATA_PATH.glob("*.sas7bdat"))

pytestmark = pytest.mark.skipif(
    not _HAS_DATA,
    reason="SCDM_SAS_DATA_DIR not set or contains no SAS files",
)
```

Test class structure:

```python
class TestSasStreamingIntegration:
    def test_real_sas_converts_with_correct_row_count(self) -> None: ...
    def test_real_sas_produces_multiple_row_groups(self) -> None: ...
    def test_real_sas_output_schema_matches_canonical_types(self) -> None: ...
```

Each test should discover the first available `.sas7bdat` file and derive the `table_key` from the filename stem (e.g., `demographic.sas7bdat` -> `"demographic"`). Use `pyreadstat.read_sas7bdat` to get the source row count for comparison. Use `pyarrow.parquet.read_metadata` to check row group count without loading data into memory.

**Testing:**

- GH-6.AC5.1: Convert a real SAS file with `chunk_size=100`. Read back parquet, verify it's valid and has correct schema types.
- GH-6.AC5.2: Compare output parquet row count to source SAS row count (via pyreadstat).
- GH-6.AC5.3: Run tests without `SCDM_SAS_DATA_DIR` set. Verify all tests in this module are skipped with clear reason message.

**Verification:**

Without data:
Run: `uv run pytest tests/test_sas_streaming_integration.py -v`
Expected: All tests skipped with reason "SCDM_SAS_DATA_DIR not set or contains no SAS files"

With data (if available):
Run: `SCDM_SAS_DATA_DIR=/path/to/sas/files uv run pytest tests/test_sas_streaming_integration.py -v`
Expected: All tests pass

**Commit:** `test: add integration tests for real SAS streaming conversion (GH-6)`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Final full regression test

**Verifies:** GH-6.AC4.1, GH-6.AC4.2

**Files:** None (read-only verification)

**Verification:**

Run: `uv run pytest`
Expected: All tests pass (416+ original + new Phase 1/2/3 tests). Integration tests skip if no SAS data available. No regressions.

**Commit:** No commit needed — verification only
<!-- END_TASK_2 -->

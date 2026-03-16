# DuckDB Global Checks Migration - Phase 6: Cleanup and Pipeline Integration Tests

**Goal:** Remove all remaining dead code (chunk-based reader creation for global checks, unused imports, SortViolation TypedDict) and add a pipeline-level integration test for SAS file skip behaviour.

**Architecture:** This is a cleanup phase. No new functionality. All check functions were migrated in Phases 2-5. This phase removes orphaned code and verifies the complete pipeline works end-to-end.

**Tech Stack:** Python 3.12+, DuckDB, polars, pytest

**Scope:** 6 phases from original design (phase 6 of 6)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

### GH-7.AC1: All global checks execute via DuckDB SQL
- **GH-7.AC1.2 Success:** No `pl.concat()` calls remain in any global check code path

### GH-7.AC6: SAS files handled correctly
- **GH-7.AC6.1 Success:** Pipeline skips global checks for SAS files with a logged warning
- **GH-7.AC6.2 Success:** SAS files do not cause errors or crashes — graceful skip

### GH-7.AC7: Results backward compatible
- **GH-7.AC7.1 Success:** All check IDs unchanged (211, 102, 111, 226, 236, 237, 215, 216, 244, 245)
- **GH-7.AC7.2 Success:** All severities unchanged per check
- **GH-7.AC7.3 Success:** StepResult shape unchanged (same fields, same types)
- **GH-7.AC7.4 Success:** n_passed + n_failed counts match expected values for identical test data

---

<!-- START_TASK_1 -->
### Task 1: Remove dead code from global_checks.py

**Verifies:** GH-7.AC1.2

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py`

**Implementation:**

Remove the following from `global_checks.py`:

1. **`SortViolation` TypedDict** (currently lines 42-44) — was used by the chunk-boundary sort order check, no longer needed
2. **`TypedDict` import** from `typing` — no longer used after removing SortViolation (keep `Iterator` only if still used; if all chunk iterators are gone, remove `Iterator` too)
3. **`Path` import** from `pathlib` — no longer needed since functions no longer accept `file_path`
4. **`Iterator` import** from `typing` — no longer needed since functions no longer accept chunk iterators
5. **Any remaining `pl.concat()` calls** — verify none exist (should already be gone from Phases 2-5)

After cleanup, the imports at the top of `global_checks.py` should be:

```python
from __future__ import annotations

import duckdb
import polars as pl
import structlog

from scdm_qa.schemas.checks import (
    ENC_COMBINATION_RULES,
    ENC_RATE_THRESHOLDS,
    get_date_ordering_checks_for_table,
    get_not_populated_checks_for_table,
)
from scdm_qa.schemas.models import TableSchema
from scdm_qa.validation.results import StepResult
```

Note: `polars` is still needed for the `.pl()` method on DuckDB query results and for `failing_rows` type annotations.

**Verification:**
Run: `uv run pytest`
Expected: All tests pass. No import errors.

**Commit:** `chore(global-checks): remove dead code — SortViolation, chunk iterators, unused imports`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Remove dead reader creation from pipeline.py

**Verifies:** GH-7.AC1.2

**Files:**
- Modify: `src/scdm_qa/pipeline.py`

**Implementation:**

By this point in the migration, all 7 `create_reader()` calls for global checks should already have been removed in Phases 2-5. Verify that no `create_reader` calls remain in the global checks section of `_process_table()`.

The remaining `create_reader` calls should only be:
1. Line 151: `reader = create_reader(file_path, chunk_size=config.chunk_size)` — the main validation reader (not a global check)

If any stale global check reader lines remain, remove them.

Also verify that the `create_reader` import is still needed (it is — for the main validation reader on line 151).

**Verification:**
Run: `uv run pytest`
Expected: All tests pass.

**Commit:** `chore(pipeline): verify no stale reader creation for global checks`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add pipeline integration test for SAS file skip behaviour

**Verifies:** GH-7.AC6.1, GH-7.AC6.2, GH-7.AC7.4

**Files:**
- Modify: `tests/test_pipeline_phases.py` (or create if needed — check if it exists first)

**Implementation:**

Add an integration test that verifies the pipeline correctly skips global checks for SAS files. This test should:

1. Create a minimal valid SAS-format file path (a dummy `.sas7bdat` file — the skip happens before any DuckDB read, so validity doesn't matter for the gate itself, but the validation runner will still need to read it)
2. Call `_process_table()` with the SAS file path
3. Verify that `TableOutcome.success` is `True`
4. Verify that no global check `StepResult`s are present in the result (only per-chunk validation results)
5. Verify that a warning was logged about skipping global checks

Since creating valid SAS files requires `pyreadstat.write_sas7bdat` which may not be available, an alternative approach is to mock the validation runner and verify the global checks gate directly:

The test should focus on verifying the `is_parquet` gate in `_process_table()`. The approach: create a dummy file with `.sas7bdat` extension, mock `run_validation` to return a canned `ValidationResult` (since we can't read a fake SAS file), and verify no DuckDB connection was created and no global check StepResults appear.

Test skeleton:

```python
from unittest.mock import patch, MagicMock
from scdm_qa.pipeline import _process_table
from scdm_qa.validation.results import ValidationResult

class TestSASFileGlobalCheckSkip:
    def test_sas_file_skips_global_checks(self, tmp_path: Path) -> None:
        """GH-7.AC6: Pipeline skips global checks for SAS files."""
        # Create a dummy .sas7bdat file (content doesn't matter for gate test)
        sas_path = tmp_path / "demographic.sas7bdat"
        sas_path.write_bytes(b"dummy")

        schema = get_schema("demographic")

        # Mock run_validation since we can't actually read a fake SAS file
        mock_vr = ValidationResult(
            table_key="demographic",
            table_name="Demographic",
            steps=(),
            total_rows=0,
            chunks_processed=0,
        )
        with patch("scdm_qa.pipeline.run_validation", return_value=mock_vr), \
             patch("scdm_qa.pipeline.create_reader") as mock_reader, \
             patch("scdm_qa.pipeline.create_connection") as mock_conn:
            # Set up mock reader to return empty chunks
            mock_reader.return_value.chunks.return_value = iter([])

            config = QAConfig(tables={"demographic": sas_path})
            outcome = _process_table("demographic", sas_path, config)

            assert outcome.success is True
            # create_connection should NOT have been called (SAS gate)
            mock_conn.assert_not_called()

            # No global check StepResults in the result
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    assert step.check_id not in {
                        "211", "102", "111", "226", "236", "237",
                        "215", "216", "244", "245",
                    }

    def test_parquet_file_produces_global_checks(self, tmp_path: Path) -> None:
        """GH-7.AC7.4: Parquet files produce global check results via DuckDB."""
        pytest.importorskip("duckdb")
        # Create a minimal demographic Parquet file
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        config = QAConfig(tables={"demographic": path})
        outcome = _process_table("demographic", path, config)

        assert outcome.success is True
        assert outcome.validation_result is not None
        # Should have global check results (at minimum uniqueness check 211)
        check_ids = [s.check_id for s in outcome.validation_result.steps if s.check_id]
        assert "211" in check_ids
```

**Testing:**

Tests must verify:
- GH-7.AC6.1: SAS files produce a logged warning and skip global checks
- GH-7.AC6.2: SAS files do not crash
- GH-7.AC7.4: Parquet files produce expected check results (n_passed + n_failed match)

**Verification:**
Run: `uv run pytest`
Expected: All tests pass including new integration tests.

**Commit:** `test(pipeline): add integration tests for SAS skip and Parquet global checks`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Final verification — full test suite

**Verifies:** GH-7.AC7.1, GH-7.AC7.2, GH-7.AC7.3, GH-7.AC7.4

**Files:** None (verification only)

**Implementation:**

Run the complete test suite and verify:
1. All tests pass
2. No `pl.concat()` calls remain in `global_checks.py` (search for it)
3. No `Iterator` imports remain in `global_checks.py`
4. No `_in_memory` functions remain in `global_checks.py`
5. No chunk-based reader creation for global checks remains in `pipeline.py`

Verification commands:

```bash
uv run pytest -v
grep -n "pl.concat" src/scdm_qa/validation/global_checks.py
grep -n "_in_memory" src/scdm_qa/validation/global_checks.py
grep -n "Iterator" src/scdm_qa/validation/global_checks.py
grep -n "create_reader.*global\|uniqueness_reader\|sort_reader\|not_pop_reader\|date_order_reader\|cod_reader\|overlap_reader\|gaps_reader\|enc_combo_reader" src/scdm_qa/pipeline.py
```

All grep commands should return no matches.

**Verification:**
Run: `uv run pytest`
Expected: All tests pass. All grep searches return empty (no dead code).

**Commit:** No commit for this task — verification only.
<!-- END_TASK_4 -->

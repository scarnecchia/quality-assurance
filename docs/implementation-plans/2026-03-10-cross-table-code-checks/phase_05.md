# Cross-Table Checks & Code/CodeType Validation — Implementation Plan

**Goal:** Extend the SCDM QA pipeline with L1 code/codetype checks and L2 cross-table validation via DuckDB.

**Architecture:** Two-level validation pipeline. L1 adds code format/length checks (223, 228) to the existing per-chunk pointblank chain. L2 adds a new DuckDB-based cross-table phase that runs after all L1 processing. Both levels independently controllable via CLI flags and TOML config.

**Tech Stack:** Python 3.12+, Polars, pointblank, DuckDB, Typer, pytest

**Scope:** 7 phases from original design (phases 1–7)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### cross-table-code-checks.AC1: Cross-table validation phase
- **cross-table-code-checks.AC1.2 Success:** Check 201: PatID in diagnosis/procedure/etc. but not in enrollment is flagged as warn
- **cross-table-code-checks.AC1.3 Success:** Check 203: Different max string lengths for same column across table groups is flagged as fail
- **cross-table-code-checks.AC1.4 Success:** Check 205: Enr_Start before Birth_Date (joined on PatID) is flagged as warn
- **cross-table-code-checks.AC1.5 Success:** Check 206: ADate/DDate before Birth_Date (joined on PatID) is flagged as warn
- **cross-table-code-checks.AC1.6 Success:** Check 227: PostalCode_Date before Birth_Date (joined on PatID) is flagged as warn
- **cross-table-code-checks.AC1.7 Success:** Check 209: Actual max column length much smaller than declared schema length across tables is flagged as warn
- **cross-table-code-checks.AC1.8 Success:** Check 224: Hispanic ≠ ImputedHispanic (both non-null) in demographic is flagged as note
- **cross-table-code-checks.AC1.10 Failure:** Missing reference table in config → check skipped with log warning, no crash
- **cross-table-code-checks.AC1.11 Failure:** DuckDB SQL error on a single check → that check returns error StepResult, pipeline continues
- **cross-table-code-checks.AC1.12 Edge:** SAS7BDAT table files are converted to temp parquet before DuckDB registration

---

## Phase 5: DuckDB Cross-Table Engine

This phase implements the DuckDB-based cross-table validation orchestrator and SQL handlers. It creates a new module `src/scdm_qa/validation/cross_table.py`.

**Existing patterns to follow:**
- `src/scdm_qa/validation/global_checks.py` — DuckDB SQL patterns, `StepResult` creation with `step_index=-1`, `check_id`, and `severity`
- `src/scdm_qa/validation/results.py` — `StepResult` frozen dataclass with `check_id: str | None` and `severity: str | None`
- `src/scdm_qa/readers/sas.py` — SAS chunked reading via pyreadstat (for SAS-to-parquet conversion)

**DuckDB API (confirmed via research):**
- `duckdb.connect(":memory:")` — in-memory database, context manager supported
- `conn.execute("CREATE VIEW ... AS SELECT * FROM read_parquet('...')")` — view registration
- `conn.execute("SELECT ...").pl()` — results as Polars DataFrame
- `duckdb.Error` — base exception for all DuckDB errors
- DuckDB cannot read SAS7BDAT natively — must convert to temp parquet first

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Create cross_table.py with orchestrator and view registration

**Verifies:** cross-table-code-checks.AC1.10, cross-table-code-checks.AC1.12

**Files:**
- Create: `src/scdm_qa/validation/cross_table.py`
- Test: `tests/test_cross_table_engine.py` (new file)

**Implementation:**

Create the cross-table validation orchestrator module. The main function signature:

```python
# pattern: Imperative Shell

import logging
import tempfile
from pathlib import Path

import duckdb
import polars as pl

from scdm_qa.config import QAConfig
from scdm_qa.schemas.cross_table_checks import CrossTableCheckDef
from scdm_qa.validation.results import StepResult

log = logging.getLogger(__name__)


def run_cross_table_checks(
    config: QAConfig,
    checks: tuple[CrossTableCheckDef, ...],
    *,
    table_filter: str | None = None,
) -> list[StepResult]:
    ...
```

**Orchestration logic:**

1. Open DuckDB in-memory connection: `duckdb.connect(":memory:")`
2. For each table in `config.tables`:
   - If path ends with `.sas7bdat`: convert to temp parquet using pyreadstat + polars, register the temp path
   - If path ends with `.parquet`: register directly
   - `conn.execute(f"CREATE VIEW {table_key} AS SELECT * FROM read_parquet('{safe_path}')")`
3. Filter checks if `table_filter` is set: only checks where `source_table == table_filter` or `reference_table == table_filter`
4. For each check, dispatch to appropriate handler by `check_type`
5. Catch `duckdb.Error` per check — return error `StepResult` and continue
6. Close connection (use context manager or finally block)
7. Clean up temp parquet files

**SAS-to-parquet conversion** (AC1.12):

Use the existing `SasReader` (in `src/scdm_qa/readers/sas.py`) for chunked reading to avoid loading the entire file into memory. Write chunks incrementally to a temp parquet file:

```python
def _convert_sas_to_parquet(sas_path: Path, chunk_size: int = 500_000) -> Path:
    """Convert SAS7BDAT to temp parquet for DuckDB registration using chunked reading."""
    from scdm_qa.readers import create_reader

    reader = create_reader(sas_path, chunk_size=chunk_size)
    tmp_path = Path(tempfile.mktemp(suffix=".parquet"))

    chunks = list(reader.chunks())
    if chunks:
        combined = pl.concat(chunks)
        combined.write_parquet(tmp_path)
    else:
        pl.DataFrame().write_parquet(tmp_path)

    log.warning("converted SAS file to temp parquet", extra={"sas_path": str(sas_path), "tmp_path": str(tmp_path)})
    return tmp_path
```

Note: This still concatenates all chunks in memory before writing. For truly large files that don't fit in memory, an incremental parquet writer (e.g., `pyarrow.parquet.ParquetWriter`) would be needed. However, since cross-table checks require DuckDB to scan the full table anyway, the memory footprint is dominated by DuckDB's own needs. Log a warning about memory usage for SAS files.

**Missing reference table handling** (AC1.10):

Before executing each check, verify all referenced tables exist as views:
```python
registered_views = set(config.tables.keys())
if check.source_table not in registered_views:
    log.warning("skipping check %s: source table %s not in config", check.check_id, check.source_table)
    continue
if check.reference_table and check.reference_table not in registered_views:
    log.warning("skipping check %s: reference table %s not in config", check.check_id, check.reference_table)
    continue
```

**DuckDB error handling** (AC1.11):
```python
try:
    result = _run_check(conn, check)
    results.append(result)
except duckdb.Error as e:
    log.error("check %s failed with DuckDB error: %s", check.check_id, e)
    results.append(StepResult(
        step_index=-1,
        assertion_type="cross_table",
        column=check.source_column or "",
        description=f"Check {check.check_id} error: {e}",
        n_passed=0,
        n_failed=0,
        failing_rows=None,
        check_id=check.check_id,
        severity=check.severity,
    ))
```

**Testing:**

Tests must verify:
- AC1.10: Config with missing reference table → check skipped with warning, no crash, returns empty results for that check
- AC1.12: SAS file is converted to temp parquet, registered as DuckDB view, query works

Create synthetic parquet files in `tmp_path` for testing. For SAS test, create a small SAS file via pyreadstat or skip if not feasible and test the parquet registration path.

**Verification:**

Run: `uv run pytest tests/test_cross_table_engine.py -v`
Expected: Tests pass.

**Commit:** `feat: add cross-table validation orchestrator with DuckDB`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement SQL check handlers

**Verifies:** cross-table-code-checks.AC1.2, cross-table-code-checks.AC1.3, cross-table-code-checks.AC1.4, cross-table-code-checks.AC1.5, cross-table-code-checks.AC1.6, cross-table-code-checks.AC1.7, cross-table-code-checks.AC1.8, cross-table-code-checks.AC1.11

**Files:**
- Modify: `src/scdm_qa/validation/cross_table.py`
- Test: `tests/test_cross_table_engine.py` (extend)

**Implementation:**

Add handler functions for each check_type. All handlers take `(conn: duckdb.DuckDBPyConnection, check: CrossTableCheckDef)` and return `StepResult`.

**referential_integrity (check 201):**

```sql
SELECT COUNT(*) AS n_missing
FROM {source_table} s
WHERE s."{source_column}" NOT IN (
    SELECT "{reference_column}" FROM {reference_table}
)
```

Returns `StepResult` with `n_failed=n_missing`, `n_passed=total_source - n_missing`.

Also fetch a sample of failing rows (up to `max_failing_rows`):
```sql
SELECT s.*
FROM {source_table} s
WHERE s."{source_column}" NOT IN (
    SELECT "{reference_column}" FROM {reference_table}
)
LIMIT {max_failing_rows}
```

Convert sample to Polars via `.pl()`.

**length_consistency (check 203):**

```sql
SELECT
    '{col}' AS column_name,
    '{table_a}' AS table_key,
    MAX(LENGTH(CAST("{col}" AS VARCHAR))) AS max_len
FROM {table_a}
UNION ALL
SELECT
    '{col}' AS column_name,
    '{table_b}' AS table_key,
    MAX(LENGTH(CAST("{col}" AS VARCHAR))) AS max_len
FROM {table_b}
```

Compare max_len values across tables. Flag if different.

**cross_date_compare (checks 205, 206, 227):**

```sql
SELECT COUNT(*) AS n_violations
FROM {source_table} s
JOIN {reference_table} r ON s."{source_column}" = r."{reference_column}"
WHERE s."{target_column}" < r."Birth_Date"
```

The target_column (e.g., Enr_Start, ADate, DDate, PostalCode_Date) is compared against Birth_Date in the reference table (demographic).

**length_excess (check 209):**

```sql
SELECT
    MAX(LENGTH(CAST("{source_column}" AS VARCHAR))) AS actual_max
FROM {source_table}
```

Compare `actual_max` against the declared schema length from `TableSchema.ColumnDef.length`. Flag as a warning when `actual_max < declared_length * 0.5` (i.e., actual usage is less than half the declared schema length). This threshold is conservative — it identifies columns where the schema declaration is substantially oversized relative to actual data. The handler needs access to the schema registry (`get_schema(table_key)`) to look up `ColumnDef.length` for the column.

**column_mismatch (check 224):**

```sql
SELECT COUNT(*) AS n_mismatches
FROM {source_table}
WHERE "{column_a}" IS NOT NULL
  AND "{column_b}" IS NOT NULL
  AND "{column_a}" != "{column_b}"
```

**StepResult creation pattern** (same as global_checks.py):

All handlers return `StepResult` with:
- `step_index=-1` (renumbered later)
- `assertion_type="cross_table"`
- `column` = relevant column name
- `description` = human-readable check description
- `check_id` = check definition's check_id
- `severity` = check definition's severity

**Testing:**

Each check type needs its own test with synthetic multi-table parquet files:
- AC1.2: Two parquet files (source with PatIDs not in enrollment) → n_failed > 0
- AC1.3: Two parquet files with same column but different max lengths → flagged
- AC1.4: Enrollment with Enr_Start before Birth_Date → n_failed > 0
- AC1.5: Encounter/diagnosis with ADate before Birth_Date → n_failed > 0
- AC1.6: Address history with PostalCode_Date before Birth_Date → n_failed > 0
- AC1.7: Table with actual max length much shorter than schema declares → flagged
- AC1.8: Demographic with Hispanic ≠ ImputedHispanic (both non-null) → n_failed > 0
- AC1.11: Inject a SQL error scenario (e.g., missing column in view) → error StepResult returned, no crash

**Verification:**

Run: `uv run pytest tests/test_cross_table_engine.py -v`
Expected: All tests pass.

**Commit:** `feat: implement cross-table SQL check handlers for all check types`

<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_TASK_3 -->
### Task 3: Full test suite verification

**Verification:**

Run: `uv run pytest`
Expected: All tests pass. No regressions.

<!-- END_TASK_3 -->

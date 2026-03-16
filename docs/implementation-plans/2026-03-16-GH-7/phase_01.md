# DuckDB Global Checks Migration - Phase 1: Pipeline View Registration

**Goal:** Add DuckDB connection lifecycle and view registration to the pipeline's per-table processing, with a Parquet-only gate for SAS files.

**Architecture:** The pipeline's `_process_table()` function will create a single DuckDB connection per table, register the Parquet file as a named view, pass the connection and view name to global check functions, and close the connection in a `finally` block. SAS files skip global checks entirely with a logged warning.

**Tech Stack:** Python 3.12+, DuckDB, structlog, polars, pytest

**Scope:** 6 phases from original design (phase 1 of 6)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

This phase is an infrastructure phase. It adds the connection lifecycle and SAS gate without changing any check function signatures yet. Existing tests must continue to pass unchanged.

### GH-7.AC6: SAS files handled correctly
- **GH-7.AC6.1 Success:** Pipeline skips global checks for SAS files with a logged warning
- **GH-7.AC6.2 Success:** SAS files do not cause errors or crashes — graceful skip

---

<!-- START_TASK_1 -->
### Task 1: Add DuckDB connection lifecycle and view registration to _process_table()

**Verifies:** GH-7.AC6.1, GH-7.AC6.2

**Files:**
- Modify: `src/scdm_qa/pipeline.py:1-28` (imports), `src/scdm_qa/pipeline.py:176-247` (global checks block)

**Implementation:**

Add imports at the top of `pipeline.py`:

```python
import duckdb
from scdm_qa.validation.duckdb_utils import create_connection
```

Replace the global checks block (lines 176-247) with a version that:

1. Before executing any global checks, checks if the file is Parquet via `file_path.suffix.lower() == ".parquet"`
2. If SAS (`.sas7bdat`), logs a warning and skips all global checks — no connection created
3. If Parquet, creates a DuckDB connection via `create_connection()` using config fields
4. Registers the Parquet file as a view: `CREATE VIEW "{table_key}" AS SELECT * FROM read_parquet('{safe_path}')`
5. Wraps the entire global checks block in a `try/finally` that closes the connection
6. All existing check function calls remain exactly as they are — no signature changes yet

The structure should follow the cross-table pattern from `cross_table.py:44-117`:

```python
# Global checks (require full-table access)
global_steps: list[StepResult] = []
is_parquet = file_path.suffix.lower() == ".parquet"

if not is_parquet:
    log.warning(
        "skipping global checks for non-Parquet file",
        table=table_key,
        file=str(file_path),
        reason="global checks require Parquet format",
    )
else:
    conn: duckdb.DuckDBPyConnection | None = None
    try:
        conn = create_connection(
            memory_limit=config.duckdb_memory_limit,
            threads=config.duckdb_threads,
            temp_directory=config.duckdb_temp_directory,
        )
        safe_path = str(file_path).replace("'", "''")
        conn.execute(
            f'CREATE VIEW "{table_key}" AS SELECT * FROM read_parquet(\'{safe_path}\')'
        )
        log.debug("registered global check view", table=table_key)

        # --- All existing global check calls go here, unchanged ---
        if schema.unique_row:
            uniqueness_reader = create_reader(file_path, chunk_size=config.chunk_size)
            uniqueness_step = check_uniqueness(
                file_path,
                schema,
                chunks=uniqueness_reader.chunks(),
                max_failing_rows=config.max_failing_rows,
                duckdb_memory_limit=config.duckdb_memory_limit,
                duckdb_threads=config.duckdb_threads,
                duckdb_temp_directory=config.duckdb_temp_directory,
            )
            if uniqueness_step is not None:
                global_steps.append(uniqueness_step)

        if schema.sort_order:
            sort_reader = create_reader(file_path, chunk_size=config.chunk_size)
            sort_step = check_sort_order(schema, sort_reader.chunks())
            if sort_step is not None:
                global_steps.append(sort_step)

        if get_not_populated_checks_for_table(schema.table_key):
            not_pop_reader = create_reader(file_path, chunk_size=config.chunk_size)
            not_pop_steps = check_not_populated(schema, not_pop_reader.chunks())
            global_steps.extend(not_pop_steps)

        if get_date_ordering_checks_for_table(schema.table_key):
            date_order_reader = create_reader(file_path, chunk_size=config.chunk_size)
            date_order_steps = check_date_ordering(
                schema, date_order_reader.chunks(),
                max_failing_rows=config.max_failing_rows,
            )
            global_steps.extend(date_order_steps)

        if schema.table_key == "cause_of_death":
            cod_reader = create_reader(file_path, chunk_size=config.chunk_size)
            cod_steps = check_cause_of_death(
                schema, cod_reader.chunks(),
                max_failing_rows=config.max_failing_rows,
            )
            global_steps.extend(cod_steps)

        if schema.table_key == "enrollment":
            overlap_reader = create_reader(file_path, chunk_size=config.chunk_size)
            overlap_step = check_overlapping_spans(
                file_path, schema, overlap_reader.chunks(),
                max_failing_rows=config.max_failing_rows,
                duckdb_memory_limit=config.duckdb_memory_limit,
                duckdb_threads=config.duckdb_threads,
                duckdb_temp_directory=config.duckdb_temp_directory,
            )
            if overlap_step is not None:
                global_steps.append(overlap_step)

            gaps_reader = create_reader(file_path, chunk_size=config.chunk_size)
            gaps_step = check_enrollment_gaps(
                schema, gaps_reader.chunks(),
                max_failing_rows=config.max_failing_rows,
            )
            if gaps_step is not None:
                global_steps.append(gaps_step)

        if schema.table_key == "encounter":
            enc_combo_reader = create_reader(file_path, chunk_size=config.chunk_size)
            enc_combo_steps = check_enc_combinations(
                schema, enc_combo_reader.chunks(),
                max_failing_rows=config.max_failing_rows,
            )
            global_steps.extend(enc_combo_steps)

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                log.warning("failed to close DuckDB connection", error=str(e))
```

The connection and view are created but not yet consumed by any check functions — they still use their existing chunk-based paths. Later phases will change check function signatures to use `conn` and `view_name` (which is `table_key`).

**Transitional resource note:** During this phase, Parquet tables will have both a DuckDB connection AND chunk-based readers created simultaneously. This is expected transitional behaviour — the chunk readers are removed as each check function is migrated in Phases 2-5.

**Testing:**

Tests must verify:
- GH-7.AC6.1: Pipeline skips global checks for SAS files with a logged warning
- GH-7.AC6.2: SAS files do not cause errors or crashes — graceful skip

Test file: `tests/test_pipeline_phases.py` (or `tests/test_global_checks.py` if SAS-skip tests fit better alongside existing global check tests)

Follow the project's existing Parquet fixture pattern from `test_cross_table_engine.py`: create temp Parquet files via `pl.DataFrame.write_parquet()`, build a `QAConfig` with those paths.

For the SAS skip test: create a dummy `.sas7bdat` file path (it does not need to be a valid SAS file — the skip happens before any read). Verify that `_process_table()` returns a `TableOutcome` with `success=True` and no global check `StepResult`s, and that the warning was logged.

**Verification:**
Run: `uv run pytest`
Expected: All 416 existing tests pass. New tests for SAS skip behaviour also pass.

**Commit:** `feat(pipeline): add DuckDB connection lifecycle and SAS file gate for global checks`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add DuckDB minimum version constraint to pyproject.toml

**Verifies:** None (infrastructure)

**Files:**
- Modify: `pyproject.toml` (dependencies section)

**Implementation:**

The design requires DuckDB v1.2+ for window function disk spillage support, which is needed for large enrollment partitions. Add a minimum version constraint to the project dependencies.

In `pyproject.toml`, find the `duckdb` entry in `dependencies` and add a version floor:

```toml
"duckdb>=1.2.0",
```

If `duckdb` currently has no version pin, add `>=1.2.0`. If it already has a constraint, ensure `>=1.2.0` is included.

**Verification:**
Run: `uv sync`
Expected: Dependency resolves without errors. DuckDB version installed is >= 1.2.0.

Run: `uv run python -c "import duckdb; print(duckdb.__version__)"`
Expected: Version >= 1.2.0

**Commit:** `chore: add duckdb>=1.2.0 minimum version constraint`
<!-- END_TASK_2 -->

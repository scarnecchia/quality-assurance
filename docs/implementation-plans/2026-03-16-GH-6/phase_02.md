# Streaming SAS-to-Parquet Conversion Implementation Plan

**Goal:** Replace the in-memory SAS-to-Parquet concat with streaming writes via `pyarrow.parquet.ParquetWriter`, keeping memory bounded to one chunk at a time.

**Architecture:** A canonical `pyarrow.Schema` is built from SCDM `TableSchema`/`ColumnDef` definitions (Numeric -> float64, Character -> utf8). Each SAS chunk is cast to this schema and written as a Parquet row group. Unknown tables fall back to first-chunk inference.

**Tech Stack:** Python 3.12+, pyarrow (ParquetWriter, Schema), polars (DataFrame chunks), pyreadstat (SAS reading), pytest

**Scope:** 3 phases from original design (phases 1-3)

**Codebase verified:** 2026-03-16

---

## Acceptance Criteria Coverage

This phase implements and tests:

### GH-6.AC1: Streaming write produces correct output
- **GH-6.AC1.1 Success:** Multi-chunk SAS file converts to valid Parquet with all rows preserved
- **GH-6.AC1.2 Success:** Each chunk becomes a separate Parquet row group
- **GH-6.AC1.3 Success:** Output Parquet schema matches canonical SCDM spec types (Numeric->float64, Character->utf8)
- **GH-6.AC1.4 Edge:** Empty SAS file (zero chunks) produces valid empty Parquet file
- **GH-6.AC1.5 Edge:** Chunk with all-null column is cast to correct canonical type, not inferred as null/float64

### GH-6.AC2: Memory stays bounded
- **GH-6.AC2.1 Success:** Memory usage stays O(chunk_size) regardless of total file size — no full-dataset concat

### GH-6.AC3: Canonical schema resolution
- **GH-6.AC3.2 Success:** Columns in data but not in spec are preserved with inferred types
- **GH-6.AC3.3 Failure:** Unknown table key falls back to first-chunk inference and logs warning

### GH-6.AC4: Backward compatibility
- **GH-6.AC4.1 Success:** All existing cross-table validation tests pass unchanged

---

## Phase 2: Streaming ParquetWriter

<!-- START_SUBCOMPONENT_A (tasks 1-2) -->
<!-- START_TASK_1 -->
### Task 1: Replace `_convert_sas_to_parquet` with streaming writer

**Verifies:** GH-6.AC1.1, GH-6.AC1.2, GH-6.AC1.3, GH-6.AC1.4, GH-6.AC1.5, GH-6.AC2.1, GH-6.AC3.2, GH-6.AC3.3

**Files:**
- Modify: `src/scdm_qa/validation/cross_table.py` — replace the `_convert_sas_to_parquet` function body (locate by function name; line numbers will have shifted after Phase 1 additions)
- Modify: `src/scdm_qa/validation/cross_table.py` — update the call site in `run_cross_table_checks` where `_convert_sas_to_parquet` is called inside the `for table_key, file_path in config.tables.items()` loop

**Implementation:**

Replace the current `_convert_sas_to_parquet` function with a streaming implementation. Locate it by function name (line numbers will have shifted after Phase 1 additions). The new function:

1. Accepts `table_key: str` as a new parameter
2. Attempts to resolve a canonical schema via `get_schema(table_key)` + `build_arrow_schema()` from Phase 1
3. If `get_schema` raises `KeyError` (unknown table), sets canonical schema to `None` and logs a warning
4. Opens a `pyarrow.parquet.ParquetWriter` after determining the write schema from the first chunk
5. For each chunk from `reader.chunks()`:
   - Converts `polars.DataFrame` to `pyarrow.Table` via `.to_arrow()`
   - Builds the merged write schema on the first chunk: canonical types for spec columns + inferred types for non-spec columns (preserving data column order)
   - Casts the arrow table to the write schema
   - Writes via `writer.write_table(table)` — each chunk becomes one row group
6. Handles the zero-chunks edge case by writing an empty table
7. Logs conversion with column count and total row count

The key schema merging logic for the first chunk:
- Get data column names from the first arrow table
- Call `build_arrow_schema(table_schema, data_columns=data_col_names)` to get canonical fields for known columns
- For each data column NOT in the canonical schema, take its inferred type from the arrow table
- Build a merged schema preserving data column order: canonical type if available, inferred type otherwise

```python
def _convert_sas_to_parquet(
    sas_path: Path,
    chunk_size: int = 500_000,
    *,
    table_key: str,
) -> Path:
    """Convert SAS7BDAT to temp Parquet via streaming writes.

    Each chunk is cast to a canonical schema (from the SCDM spec) and written
    as a separate Parquet row group, keeping memory bounded to one chunk.

    Args:
        sas_path: Path to .sas7bdat file.
        chunk_size: Chunk size for reading.
        table_key: SCDM table key for canonical schema lookup.

    Returns:
        Path to temporary Parquet file.
    """
    import pyarrow.parquet as pq

    from scdm_qa.readers import create_reader

    reader = create_reader(sas_path, chunk_size=chunk_size)

    # Resolve canonical schema (None if table_key unknown)
    canonical_schema: TableSchema | None = None
    try:
        canonical_schema = get_schema(table_key)
    except KeyError:
        log.warning(
            "no SCDM spec for table; schema will be inferred from data",
            table_key=table_key,
        )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    writer: pq.ParquetWriter | None = None
    write_schema: pa.Schema | None = None
    total_rows = 0

    try:
        for chunk_df in reader.chunks():
            arrow_table = chunk_df.to_arrow()

            if write_schema is None:
                write_schema = _build_write_schema(
                    canonical_schema, arrow_table.schema
                )
                writer = pq.ParquetWriter(str(tmp_path), write_schema)

            arrow_table = arrow_table.cast(write_schema)
            writer.write_table(arrow_table)
            total_rows += arrow_table.num_rows

        # Handle zero-chunk case
        if writer is None:
            if canonical_schema is not None:
                write_schema = build_arrow_schema(canonical_schema)
            else:
                write_schema = pa.schema([])
            writer = pq.ParquetWriter(str(tmp_path), write_schema)
    finally:
        if writer is not None:
            writer.close()

    log.info(
        "converted SAS file to temp parquet (streaming)",
        sas_path=str(sas_path),
        tmp_path=str(tmp_path),
        n_rows=total_rows,
        n_columns=write_schema.length if write_schema else 0,
    )
    return tmp_path
```

Add a helper `_build_write_schema` that merges canonical and inferred types:

```python
def _build_write_schema(
    canonical_schema: TableSchema | None,
    data_schema: pa.Schema,
) -> pa.Schema:
    """Build a write schema merging canonical SCDM types with inferred types.

    Canonical types are used for columns defined in the SCDM spec. Columns
    present in data but not in spec keep their inferred types. Column order
    follows the data schema.

    Args:
        canonical_schema: SCDM TableSchema, or None for unknown tables.
        data_schema: Schema inferred from the first data chunk.

    Returns:
        Merged pyarrow.Schema with canonical types where available.
    """
    if canonical_schema is None:
        return data_schema

    data_col_names = tuple(data_schema.names)
    canonical = build_arrow_schema(canonical_schema, data_columns=data_col_names)
    canonical_lookup = {f.name: f for f in canonical}

    merged_fields: list[pa.Field] = []
    for i, name in enumerate(data_col_names):
        if name in canonical_lookup:
            merged_fields.append(canonical_lookup[name])
        else:
            merged_fields.append(data_schema.field(i))

    return pa.schema(merged_fields)
```

**Signature compatibility note:** The existing `chunk_size` parameter stays positional with its default. The new `table_key` is keyword-only (after `*`). The existing call site already uses `chunk_size=config.chunk_size` (keyword style), so adding `table_key=table_key` is fully backward-compatible with the call pattern.

Update the call site in `run_cross_table_checks` (inside the `for table_key, file_path` loop):

```python
# Before:
temp_path = _convert_sas_to_parquet(file_path, chunk_size=config.chunk_size)

# After:
temp_path = _convert_sas_to_parquet(
    file_path, chunk_size=config.chunk_size, table_key=table_key
)
```

Add `import pyarrow as pa` at the top of the file (alongside existing imports). The `import pyarrow.parquet as pq` stays lazy inside the function (following existing pattern for format-specific imports).

**Testing:**

Tests go in `tests/test_cross_table_engine.py`.

**Note on testing private functions:** `_convert_sas_to_parquet` is a private function (leading underscore). Tests import it directly because end-to-end testing through the public `run_cross_table_checks` would require full DuckDB setup with registered views, which isn't practical for unit-level verification of the streaming write logic. Direct import of private functions for testing is acceptable in this project.

Since creating valid SAS7BDAT files requires `pyreadstat.write_sas7bdat` which may not be reliably available, tests should mock the reader to yield known polars DataFrames. This is acceptable because we're testing the streaming write logic, not the SAS reading (which is tested separately in `test_readers.py`).

Tests must verify:
- **GH-6.AC1.1**: Multi-chunk write preserves all rows. Mock reader yields 3 chunks of 10 rows each. Read back parquet, assert 30 total rows and correct data.
- **GH-6.AC1.2**: Each chunk becomes a row group. Read parquet metadata, assert `num_row_groups == 3`.
- **GH-6.AC1.3**: Output schema matches canonical types. Read parquet, verify float64 for Numeric columns, utf8 for Character columns.
- **GH-6.AC1.4**: Zero chunks produces valid empty parquet. Mock reader yields nothing. Read back parquet, assert 0 rows and valid schema.
- **GH-6.AC1.5**: All-null column cast to canonical type. One chunk where a Numeric column is entirely null. Read back, verify column type is float64 (not null or object).
- **GH-6.AC2.1**: No `pl.concat` in the function. This is verified structurally by code review — the old concat pattern is removed.
- **GH-6.AC3.2**: Data has extra columns not in SCDM spec. Verify they appear in output with inferred types.
- **GH-6.AC3.3**: Pass an unknown `table_key`. Verify output is written successfully with inferred types and a warning is logged.

Use `unittest.mock.patch` on `scdm_qa.readers.create_reader` to return a mock reader yielding controlled polars DataFrames. This follows the project convention of mocking only for unavailable services/fallback logic (SAS file creation is the unavailable service here).

```python
class TestStreamingSasConversion:
    def test_multi_chunk_preserves_all_rows(self) -> None: ...
    def test_each_chunk_becomes_row_group(self) -> None: ...
    def test_output_schema_matches_canonical_types(self) -> None: ...
    def test_empty_input_produces_valid_parquet(self) -> None: ...
    def test_all_null_column_cast_to_canonical_type(self) -> None: ...
    def test_extra_columns_preserved_with_inferred_types(self) -> None: ...
    def test_unknown_table_key_falls_back_to_inference(self) -> None: ...
```

**Verification:**

Run: `uv run pytest tests/test_cross_table_engine.py -v -k TestStreamingSasConversion`
Expected: All tests pass

**Commit:** `feat: stream SAS-to-Parquet via ParquetWriter with canonical schema (GH-6)`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Test `_build_write_schema` merge logic

**Verifies:** GH-6.AC3.2

**Files:**
- Test: `tests/test_cross_table_engine.py`

**Implementation:** No new production code. Testing the helper added in Task 1.

**Testing:**

Tests must verify the merge logic directly:
- Canonical columns get canonical types (not inferred types)
- Non-spec columns keep their inferred types
- Column order follows data schema, not spec order
- When `canonical_schema` is `None`, returns the data schema unchanged

```python
class TestBuildWriteSchema:
    def test_canonical_columns_get_spec_types(self) -> None: ...
    def test_non_spec_columns_keep_inferred_types(self) -> None: ...
    def test_column_order_follows_data(self) -> None: ...
    def test_none_canonical_returns_data_schema(self) -> None: ...
```

**Verification:**

Run: `uv run pytest tests/test_cross_table_engine.py -v -k TestBuildWriteSchema`
Expected: All tests pass

**Commit:** `test: add _build_write_schema merge logic tests (GH-6)`
<!-- END_TASK_2 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_TASK_3 -->
### Task 3: Verify all existing tests still pass

**Verifies:** GH-6.AC4.1

**Files:** None (read-only verification)

**Verification:**

Run: `uv run pytest`
Expected: All 416+ tests pass (including new Phase 1 and Phase 2 tests), no regressions

**Commit:** No commit needed — verification only
<!-- END_TASK_3 -->

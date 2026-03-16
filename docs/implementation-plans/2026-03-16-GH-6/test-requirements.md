# Test Requirements: GH-6 Streaming SAS-to-Parquet Conversion

Maps each acceptance criterion to automated tests or human verification.

---

## GH-6.AC1: Streaming write produces correct output

### AC1.1: Multi-chunk SAS file converts to valid Parquet with all rows preserved

- **Type:** Automated — Unit
- **File:** `tests/test_cross_table_engine.py`
- **Class:** `TestStreamingSasConversion`
- **Method:** `test_multi_chunk_preserves_all_rows`
- **Verifies:** Mock reader yields 3 chunks of 10 rows each. Reads back the output Parquet and asserts 30 total rows with correct data values across all chunks.

### AC1.2: Each chunk becomes a separate Parquet row group

- **Type:** Automated — Unit
- **File:** `tests/test_cross_table_engine.py`
- **Class:** `TestStreamingSasConversion`
- **Method:** `test_each_chunk_becomes_row_group`
- **Verifies:** Mock reader yields 3 chunks. Reads Parquet metadata via `pyarrow.parquet.read_metadata` and asserts `num_row_groups == 3`.

### AC1.3: Output Parquet schema matches canonical SCDM spec types

- **Type:** Automated — Unit
- **File:** `tests/test_cross_table_engine.py`
- **Class:** `TestStreamingSasConversion`
- **Method:** `test_output_schema_matches_canonical_types`
- **Verifies:** Reads back output Parquet schema and asserts Numeric columns are `float64` and Character columns are `utf8`, matching SCDM spec definitions.

### AC1.4: Empty SAS file produces valid empty Parquet file

- **Type:** Automated — Unit
- **File:** `tests/test_cross_table_engine.py`
- **Class:** `TestStreamingSasConversion`
- **Method:** `test_empty_input_produces_valid_parquet`
- **Verifies:** Mock reader yields zero chunks. Reads back Parquet and asserts 0 rows with a valid schema (canonical types for known table key).

### AC1.5: Chunk with all-null column is cast to correct canonical type

- **Type:** Automated — Unit
- **File:** `tests/test_cross_table_engine.py`
- **Class:** `TestStreamingSasConversion`
- **Method:** `test_all_null_column_cast_to_canonical_type`
- **Verifies:** Mock reader yields one chunk where a Numeric column is entirely null. Reads back Parquet and asserts column type is `float64` (not null or object type from inference).

---

## GH-6.AC2: Memory stays bounded

### AC2.1: Memory usage stays O(chunk_size) regardless of total file size

- **Type:** Human verification
- **Justification:** True O(chunk_size) memory bounding is a property of the algorithm (no `pl.concat` of all chunks), not a discrete output. While a memory-profiling test is theoretically possible, it would be flaky across environments, sensitive to GC timing, and dependent on dataset size. The meaningful verification is structural: the old `pl.concat(chunks)` pattern is removed and replaced by per-chunk `writer.write_table()` calls.
- **Verification approach:**
  1. **Code review:** Confirm `_convert_sas_to_parquet` has no list accumulation of chunks and no `pl.concat`. Each chunk is written to `ParquetWriter` and then falls out of scope.
  2. **Automated structural check (supplementary):** The unit tests in `TestStreamingSasConversion` exercise multi-chunk writes, confirming the streaming path is functional. The absence of `pl.concat` in the function body can be confirmed during review.
  3. **Integration test (supplementary):** AC5.1 exercises a real SAS file with `chunk_size=100`, forcing many chunks through the streaming path. If memory were unbounded, large files would cause visible issues.

---

## GH-6.AC3: Canonical schema resolution

### AC3.1: Known SCDM table key resolves to canonical pyarrow.Schema from spec

- **Type:** Automated — Unit
- **File:** `tests/test_cross_table_engine.py`
- **Class:** `TestBuildArrowSchema`
- **Methods:**
  - `test_known_table_produces_correct_types` — Calls `build_arrow_schema(get_schema("demographic"))` and asserts correct field count and types.
  - `test_numeric_columns_map_to_float64` — Asserts all Numeric `ColumnDef` entries map to `pa.float64()`.
  - `test_character_columns_map_to_utf8` — Asserts all Character `ColumnDef` entries map to `pa.utf8()`.
  - `test_nullability_matches_missing_allowed` — Asserts each field's `nullable` matches the corresponding `ColumnDef.missing_allowed`.
- **Verifies:** End-to-end resolution from SCDM table key through `get_schema()` to a correct `pyarrow.Schema` with canonical types and nullability.

### AC3.2: Columns in data but not in spec are preserved with inferred types

- **Type:** Automated — Unit
- **File:** `tests/test_cross_table_engine.py`
- **Classes/Methods:**
  - `TestBuildArrowSchema.test_data_columns_excludes_non_spec_columns` — Verifies `build_arrow_schema` excludes non-spec columns (caller responsibility to merge).
  - `TestBuildWriteSchema.test_non_spec_columns_keep_inferred_types` — Verifies `_build_write_schema` preserves inferred types for non-spec columns in the merged output.
  - `TestBuildWriteSchema.test_column_order_follows_data` — Verifies merged schema respects data column order.
  - `TestStreamingSasConversion.test_extra_columns_preserved_with_inferred_types` — End-to-end: mock reader yields chunks with extra columns not in SCDM spec; reads back Parquet and asserts those columns are present with inferred types.
- **Verifies:** Non-spec columns survive the full pipeline from chunk through schema merge to Parquet output.

### AC3.3: Unknown table key falls back to first-chunk inference and logs warning

- **Type:** Automated — Unit
- **File:** `tests/test_cross_table_engine.py`
- **Class:** `TestStreamingSasConversion`
- **Method:** `test_unknown_table_key_falls_back_to_inference`
- **Verifies:** Passes a `table_key` not in the SCDM spec. Asserts output Parquet is written successfully with inferred types and that a warning is logged containing the unknown table key.
- **Supplementary:** `TestBuildWriteSchema.test_none_canonical_returns_data_schema` verifies the merge helper returns the data schema unchanged when canonical is `None`.

---

## GH-6.AC4: Backward compatibility

### AC4.1: All existing cross-table validation tests pass unchanged

- **Type:** Automated — Regression
- **File:** All existing test files (full `uv run pytest` suite)
- **Verifies:** Run the full test suite after all phases. All 416+ pre-existing tests pass with zero modifications. This is verified at the end of Phase 2 (Task 3) and Phase 3 (Task 2).
- **Note:** This is not a new test but a regression gate. The existing tests in `tests/test_cross_table_engine.py` and all other test files serve as the backward compatibility verification.

### AC4.2: Cross-table checks produce identical results for Parquet inputs

- **Type:** Automated — Regression
- **File:** `tests/test_cross_table_engine.py` (existing tests)
- **Verifies:** Existing cross-table validation tests already exercise Parquet-input paths (no SAS conversion involved). These tests passing unchanged confirms that the Parquet-only code path is unaffected by the new streaming conversion logic. No new test required — the existing suite covers this.

---

## GH-6.AC5: Integration with real SAS data

### AC5.1: Real SCDM SAS file converts correctly with small chunk_size

- **Type:** Automated — Integration
- **File:** `tests/test_sas_streaming_integration.py`
- **Class:** `TestSasStreamingIntegration`
- **Methods:**
  - `test_real_sas_output_schema_matches_canonical_types` — Converts a real SAS file with `chunk_size=100`, reads back Parquet, verifies schema has canonical SCDM types for spec columns.
  - `test_real_sas_produces_multiple_row_groups` — Asserts output Parquet has multiple row groups (confirming the small chunk_size forced multi-chunk writes).
- **Verifies:** Full streaming conversion path works against real production-representative SAS data with forced multi-chunk behaviour.
- **Prerequisite:** `SCDM_SAS_DATA_DIR` environment variable pointing to a directory containing `.sas7bdat` files.

### AC5.2: Output row count matches source SAS row count

- **Type:** Automated — Integration
- **File:** `tests/test_sas_streaming_integration.py`
- **Class:** `TestSasStreamingIntegration`
- **Method:** `test_real_sas_converts_with_correct_row_count`
- **Verifies:** Reads source SAS file via `pyreadstat.read_sas7bdat` to get expected row count, converts via streaming writer, reads back Parquet row count, and asserts they match exactly.
- **Prerequisite:** `SCDM_SAS_DATA_DIR` environment variable.

### AC5.3: Integration test skips cleanly when data directory is unavailable

- **Type:** Automated — Skip behaviour
- **File:** `tests/test_sas_streaming_integration.py`
- **Verifies:** Module-level `pytestmark = pytest.mark.skipif(not _HAS_DATA, reason=...)` causes all tests in the module to be skipped with a clear reason message when `SCDM_SAS_DATA_DIR` is unset or points to a nonexistent/empty directory.
- **Verification approach:** Run `uv run pytest tests/test_sas_streaming_integration.py -v` without setting the environment variable. All tests should show as skipped with reason "SCDM_SAS_DATA_DIR not set or contains no SAS files".

---

## Summary Matrix

| Criterion | Verification | Test File | Test Class/Method |
|-----------|-------------|-----------|-------------------|
| AC1.1 | Automated (unit) | `test_cross_table_engine.py` | `TestStreamingSasConversion::test_multi_chunk_preserves_all_rows` |
| AC1.2 | Automated (unit) | `test_cross_table_engine.py` | `TestStreamingSasConversion::test_each_chunk_becomes_row_group` |
| AC1.3 | Automated (unit) | `test_cross_table_engine.py` | `TestStreamingSasConversion::test_output_schema_matches_canonical_types` |
| AC1.4 | Automated (unit) | `test_cross_table_engine.py` | `TestStreamingSasConversion::test_empty_input_produces_valid_parquet` |
| AC1.5 | Automated (unit) | `test_cross_table_engine.py` | `TestStreamingSasConversion::test_all_null_column_cast_to_canonical_type` |
| AC2.1 | Human (code review) | n/a | Structural: no `pl.concat`, per-chunk `write_table` |
| AC3.1 | Automated (unit) | `test_cross_table_engine.py` | `TestBuildArrowSchema` (4 methods) |
| AC3.2 | Automated (unit) | `test_cross_table_engine.py` | `TestBuildWriteSchema` + `TestStreamingSasConversion::test_extra_columns_preserved` |
| AC3.3 | Automated (unit) | `test_cross_table_engine.py` | `TestStreamingSasConversion::test_unknown_table_key_falls_back_to_inference` |
| AC4.1 | Automated (regression) | All existing tests | Full `uv run pytest` suite |
| AC4.2 | Automated (regression) | `test_cross_table_engine.py` | Existing Parquet-path tests |
| AC5.1 | Automated (integration) | `test_sas_streaming_integration.py` | `TestSasStreamingIntegration` (schema + row groups) |
| AC5.2 | Automated (integration) | `test_sas_streaming_integration.py` | `TestSasStreamingIntegration::test_real_sas_converts_with_correct_row_count` |
| AC5.3 | Automated (skip behaviour) | `test_sas_streaming_integration.py` | Module-level `pytest.mark.skipif` |

**Coverage:** 15/15 acceptance criteria mapped. 14 automated, 1 human verification (AC2.1 — memory boundedness is a structural property verified by code review).

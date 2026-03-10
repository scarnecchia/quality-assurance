# SCDM-QA Test Requirements

## Overview

This document maps every acceptance criterion from the SCDM-QA design to specific automated tests or documented human verification steps. Test file paths reference the planned project layout under `tests/`. Phase references indicate which implementation plan document defined the test.

---

## Automated Test Coverage

### scdm-qa.AC1: CLI tool validates SCDM data and produces reports

#### scdm-qa.AC1.1 — `scdm-qa run config.toml` validates all configured tables and produces HTML reports in output directory

- **Test type:** Integration
- **Test file:** `tests/test_cli.py`
- **Test class/method:** `TestRunCommand.test_produces_reports_for_clean_data`
- **What it verifies:** Creates a Parquet file with valid demographic data, writes a TOML config, invokes `scdm-qa run` via CliRunner, asserts exit code 0 and checks that `demographic.html` and `index.html` exist in the output directory.
- **Phase:** 8, Task 3

#### scdm-qa.AC1.2 — `scdm-qa run config.toml --table enrollment` validates only the specified table

- **Test type:** Integration
- **Test file:** `tests/test_cli.py`
- **Test class/method:** `TestRunCommand.test_run_with_table_filter` (Phase 1 stub; superseded by Phase 8 pipeline integration)
- **What it verifies:** Invokes CLI with `--table` flag and validates that only the specified table is processed. The `table_filter` parameter is threaded into `run_pipeline()` which filters the `tables` dict to a single entry before processing.
- **Phase:** 1 (stub), 8 (full pipeline wiring via `run_pipeline(table_filter=...)`)

#### scdm-qa.AC1.3 — When one table fails, remaining tables still validate successfully

- **Test type:** Integration
- **Test file:** `tests/test_cli.py`
- **Test class/method:** `TestRunCommandTableIsolation.test_one_table_failure_doesnt_block_others`
- **What it verifies:** Creates a config with two tables — one valid Parquet file and one referencing a non-existent file. Asserts that the valid table's HTML report is still created despite the other table's failure.
- **Phase:** 8, Task 3

#### scdm-qa.AC1.4 — Exit code 0 when all checks pass, 1 when warnings (threshold exceeded), 2 when failures

- **Test type:** Integration + Unit
- **Test file:** `tests/test_cli.py`
- **Test class/methods:**
  - `TestRunCommand.test_produces_reports_for_clean_data` — exit code 0 for clean data
  - `TestRunCommand.test_exit_code_2_when_failures_exceed_threshold` — exit code 2 when failure rate (20%) exceeds error_threshold (5%)
  - `TestRunCommand.test_missing_config_exits_2` — exit code 2 for configuration errors
- **What it verifies:** The `compute_exit_code()` function in `pipeline.py` implements three-tier logic: 0 = no failures, 1 = failures within threshold, 2 = failures exceeding threshold or processing errors.
- **Phase:** 8, Task 1 (pipeline logic) + Task 3 (CLI tests)
- **Note:** The exit code 1 (warnings) case is implicitly tested through the `compute_exit_code` function but could benefit from an explicit test with a failure rate between 0% and the threshold. See Human Verification section.

#### scdm-qa.AC1.5 — `scdm-qa profile config.toml` runs profiling only (no rule validation)

- **Test type:** Integration
- **Test file:** `tests/test_cli.py`
- **Test class/method:** `TestProfileCommand.test_runs_profiling_only`
- **What it verifies:** Creates valid Parquet data and config, invokes `scdm-qa profile`, asserts exit code 0 and output contains "profiled". The pipeline runs with `profile_only=True`, which iterates chunks through the ProfilingAccumulator without building pointblank validation chains.
- **Phase:** 8, Task 3

#### scdm-qa.AC1.6 — `scdm-qa serve ./qa-reports/` launches local HTTP server serving report files

- **Test type:** Integration (partial)
- **Test file:** `tests/test_cli.py`
- **Test class/method:** `TestServeCommand.test_nonexistent_dir_exits_2`
- **What it verifies:** Asserts that serving a non-existent directory produces exit code 2. The full HTTP server lifecycle cannot be tested via CliRunner due to the blocking `serve_forever()` call. See Human Verification section.
- **Phase:** 8, Task 3

---

### scdm-qa.AC2: Validation rules cover the full SCDM spec

#### scdm-qa.AC2.1 — Non-nullable columns with null values produce validation warnings

- **Test type:** Unit + Integration
- **Test files:**
  - `tests/test_validation.py` — `TestBuildValidationNullability.test_non_nullable_column_with_null_fails`
  - `tests/test_accumulator.py` — `TestAccumulatorSumsAcrossChunks.test_sums_pass_fail_counts`
  - `tests/test_runner.py` — `TestRunnerDetectsNullViolation.test_null_patid_in_demographic`
- **What it verifies:**
  - Unit: `build_validation()` produces a `col_vals_not_null` step for columns where `missing_allowed=False`. A DataFrame with null PatID triggers a failing step after `interrogate()`.
  - Integration: The full runner pipeline (Parquet reader -> schema -> validation -> accumulation) detects null PatID across chunks and reports `all_passed=False`.
- **Phase:** 2 (Task 7), 4 (Tasks 4-5)

#### scdm-qa.AC2.2 — Values outside defined enums produce validation warnings

- **Test type:** Unit + Integration
- **Test files:**
  - `tests/test_validation.py` — `TestBuildValidationEnumMembership.test_invalid_enum_value_fails`
  - `tests/test_runner.py` — `TestRunnerDetectsInvalidEnum.test_invalid_enctype`
- **What it verifies:**
  - Unit: `build_validation()` produces a `col_vals_in_set` step for columns with `allowed_values`. EncType="XX" (not in {AV, ED, IP, IS, OA}) triggers a failing step.
  - Integration: Runner detects invalid EncType in a Parquet file through the full pipeline.
- **Phase:** 2 (Task 7), 4 (Task 5)

#### scdm-qa.AC2.3 — Character columns exceeding spec-defined string lengths produce validation warnings

- **Test type:** Unit + Integration
- **Test files:**
  - `tests/test_validation.py` — `TestBuildValidationStringLength.test_character_column_exceeding_length_fails`
  - `tests/test_runner.py` — (covered implicitly via the full runner tests when over-length data would fail regex checks)
- **What it verifies:** `build_validation()` produces a `col_vals_regex` step with pattern `^.{0,N}$` for Character columns with integer length. A value exceeding the length triggers a failing step.
- **Phase:** 2 (Task 7), 4 (Task 5)

#### scdm-qa.AC2.4 — Duplicate rows on unique key columns produce validation warnings

- **Test type:** Unit
- **Test file:** `tests/test_global_checks.py`
- **Test classes:**
  - `TestUniquenessInMemory.test_detects_duplicate_keys` — PatID="P2" appears in two chunks, 2 duplicate rows detected via in-memory path
  - `TestUniquenessInMemory.test_no_duplicates_passes` — unique keys across chunks, n_failed=0
  - `TestUniquenessDuckDB.test_detects_duplicates_via_duckdb` — duplicate PatID detected via DuckDB SQL path (skipped if DuckDB not installed)
  - `TestUniquenessInMemory.test_returns_none_for_table_without_unique_row` — tables without `unique_row` (e.g., vital_signs) return None
- **What it verifies:** `check_uniqueness()` detects duplicate composite keys via both DuckDB (SQL `GROUP BY ... HAVING COUNT(*) > 1`) and in-memory (Polars `group_by` + filter) paths. The demographic table's `unique_row=["PatID"]` is tested.
- **Phase:** 5, Task 3

#### scdm-qa.AC2.5 — Conditional rules fire correctly (e.g., DDate required when EncType in {IP, IS})

- **Test type:** Unit + Integration
- **Test files:**
  - `tests/test_validation.py`:
    - `TestBuildValidationConditionalRules.test_ddate_null_when_enctype_ip_fails` — DDate=null with EncType=IP triggers conditional not-null failure
    - `TestBuildValidationConditionalRules.test_ddate_null_when_enctype_av_passes` — DDate=null with EncType=AV does not trigger failure (AV is not in the conditional set)
  - `tests/test_parser.py`:
    - `TestConditionalRules.test_ddate_conditional_on_enctype` — parser correctly extracts the conditional rule from free-text `missing_allowed` string
- **What it verifies:** The codegen parser extracts conditional rules from spec text. `build_validation()` uses `pre` (preconditions) to filter the DataFrame to rows matching the condition before running `col_vals_not_null`, so the check only fires for relevant EncType values.
- **Phase:** 2 (Tasks 4, 7), 4 (Task 5)

#### scdm-qa.AC2.6 — Generated schemas cover all 19 SCDM tables from `tables_documentation.json`

- **Test type:** Unit
- **Test files:**
  - `tests/test_parser.py`:
    - `TestParseSpec.test_parses_all_19_tables` — `parse_spec()` returns exactly 19 tables
    - `TestParseSpec.test_all_table_keys_are_unique` — no duplicate keys
    - `TestParseSpec.test_expected_table_keys_present` — all 19 expected keys present
  - `tests/test_validation.py`:
    - `TestSchemaRegistry.test_lists_19_table_keys` — registry contains 19 entries
- **What it verifies:** The parser reads `tables_documentation.json` and produces a TableSchema for every SCDM table. The registry maps all 19 table keys correctly.
- **Phase:** 2, Tasks 4 and 7

---

### scdm-qa.AC3: Basic data profiling

#### scdm-qa.AC3.1 — Per-column completeness rates (% non-null) reported for all columns

- **Test type:** Unit
- **Test file:** `tests/test_profiling.py`
- **Test class/method:** `TestCompletenessRate.test_computes_completeness_across_chunks`
- **What it verifies:** ProfilingAccumulator processes a chunk with 1 null out of 3 PatID values. Asserts `null_count=1`, `total_count=3`, and `completeness_pct` is approximately 66.67%.
- **Phase:** 6, Task 3

#### scdm-qa.AC3.2 — Value frequency distributions reported for enumerated columns

- **Test type:** Unit
- **Test file:** `tests/test_profiling.py`
- **Test class/method:** `TestValueDistribution.test_tracks_enum_frequencies`
- **What it verifies:** ProfilingAccumulator tracks value_counter for columns where the schema defines `allowed_values` (i.e., `is_enumerated=True`). Asserts Sex="F" count is 3 and Sex="M" count is 1 from a 4-row chunk.
- **Phase:** 6, Task 3

#### scdm-qa.AC3.3 — Date columns show min/max range

- **Test type:** Unit
- **Test file:** `tests/test_profiling.py`
- **Test class/method:** `TestDateRange.test_tracks_min_max_across_chunks`
- **What it verifies:** ProfilingAccumulator processes two chunks where Birth_Date ranges [1000,2000] and [500,3000]. Asserts `min_value="500"` and `max_value="3000"` after merging.
- **Phase:** 6, Task 3

#### scdm-qa.AC3.4 — Cardinality counts reported for identifier columns

- **Test type:** Unit
- **Test file:** `tests/test_profiling.py`
- **Test class/method:** `TestCardinality.test_counts_distinct_across_chunks`
- **What it verifies:** Two chunks with PatID values [P1, P2] and [P2, P3] (P2 overlapping). Asserts `distinct_count=3` after deduplication across chunks.
- **Phase:** 6, Task 3

---

### scdm-qa.AC4: Interactive HTML dashboard

#### scdm-qa.AC4.1 — Pointblank HTML report shows pass/fail summary with threshold-based colouring per validation step

- **Test type:** Integration
- **Test file:** `tests/test_reporting.py`
- **Test class/methods:**
  - `TestSaveTableReport.test_creates_html_file` — HTML file is created
  - `TestSaveTableReport.test_html_contains_validation_section` — HTML contains "Validation" and column names
- **What it verifies:** `build_validation_table()` creates a great_tables GT object with conditional fill styling: green (`#d4edda`) for pass rate >= 99%, yellow (`#fff3cd`) for 95-99%, red (`#f8d7da`) for < 95%. `save_table_report()` renders this as self-contained HTML.
- **Phase:** 7, Task 4
- **Note:** Threshold colour correctness is structural (validated by code review of the `tab_style` calls) rather than pixel-level. See Human Verification section.

#### scdm-qa.AC4.2 — Failing row extracts downloadable from report (bounded, capped at configurable limit)

- **Test type:** Integration
- **Test files:**
  - `tests/test_reporting.py` — `TestSaveTableReport.test_html_contains_failing_rows_when_present`
  - `tests/test_accumulator.py` — `TestAccumulatorBoundsFailingRows.test_caps_failing_rows_at_limit`
- **What it verifies:**
  - Accumulator caps failing row extracts at `max_failing_rows` (tested with limit=5, input=10 rows, asserts height <= 5).
  - Report builder wraps failing row tables in `<details>` collapsible sections with a "Download CSV" JavaScript button.
  - HTML output contains "Failing Row" text when failures exist.
- **Phase:** 4 (Task 4), 7 (Task 4)

#### scdm-qa.AC4.3 — Index page links all table reports for multi-table runs

- **Test type:** Unit
- **Test file:** `tests/test_reporting.py`
- **Test class/method:** `TestSaveIndex.test_creates_index_html`
- **What it verifies:** `save_index()` renders a Jinja2 template with report summaries. Asserts the output HTML contains links to `demographic.html` and `encounter.html`, and displays "PASS" / "FAIL" status labels.
- **Phase:** 7, Task 4

---

### scdm-qa.AC5: Handles TB-scale data

#### scdm-qa.AC5.1 — Peak memory stays bounded by chunk size regardless of input file size (Parquet)

- **Test type:** Unit
- **Test file:** `tests/test_readers.py`
- **Test class/methods:**
  - `TestParquetReader.test_chunks_yields_all_rows` — 100-row file read with chunk_size=30, total rows summed correctly
  - `TestParquetReader.test_chunks_respects_chunk_size` — asserts more than 1 chunk produced
- **What it verifies:** ParquetReader uses `scan_parquet().collect_batches(chunk_size=N)` to yield bounded DataFrames. The test proves chunking works, but does not measure peak memory directly. See Human Verification section.
- **Phase:** 3, Task 4

#### scdm-qa.AC5.2 — SAS files read via chunked reader without full materialisation

- **Test type:** Unit (protocol compliance)
- **Test file:** `tests/test_readers.py`
- **Test class/methods:**
  - `TestSasReader.test_implements_table_reader_protocol` — `SasReader` is a subclass of `TableReader` protocol
  - `TestSasReader.test_factory_creates_sas_reader_for_sas_extension` — factory creates SasReader for `.sas7bdat` files
- **What it verifies:** SasReader structurally implements the TableReader protocol (has `metadata()` and `chunks()` methods). Uses `pyreadstat.read_file_in_chunks()` internally. Full integration test requires an actual .sas7bdat fixture file. See Human Verification section.
- **Phase:** 3, Task 5

#### scdm-qa.AC5.3 — DuckDB used for global checks on Parquet when installed; graceful fallback when not

- **Test type:** Unit
- **Test file:** `tests/test_global_checks.py`
- **Test class/methods:**
  - `TestUniquenessDuckDB.test_detects_duplicates_via_duckdb` — uses `pytest.importorskip("duckdb")` to conditionally test the DuckDB path, creating a Parquet file with duplicate keys
  - `TestUniquenessInMemory.test_detects_duplicate_keys` — tests the in-memory fallback path using a `.sas7bdat` extension (forces non-Parquet path)
- **What it verifies:** `_uniqueness_duckdb()` catches `ImportError` and returns None when DuckDB is not installed. The caller (`check_uniqueness()`) falls back to `_uniqueness_in_memory()`. When DuckDB is installed, the SQL path runs `GROUP BY ... HAVING COUNT(*) > 1` directly against the Parquet file.
- **Phase:** 5, Task 3

---

### scdm-qa.AC6: Easy rule authoring

#### scdm-qa.AC6.1 — Adding a new validation rule requires only appending a pointblank method call

- **Test type:** Unit
- **Test file:** `tests/test_custom_rules.py`
- **Test class/method:** `TestApplyCustomRules.test_extends_validation_chain`
- **What it verifies:** A custom rules file that appends `col_vals_not_null(columns='PatID')` to the validation chain produces more steps than the base `build_validation()` chain. This demonstrates that the pointblank API's chainable design makes rule addition a single method call.
- **Phase:** 2, Task 9

#### scdm-qa.AC6.2 — Custom user rules loaded from extension Python file and appended to generated validation chain

- **Test type:** Unit
- **Test file:** `tests/test_custom_rules.py`
- **Test class/methods:**
  - `TestLoadCustomRules.test_returns_none_when_no_dir` — no custom rules dir returns None
  - `TestLoadCustomRules.test_returns_none_when_file_missing` — dir exists but no matching file returns None
  - `TestLoadCustomRules.test_loads_extension_file` — creates `demographic_rules.py` with `extend_validation()`, asserts the function is loaded and callable
  - `TestApplyCustomRules.test_extends_validation_chain` — extension function adds extra validation steps
  - `TestApplyCustomRules.test_noop_when_no_extension` — None extend_fn returns the validation object unchanged
- **What it verifies:** `load_custom_rules()` uses `importlib.util` to dynamically load a Python file named `{table_key}_rules.py` from a configured directory, extracts the `extend_validation` function, and `apply_custom_rules()` calls it to append steps to the pointblank chain.
- **Phase:** 2, Task 9

---

## Human Verification

### scdm-qa.AC1.4 — Exit code 1 (warning) path

- **Why:** The automated tests cover exit code 0 (all pass) and exit code 2 (exceeds threshold), but the exit code 1 case (failures exist but all within threshold) lacks a dedicated test case in the current plan. The `compute_exit_code()` logic handles it, but no integration test creates data with a failure rate below `error_threshold`.
- **Verification approach:** Manually create a Parquet file where exactly 1 row out of 100 has a null in a non-nullable column (1% failure rate, below the default 5% threshold). Run `scdm-qa run config.toml` and verify exit code is 1. Alternatively, add this test case to `tests/test_cli.py` during implementation.

### scdm-qa.AC1.6 — HTTP server actually serves files

- **Why:** The `serve` command launches a blocking HTTP server (`serve_forever()`), which cannot be fully tested via typer's CliRunner without threading/timeout complexity. The existing test only verifies error handling for a missing directory.
- **Verification approach:** After building reports with `scdm-qa run`, manually run `scdm-qa serve ./qa-reports/` and verify in a browser that `http://localhost:8080/` loads the index page, links work, and table reports render correctly. Verify the browser auto-opens.

### scdm-qa.AC4.1 — Threshold-based colouring renders correctly

- **Why:** Automated tests verify that the HTML file contains the validation section, but cannot verify that the great_tables conditional styling (green/yellow/red fill colours) renders visually correct in a browser.
- **Verification approach:** Open a generated `{table}.html` report in a browser. Confirm that:
  - Steps with pass rate >= 99% show green background on the Pass Rate cell
  - Steps with pass rate 95-99% show yellow background
  - Steps with pass rate < 95% show red background

### scdm-qa.AC4.2 — CSV download button works in browser

- **Why:** The download functionality uses JavaScript (`downloadCSV()`) which cannot be tested in a Python test suite.
- **Verification approach:** Open a report with failing rows in a browser. Click the "Download CSV" button. Verify a CSV file downloads with the correct failing row data.

### scdm-qa.AC5.1 — Peak memory actually stays bounded for large files

- **Why:** The automated tests verify chunking with 100 rows, which does not stress memory. TB-scale verification requires actual large files and memory profiling.
- **Verification approach:** Create a Parquet file of at least 1GB (e.g., 50M rows of demographic data). Run `scdm-qa run` with `chunk_size=500000` while monitoring memory via `tracemalloc`, `/usr/bin/time -v`, or similar. Verify peak RSS stays proportional to chunk size (not input size). Repeat with 2x and 4x file sizes to confirm linear chunk-size relationship, not linear file-size relationship.

### scdm-qa.AC5.2 — SAS chunked reading with real .sas7bdat files

- **Why:** The automated tests verify protocol compliance and factory routing, but do not exercise actual SAS file I/O because .sas7bdat files cannot be created from Python without SAS. The `pyreadstat.read_file_in_chunks()` path is untested end-to-end.
- **Verification approach:** Obtain a real .sas7bdat file (e.g., from an existing SCDM dataset). Configure it in `config.toml`. Run `scdm-qa run` and verify that chunks are read, validation runs, and a report is produced. Check logs for chunk iteration messages.

---

## Test File Summary

| Test File | Phase | ACs Covered |
|---|---|---|
| `tests/test_logging.py` | 1 | (infrastructure) |
| `tests/test_config.py` | 1 | (infrastructure) |
| `tests/test_cli.py` | 1, 8 | AC1.1, AC1.2, AC1.3, AC1.4, AC1.5, AC1.6 |
| `tests/test_parser.py` | 2 | AC2.5, AC2.6 |
| `tests/test_validation.py` | 2 | AC2.1, AC2.2, AC2.3, AC2.5, AC2.6 |
| `tests/test_custom_rules.py` | 2 | AC6.1, AC6.2 |
| `tests/test_readers.py` | 3 | AC5.1, AC5.2 |
| `tests/test_accumulator.py` | 4 | AC2.1, AC2.2, AC4.2 |
| `tests/test_runner.py` | 4 | AC2.1, AC2.2, AC2.3, AC2.5 |
| `tests/test_global_checks.py` | 5 | AC2.4, AC5.3 |
| `tests/test_profiling.py` | 6 | AC3.1, AC3.2, AC3.3, AC3.4 |
| `tests/test_reporting.py` | 7 | AC4.1, AC4.2, AC4.3 |

---

## Available Test Data

Real SCDM Parquet files are available at `/Users/scarndp/dev/numina-systems/scdm-prepare/output/` for 9 tables:

| File | Table Key |
|---|---|
| `death.parquet` | `death` |
| `demographic.parquet` | `demographic` |
| `diagnosis.parquet` | `diagnosis` |
| `dispensing.parquet` | `dispensing` |
| `encounter.parquet` | `encounter` |
| `enrollment.parquet` | `enrollment` |
| `facility.parquet` | `facility` |
| `procedure.parquet` | `procedure` |
| `provider.parquet` | `provider` |

These can be used for:
- **Human verification** of all AC1.x, AC4.x, and AC5.x criteria (point a `config.toml` at these files)
- **Integration smoke tests** during development (validate against real SCDM data to catch schema/parser issues early)
- **Memory profiling** for AC5.1 (if any files are large enough to stress chunk boundaries)

**Note:** These files are outside the project repo. Do not copy them into the project — reference them via absolute path in test configs only.

---

## Coverage Gaps and Recommendations

1. **Add explicit exit code 1 test.** Create test data with a failure rate between 0% and `error_threshold` to verify the warning exit path. This is a straightforward addition to `TestRunCommand` in `tests/test_cli.py`.

2. **Add SAS integration test fixture.** If a sample .sas7bdat file can be committed to the repo (even a small synthetic one), add integration tests in `tests/test_readers.py` that exercise `SasReader.metadata()` and `SasReader.chunks()` end-to-end.

3. **Consider a memory benchmark test.** A pytest benchmark or `memray`-based test could automate AC5.1 verification by asserting peak memory stays below a threshold for a moderately large Parquet file.

4. **Consider a Playwright/Selenium test for AC4.1/AC4.2.** If browser-level verification is important, a test could render a report and assert CSS background colours on specific cells.

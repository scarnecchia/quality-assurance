# Human Test Plan: Cross-Table Code Checks

## Prerequisites

- Python 3.12+ environment with `uv` installed
- Project dependencies installed (`uv sync`)
- `uv run pytest` passing (316 tests, 0 failures)
- Access to sample Parquet data files (or ability to generate them via the CLI)
- Optionally: a real SAS7BDAT file for AC1.12 manual verification

## Phase 1: Cross-Table Report Visual Verification

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | Create a TOML config pointing to at least `demographic` and `enrollment` parquet files with some overlapping PatIDs and some orphans. Run `uv run scdm-qa run config.toml` | Pipeline completes with exit code 0 or 1. Output directory contains per-table HTML reports. |
| 1.2 | Open the generated `cross_table.html` in a browser | Page loads without errors. Heading shows "Cross-Table Checks". |
| 1.3 | Inspect the validation results table in `cross_table.html` | Table displays columns for check ID, description, pass count, fail count, severity. Rows correspond to executed cross-table checks (201, 203, 205, 206, 209, 224, 227). |
| 1.4 | Confirm no "Data Profile" section appears in `cross_table.html` | Since cross-table checks have no per-column profiling, the profiling section should be absent or empty. No broken layout. |
| 1.5 | Open `index.html` in a browser | Index page loads. Contains a row/link for "Cross-Table Checks" pointing to `cross_table.html`. Shows total failures count. |
| 1.6 | Click the "Cross-Table Checks" link in the index | Navigates to `cross_table.html` correctly (no 404, no broken relative path). |

## Phase 2: SAS7BDAT Conversion Smoke Test

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Create a TOML config where one table path points to a real SAS7BDAT file (e.g., `diagnosis = "/path/to/diagnosis.sas7bdat"`). Other tables can be Parquet. | Config loads without error. |
| 2.2 | Run `uv run scdm-qa run config.toml` | Pipeline does not crash. Log output contains a warning about SAS-to-Parquet conversion. |
| 2.3 | Verify cross-table checks execute against the SAS-sourced table | Cross-table results include checks involving the SAS table (e.g., referential integrity check 201 if the SAS file is a diagnosis table). |
| 2.4 | After the run completes, check the temp directory for leftover parquet files | No orphaned temp parquet files remain. Cleanup was successful. |

## Phase 3: CLI Flag Interaction

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | Run `uv run scdm-qa run config.toml --l1-only` | Only per-table reports generated. No `cross_table.html` in output. Log shows L2 skipped. |
| 3.2 | Run `uv run scdm-qa run config.toml --l2-only` | Only `cross_table.html` generated (no per-table reports). Log shows L1 skipped. |
| 3.3 | Run `uv run scdm-qa run config.toml` (no flags) | Both per-table reports and `cross_table.html` generated. |
| 3.4 | Run `uv run scdm-qa run config.toml --l1-only --l2-only` | Exit code 2. Error message about mutual exclusion printed to stderr/console. |
| 3.5 | Create TOML with `run_l2 = false`. Run `uv run scdm-qa run config.toml --l2-only` | CLI flag overrides TOML: cross-table checks run despite TOML saying `run_l2 = false`. |
| 3.6 | Run `uv run scdm-qa run config.toml --table diagnosis` | Only diagnosis per-table report created. Cross-table checks scoped to those involving diagnosis table only. |

## Phase 4: Code/CodeType Validation Spot Check

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | Prepare a diagnosis Parquet file with rows containing `DX="250.00"`, `Dx_Codetype="09"` (decimal in ICD-9). Run the pipeline. | Check 223 (no_decimal) fires for the decimal-containing row. The report shows a format check failure. |
| 4.2 | Prepare a procedure Parquet file with `PX="ABC"`, `PX_CodeType="C4"`. Run the pipeline. | Check 223 (regex) flags the invalid CPT-4 code. |
| 4.3 | Prepare a diagnosis Parquet file with `DX="250"`, `Dx_Codetype="09"`, `ADate=20200101`. Run the pipeline. | Check 223 (era_date) flags ICD-9 code used after the 2015-10-01 transition. |
| 4.4 | Prepare a diagnosis Parquet file with `DX="25"` (length 2), `Dx_Codetype="09"`. Run the pipeline. | Check 228 (length) flags ICD-9 code below minimum length of 3. |

## End-to-End: Full Pipeline with Mixed Severity Results

1. Create a multi-table config with `demographic`, `enrollment`, `diagnosis`, `encounter` Parquet files.
2. Introduce data issues: orphan PatID in diagnosis (not in enrollment), ICD-9 code with decimal, Enr_Start before Birth_Date for one patient.
3. Run `uv run scdm-qa run config.toml`.
4. Verify exit code is 1 or 2 depending on failure rate vs threshold.
5. Open `index.html`: confirm all table reports and cross-table report are linked.
6. Open each report: confirm check IDs, descriptions, and failure counts match expected violations.
7. Run again with `--l1-only`: verify exit code only reflects L1 failures.
8. Run again with `--l2-only`: verify exit code only reflects L2 (cross-table) failures.

## Human Verification Required

| Criterion | Why Manual | Steps |
|-----------|------------|-------|
| AC1.9 Cross-table HTML report layout | Visual layout, readability, and correct rendering of cross-table results (which lack profiling data) cannot be fully automated | Open `cross_table.html` in a browser. Verify heading, validation table layout, absence of broken profiling section, and index page link. |
| AC1.12 SAS7BDAT real-file behaviour | Creating valid SAS fixtures in test environments may not be portable; real SAS files exercise pyreadstat edge cases | Run pipeline with a real SAS7BDAT file. Verify no crash, cross-table checks produce results, temp files cleaned up. |
| Code check report clarity | Whether check descriptions and failure details are human-readable in the HTML report | Open a per-table report (e.g., diagnosis) after triggering code check failures. Verify check 223/228 results display clearly with check IDs, descriptions, and failing row samples. |

## Traceability

| Acceptance Criterion | Automated Test | Manual Step |
|----------------------|----------------|-------------|
| AC1.1 | `test_cross_table_checks.py::TestCrossTableChecksParser` | -- |
| AC1.2 | `test_cross_table_engine.py::TestReferentialIntegrity` | -- |
| AC1.3 | `test_cross_table_engine.py::TestLengthConsistency` | -- |
| AC1.4 | `test_cross_table_engine.py::TestCrossDateCompare::test_detects_enr_start_before_birth_date` | -- |
| AC1.5 | `test_cross_table_engine.py::TestCrossDateCompare::test_detects_adate_before_birth_date` | -- |
| AC1.6 | `test_cross_table_engine.py::TestCrossDateCompare::test_detects_postal_code_date_before_birth_date` | -- |
| AC1.7 | `test_cross_table_engine.py::TestLengthExcess` | -- |
| AC1.8 | `test_cross_table_engine.py::TestColumnMismatch` | -- |
| AC1.9 | `test_pipeline_phases.py::TestCrossTableReporting`, `test_reporting.py::TestSaveTableReport__EmptyProfiling` | Phase 1 steps 1.1-1.6 |
| AC1.10 | `test_cross_table_engine.py::test_missing_reference_table_is_skipped` | -- |
| AC1.11 | `test_cross_table_engine.py::TestErrorHandling` | -- |
| AC1.12 | `test_cross_table_engine.py::test_temp_parquet_cleanup` | Phase 2 steps 2.1-2.4 |
| AC2.1 | `test_code_checks.py::TestCodeChecksParser` | -- |
| AC2.2 | `test_code_checks.py::test_get_format_checks_for_diagnosis_table`, `test_get_length_checks_for_procedure_table` | -- |
| AC2.3 | `test_code_check_validation.py::TestFormatCheckNoDecimal` | Phase 4, step 4.1 |
| AC2.4 | `test_code_check_validation.py::TestFormatCheckRegex` | Phase 4, step 4.2 |
| AC2.5 | `test_code_check_validation.py::TestFormatCheckEraDate` | Phase 4, step 4.3 |
| AC2.6 | `test_code_check_validation.py::TestFormatCheckConditionalPresence` | -- |
| AC2.7 | `test_code_check_validation.py::TestLengthCheckValidation` | Phase 4, step 4.4 |
| AC2.8 | `test_code_checks.py::test_malformed_json_raises_config_error` | -- |
| AC2.9 | `test_code_check_validation.py::test_null_codetype_row_not_flagged`, `test_null_codetype_length_check_skipped` | -- |
| AC3.1 | `test_cli.py::test_l1_only_flag_succeeds`, `test_pipeline_phases.py::test_l1_only_no_cross_table_outcome` | Phase 3, step 3.1 |
| AC3.2 | `test_cli.py::test_l2_only_flag_succeeds`, `test_pipeline_phases.py::test_l2_only_no_per_table_outcomes` | Phase 3, step 3.2 |
| AC3.3 | `test_cli.py::test_no_flags_succeeds`, `test_pipeline_phases.py::test_both_l1_and_l2_executes_both` | Phase 3, step 3.3 |
| AC3.4 | `test_cli.py::test_l1_only_and_l2_only_together_exits_2` | Phase 3, step 3.4 |
| AC3.5 | `test_config.py::TestL1L2ConfigOptions` | -- |
| AC3.6 | `test_cli.py::test_l1_only_overrides_toml_run_l2_true`, `test_l2_only_overrides_toml_config` | Phase 3, step 3.5 |
| AC3.7 | `test_pipeline_phases.py::test_l2_with_table_filter_filters_checks` | Phase 3, step 3.6 |
| AC3.8 | `test_pipeline_phases.py::TestExitCodeWithCrossTableResults` | End-to-End steps 4, 7, 8 |

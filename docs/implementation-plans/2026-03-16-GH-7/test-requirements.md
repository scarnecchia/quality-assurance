# Test Requirements: DuckDB Global Checks Migration (GH-7)

## Automated Tests

| AC ID | Criterion | Test Type | Expected Test Location | Phase |
|-------|-----------|-----------|----------------------|-------|
| GH-7.AC1.1 | `check_uniqueness` executes SQL against DuckDB view, returns valid StepResult | unit | tests/test_global_checks.py::TestUniqueness | 2 |
| GH-7.AC1.1 | `check_overlapping_spans` executes SQL against DuckDB view, returns valid StepResult | unit | tests/test_global_checks.py::TestOverlappingSpans | 2 |
| GH-7.AC1.1 | `check_sort_order` executes SQL against DuckDB view, returns valid StepResult | unit | tests/test_global_checks.py::TestSortOrder | 3 |
| GH-7.AC1.1 | `check_not_populated` executes SQL against DuckDB view, returns valid StepResult | unit | tests/test_global_checks.py::TestNotPopulated | 3 |
| GH-7.AC1.1 | `check_date_ordering` executes SQL against DuckDB view, returns valid StepResult | unit | tests/test_global_checks.py::TestDateOrdering | 4 |
| GH-7.AC1.1 | `check_cause_of_death` executes SQL against DuckDB view, returns valid StepResult | unit | tests/test_global_checks.py::TestCauseOfDeath | 4 |
| GH-7.AC1.1 | `check_enrollment_gaps` executes SQL against DuckDB view, returns valid StepResult | unit | tests/test_global_checks.py::TestEnrollmentGaps | 5 |
| GH-7.AC1.1 | `check_enc_combinations` executes SQL against DuckDB view, returns valid StepResult | unit | tests/test_global_checks.py::TestEncCombinations | 5 |
| GH-7.AC1.2 | No `pl.concat()` calls remain in global check code paths | static | Verified by grep in Phase 6 Task 4; also implicitly verified by all unit tests using DuckDB views exclusively | 6 |
| GH-7.AC1.3 | `check_uniqueness` accepts `conn: DuckDBPyConnection` and `view_name: str` (not chunk iterators) | unit | tests/test_global_checks.py::TestUniqueness (all tests call with conn + view_name) | 2 |
| GH-7.AC1.3 | `check_overlapping_spans` accepts `conn` + `view_name` | unit | tests/test_global_checks.py::TestOverlappingSpans | 2 |
| GH-7.AC1.3 | `check_sort_order` accepts `conn` + `view_name` | unit | tests/test_global_checks.py::TestSortOrder | 3 |
| GH-7.AC1.3 | `check_not_populated` accepts `conn` + `view_name` | unit | tests/test_global_checks.py::TestNotPopulated | 3 |
| GH-7.AC1.3 | `check_date_ordering` accepts `conn` + `view_name` | unit | tests/test_global_checks.py::TestDateOrdering | 4 |
| GH-7.AC1.3 | `check_cause_of_death` accepts `conn` + `view_name` | unit | tests/test_global_checks.py::TestCauseOfDeath | 4 |
| GH-7.AC1.3 | `check_enrollment_gaps` accepts `conn` + `view_name` | unit | tests/test_global_checks.py::TestEnrollmentGaps | 5 |
| GH-7.AC1.3 | `check_enc_combinations` accepts `conn` + `view_name` | unit | tests/test_global_checks.py::TestEncCombinations | 5 |
| GH-7.AC2.1 | `_uniqueness_in_memory()` function is deleted | unit | tests/test_global_checks.py::TestUniqueness (old TestUniquenessInMemory and TestUniquenessDuckDB classes replaced by single TestUniqueness; fallback tests removed) | 2 |
| GH-7.AC2.2 | `_overlapping_spans_in_memory()` function is deleted | unit | tests/test_global_checks.py::TestOverlappingSpans (old fallback test `test_duckdb_fallback_to_in_memory` removed) | 2 |
| GH-7.AC2.3 | No conditional fallback logic remains | unit | tests/test_global_checks.py (all fallback tests deleted; all tests use DuckDB exclusively -- if any fallback path existed, the tests would fail because they only provide conn + view_name) | 2 |
| GH-7.AC3.1 | Sort order violation within a single logical chunk is detected | unit | tests/test_global_checks.py::TestSortOrder::test_detects_intra_chunk_sort_violation (new test: rows [P3, P1, P2] in a single Parquet file) | 3 |
| GH-7.AC3.2 | Correctly sorted file passes with zero violations | unit | tests/test_global_checks.py::TestSortOrder::test_correctly_sorted_passes | 3 |
| GH-7.AC3.3 | File with equal adjacent rows in sort columns passes | unit | tests/test_global_checks.py::TestSortOrder::test_equal_adjacent_rows_pass (new test: [P1, P1, P2]) | 3 |
| GH-7.AC4.1 | Date ordering violations detected when date_a > date_b | unit | tests/test_global_checks.py::TestDateOrdering::test_detects_violations_adate_greater_than_ddate | 4 |
| GH-7.AC4.2 | Rows where either date is null are skipped | unit | tests/test_global_checks.py::TestDateOrdering::test_skips_rows_with_null_dates | 4 |
| GH-7.AC4.3 | Patients missing CauseType='U' detected (check 236) | unit | tests/test_global_checks.py::TestCauseOfDeath::test_check_236_detects_missing_underlying_cause | 4 |
| GH-7.AC4.4 | Patients with multiple CauseType='U' detected (check 237) | unit | tests/test_global_checks.py::TestCauseOfDeath::test_check_237_detects_multiple_underlying_causes | 4 |
| GH-7.AC4.5 | Failing row samples bounded by max_failing_rows | unit | tests/test_global_checks.py::TestDateOrdering::test_failing_rows_sampled, TestCauseOfDeath::test_failing_rows_are_capped | 4 |
| GH-7.AC5.1 | Overlapping enrollment spans detected via LAG window | unit | tests/test_global_checks.py::TestOverlappingSpans::test_detects_overlapping_spans | 2 |
| GH-7.AC5.2 | Non-bridged enrollment gaps detected (prev_end + 1 day < Enr_Start) | unit | tests/test_global_checks.py::TestEnrollmentGaps::test_detects_gaps | 5 |
| GH-7.AC5.3 | Adjacent spans (Enr_End + 1 day == next Enr_Start) pass | unit | tests/test_global_checks.py::TestEnrollmentGaps::test_adjacent_spans_pass | 5 |
| GH-7.AC5.4 | Invalid ENC field combinations flagged per combination rules | unit | tests/test_global_checks.py::TestEncCombinations::test_invalid_combo_ip_missing_ddate | 5 |
| GH-7.AC5.5 | EncType rate threshold violations detected (check 245) | unit | tests/test_global_checks.py::TestEncCombinations::test_threshold_exceeded_check_245 | 5 |
| GH-7.AC5.6 | Unknown EncType values flagged as invalid | unit | tests/test_global_checks.py::TestEncCombinations::test_unknown_enctype_flagged | 5 |
| GH-7.AC6.1 | Pipeline skips global checks for SAS files with a logged warning | integration | tests/test_pipeline_phases.py::TestSASFileGlobalCheckSkip::test_sas_file_skips_global_checks | 1, 6 |
| GH-7.AC6.2 | SAS files do not cause errors or crashes -- graceful skip | integration | tests/test_pipeline_phases.py::TestSASFileGlobalCheckSkip::test_sas_file_skips_global_checks (asserts outcome.success is True) | 1, 6 |
| GH-7.AC7.1 | check_id "211" unchanged for uniqueness | unit | tests/test_global_checks.py::TestUniqueness (assert check_id == "211") | 2 |
| GH-7.AC7.1 | check_id "215" unchanged for overlapping spans | unit | tests/test_global_checks.py::TestOverlappingSpans::test_check_id_215_and_severity_fail | 2 |
| GH-7.AC7.1 | check_id "102" unchanged for sort order | unit | tests/test_global_checks.py::TestSortOrder (assert check_id == "102") | 3 |
| GH-7.AC7.1 | check_id "111" unchanged for not populated | unit | tests/test_global_checks.py::TestNotPopulated::test_check_id_is_111 | 3 |
| GH-7.AC7.1 | check_id "226" unchanged for date ordering | unit | tests/test_global_checks.py::TestDateOrdering::test_check_id_is_226 | 4 |
| GH-7.AC7.1 | check_ids "236" and "237" unchanged for cause of death | unit | tests/test_global_checks.py::TestCauseOfDeath::test_check_ids_are_236_and_237 | 4 |
| GH-7.AC7.1 | check_id "216" unchanged for enrollment gaps | unit | tests/test_global_checks.py::TestEnrollmentGaps::test_check_id_216_and_severity_warn | 5 |
| GH-7.AC7.1 | check_ids "244" and "245" unchanged for ENC combinations | unit | tests/test_global_checks.py::TestEncCombinations::test_check_244_has_correct_id | 5 |
| GH-7.AC7.2 | Severity "Fail" for uniqueness (211) | unit | tests/test_global_checks.py::TestUniqueness (assert severity == "Fail") | 2 |
| GH-7.AC7.2 | Severity "Fail" for overlapping spans (215) | unit | tests/test_global_checks.py::TestOverlappingSpans::test_check_id_215_and_severity_fail | 2 |
| GH-7.AC7.2 | Severity "Fail" for sort order (102) | unit | tests/test_global_checks.py::TestSortOrder (assert severity == "Fail") | 3 |
| GH-7.AC7.2 | Severity from registry for not populated (111) | unit | tests/test_global_checks.py::TestNotPopulated::test_severity_from_registry | 3 |
| GH-7.AC7.2 | Severity from registry for date ordering (226) | unit | tests/test_global_checks.py::TestDateOrdering::test_severity_from_registry | 4 |
| GH-7.AC7.2 | Severity "Fail" for cause of death (236, 237) | unit | tests/test_global_checks.py::TestCauseOfDeath::test_severity_is_fail | 4 |
| GH-7.AC7.2 | Severity "Warn" for enrollment gaps (216) | unit | tests/test_global_checks.py::TestEnrollmentGaps::test_check_id_216_and_severity_warn | 5 |
| GH-7.AC7.2 | Severity "Fail" for ENC combinations (244, 245) | unit | tests/test_global_checks.py::TestEncCombinations::test_check_244_has_correct_id | 5 |
| GH-7.AC7.3 | StepResult shape unchanged -- all returned objects have expected fields and types | unit | tests/test_global_checks.py (all test classes assert StepResult fields: step_index, assertion_type, column, description, n_passed, n_failed, failing_rows, check_id, severity) | 2-5 |
| GH-7.AC7.4 | n_passed + n_failed counts match expected values for identical test data | unit | tests/test_global_checks.py (all test classes verify n_passed and n_failed against fixture data) | 2-5 |
| GH-7.AC7.4 | Parquet files produce expected global check results in pipeline | integration | tests/test_pipeline_phases.py::TestSASFileGlobalCheckSkip::test_parquet_file_produces_global_checks | 6 |

## Human Verification

| AC ID | Criterion | Justification | Verification Approach |
|-------|-----------|---------------|----------------------|
| GH-7.AC1.2 | No `pl.concat()` calls remain in any global check code path | Absence of dead code is best confirmed by static search since tests cannot prove code does not exist. | Run `grep -n "pl.concat" src/scdm_qa/validation/global_checks.py` during Phase 6 Task 4. Must return no matches. |
| GH-7.AC2.1 | `_uniqueness_in_memory()` function is deleted | Function deletion cannot be asserted by a unit test -- the function simply would not be called. | Run `grep -n "_uniqueness_in_memory" src/scdm_qa/validation/global_checks.py` during Phase 6 Task 4. Must return no matches. Reviewer confirms during code review. |
| GH-7.AC2.2 | `_overlapping_spans_in_memory()` function is deleted | Same rationale as AC2.1. | Run `grep -n "_overlapping_spans_in_memory" src/scdm_qa/validation/global_checks.py` during Phase 6 Task 4. Must return no matches. Reviewer confirms during code review. |
| GH-7.AC2.3 | No conditional fallback logic remains | Absence of branching logic is a structural property, not a behavioural one. | Run `grep -n "_in_memory\|fallback\|pl\.concat" src/scdm_qa/validation/global_checks.py` during Phase 6 Task 4. Must return no matches. |
| GH-7.AC6.1 | Pipeline skips global checks for SAS files with a logged warning (log content) | Verifying specific structlog warning message content requires log capture that may be fragile. | Reviewer inspects structlog output during manual pipeline run with a SAS file, or extend integration test with `structlog.testing.capture_logs()` to assert warning contains "skipping global checks". |

## Test Fixture Migration Notes

All existing test classes in `tests/test_global_checks.py` currently use in-memory polars DataFrames passed as chunk iterators. During Phases 2-5, each test class is migrated to:

1. Write Parquet temp files via `pl.DataFrame.write_parquet()` using pytest `tmp_path`
2. Open a DuckDB connection via `create_connection()`
3. Register the Parquet file as a named view
4. Call the check function with `conn` + `view_name` instead of `chunks`
5. Close the connection in a `finally` block

This follows the pattern established in `tests/test_cross_table_engine.py`.

### Test Classes Deleted During Migration

The following test classes/methods are removed (they test removed functionality):

- `TestUniquenessInMemory` (replaced by `TestUniqueness`) -- Phase 2
- `TestUniquenessDuckDB` (merged into `TestUniqueness`) -- Phase 2
- `TestUniquenessDuckDB::test_fallback_to_in_memory_when_duckdb_unavailable` -- Phase 2
- `TestOverlappingSpans::test_duckdb_fallback_to_in_memory` -- Phase 2
- `TestOverlappingSpans::test_duckdb_fast_path_with_parquet` (logic absorbed into standard tests) -- Phase 2
- `TestGlobalCheckCheckIds` (check_id assertions absorbed into per-check test classes) -- Phase 2
- All `test_multiple_chunks_accumulation` tests (single SQL query replaces multi-chunk accumulation) -- Phases 3-5
- All `test_*_across_chunks` tests (DuckDB handles full table natively) -- Phases 2-5

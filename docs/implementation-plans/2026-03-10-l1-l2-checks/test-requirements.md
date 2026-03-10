# Test Requirements: L1 & L2 Validation Checks

Generated from: docs/design-plans/2026-03-10-l1-l2-checks.md

## Automated Test Coverage

### AC1: L1 checks detect column-level data quality issues

| AC ID | Description | Test Type | Test File | Phase |
|-------|-------------|-----------|-----------|-------|
| l1-l2-checks.AC1.1 | Check 111 flags columns with zero non-null records as not populated | unit | tests/test_global_checks.py | 3 |
| l1-l2-checks.AC1.2 | Check 122 flags character values with leading whitespace | unit | tests/test_l1_checks.py | 2 |
| l1-l2-checks.AC1.3 | Check 124 flags numeric columns containing zero values as suspicious | unit | tests/test_l1_checks.py | 2 |
| l1-l2-checks.AC1.4 | Check 128 flags PostalCode values containing non-numeric characters | unit | tests/test_l1_checks.py | 2 |
| l1-l2-checks.AC1.5 | Columns with all nulls EXCEPT check-111 targets still pass (only specific columns are checked) | unit | tests/test_global_checks.py | 3 |
| l1-l2-checks.AC1.6 | Null values in L1 check columns are not flagged (na_pass=True for 122, 124, 128) | unit | tests/test_l1_checks.py | 2 |

**Test details:**

- **AC1.1:** Call `check_not_populated()` with an encounter schema and chunks where DDate column is entirely null. Assert StepResult for DDate has `n_failed == total_rows`, `n_passed == 0`, and `check_id == "111"`. Separate test: column with at least one non-null value passes (`n_failed == 0`).
- **AC1.2:** Write encounter parquet with DRG values including `" X123"` (leading space). Run `run_validation`. Assert step with `check_id="122"` has `n_failed > 0`. Separate clean-data test asserts `n_failed == 0`.
- **AC1.3:** Write dispensing parquet with RxSup containing a zero value. Run `run_validation`. Assert step with `check_id="124"` has `n_failed > 0`. Separate clean-data test asserts `n_failed == 0`.
- **AC1.4:** Write demographic parquet with PostalCode containing `"K1A0B1"`. Run `run_validation`. Assert step with `check_id="128"` has `n_failed > 0`. Separate clean-data test asserts `n_failed == 0`.
- **AC1.5:** Call `check_not_populated()` with demographic schema where `Race` column is all null but `Race` is NOT a check-111 target. Assert no StepResult for `Race` is produced. Only `ImputedHispanic` and `ImputedRace` produce results.
- **AC1.6:** For checks 122, 124, 128: include null values in target columns. Assert nulls do NOT contribute to `n_failed` (verify `na_pass=True` behaviour).

---

### AC2: L2 checks detect cross-record data quality issues

| AC ID | Description | Test Type | Test File | Phase |
|-------|-------------|-----------|-----------|-------|
| l1-l2-checks.AC2.1 | Check 226 flags rows where date_a > date_b for all configured pairs | unit | tests/test_global_checks.py | 4 |
| l1-l2-checks.AC2.2 | Check 215 flags overlapping enrollment spans within the same PatID | unit | tests/test_global_checks.py | 6 |
| l1-l2-checks.AC2.3 | Check 236 flags patients in COD with no CauseType='U' record | unit | tests/test_global_checks.py | 5 |
| l1-l2-checks.AC2.4 | Check 244 flags ENC rows not matching valid combination rules | unit | tests/test_global_checks.py | 7 |
| l1-l2-checks.AC2.5 | Check 245 flags EncType groups exceeding rate threshold for invalid combos | unit | tests/test_global_checks.py | 7 |
| l1-l2-checks.AC2.6 | Check 226 does not flag rows where either date is null | unit | tests/test_global_checks.py | 4 |
| l1-l2-checks.AC2.7 | Check 237 flags patients with >1 CauseType='U'; patient with exactly 1 'U' passes both 236 and 237 | unit | tests/test_global_checks.py | 5 |
| l1-l2-checks.AC2.8 | Check 216 flags enrollment gaps; adjacent spans (Enr_End + 1 day = next Enr_Start) pass | unit | tests/test_global_checks.py | 6 |

**Test details:**

- **AC2.1:** Create encounter-schema chunk iterator with rows where `ADate > DDate`. Call `check_date_ordering()`. Assert `n_failed > 0` and `check_id="226"`. Separate clean-data test: all rows have `ADate <= DDate`, assert `n_failed == 0`. Multi-chunk test: violations spread across chunks accumulate correctly.
- **AC2.2:** Create enrollment data with patient P1 having overlapping spans (e.g., `Enr_Start=100, Enr_End=200` and `Enr_Start=150, Enr_End=300`). Assert `check_overlapping_spans()` returns `n_failed > 0` with `check_id="215"`. Non-overlapping spans test: `n_failed == 0`. DuckDB fast path test (parquet file, `pytest.importorskip("duckdb")`). DuckDB fallback test (mock `_overlapping_spans_duckdb` to return None).
- **AC2.3:** Create COD data with patient P1 having `CauseType='C'` and `'I'` but no `'U'`. Call `check_cause_of_death()` with cause_of_death schema. Assert check 236 result has `n_failed >= 1`.
- **AC2.4:** Create encounter data with IP row where DDate is null (IP requires DDate Present). Assert check 244 result has `n_failed > 0` and `check_id="244"`. Valid combo test: IP row with DDate present + Discharge_Disposition + Discharge_Status set, assert `n_failed == 0`. AV-with-nulls test: AV row with all optional fields null passes.
- **AC2.5:** Create encounter data with many IP rows, >5% having invalid combos. Assert check 245 result for `EncType=IP` has `n_failed > 0` and `check_id="245"`. Below-threshold test: <5% invalid combos, assert `n_failed == 0`.
- **AC2.6:** Create encounter rows where ADate is null or DDate is null. Assert those rows are NOT counted as failures (`n_failed == 0` for null-date rows).
- **AC2.7:** Two sub-tests. (1) Patient P2 with exactly one `CauseType='U'`: assert check 236 `n_failed == 0` and check 237 `n_failed == 0`. (2) Patient P3 with two `CauseType='U'` records: assert check 237 `n_failed >= 1`.
- **AC2.8:** Two sub-tests. (1) Gap detected: `Enr_End=100`, next `Enr_Start=200`, assert `check_enrollment_gaps()` returns `n_failed > 0` with `check_id="216"`. (2) Adjacent passes: `Enr_End=100`, next `Enr_Start=101` (or `Enr_End + 1 day == next Enr_Start` for date types), assert `n_failed == 0`.

---

### AC3: StepResult carries SAS CheckID cross-reference

| AC ID | Description | Test Type | Test File | Phase |
|-------|-------------|-----------|-----------|-------|
| l1-l2-checks.AC3.1 | New checks produce StepResult with check_id set to SAS CheckID string | unit | tests/test_accumulator.py | 1 |
| l1-l2-checks.AC3.2 | Existing checks produce StepResult with check_id=None | unit | tests/test_accumulator.py, tests/test_global_checks.py | 1 |
| l1-l2-checks.AC3.3 | check_id field does not break serialisation or reporting of existing results | unit | tests/test_reporting.py | 1, 8 |

**Test details:**

- **AC3.1:** In `TestAccumulatorPropagatesCheckId`, add chunk results with `check_id="122"` in the 6-element step description tuple. Call `acc.result()`. Assert `result.steps[0].check_id == "122"`.
- **AC3.2:** Construct StepResult without `check_id` kwarg, assert `.check_id is None`. In accumulator tests, pass 6-element tuples with `None` as 5th element, assert `check_id is None` on result. In global checks tests, assert existing `check_uniqueness()` and `check_sort_order()` produce `check_id=None`.
- **AC3.3:** Run existing reporting test suite without modification (Phase 1 Task 7). Phase 8 Task 1: create StepResult with `check_id="122"` and one with `check_id=None`, build validation table, assert output HTML contains `"122"` and dash placeholder.

---

### AC4: Severity levels match SAS reference

| AC ID | Description | Test Type | Test File | Phase |
|-------|-------------|-----------|-----------|-------|
| l1-l2-checks.AC4.1 | Checks marked Fail in SAS reference produce error-level results | unit | tests/test_l1_checks.py, tests/test_global_checks.py, tests/test_l1_l2_integration.py | 2, 3, 4, 5, 6, 7, 8 |
| l1-l2-checks.AC4.2 | Checks marked Note in SAS reference produce informational results | unit | tests/test_global_checks.py, tests/test_l1_l2_integration.py | 3, 8 |
| l1-l2-checks.AC4.3 | Checks marked Warn in SAS reference produce warning-level results | unit | tests/test_global_checks.py, tests/test_l1_l2_integration.py | 6, 8 |

**Test details:**

- **AC4.1:** Verified across all check-specific tests. Each Fail-severity check (111/PDX, 111/DDate, 111/Discharge_Disposition, 111/Discharge_Status, 111/Admitting_Source, 215, 226/ENC, 226/ENR, 236, 237, 244, 245) carries `severity="Fail"` on its StepResult. Phase 8 Task 2: `compute_exit_code` with `severity="Fail"` StepResult having `f_failed > threshold` returns exit code 2.
- **AC4.2:** Check 111 for ImputedHispanic, ImputedRace, PlanType, PayerType carries `severity="Note"`. Phase 3 tests verify registry severity values. Phase 8 Task 2: `compute_exit_code` with `severity="Note"` StepResult having `n_failed > 0` returns exit code 0 (Note checks are informational only, never escalate).
- **AC4.3:** Check 216 (enrollment gaps) carries `severity="Warn"`. Check 122, 124, 128 carry `severity="Warn"`. Phase 6 tests assert `check_id="216"` StepResult has Warn severity. Phase 8 Task 2: `compute_exit_code` with `severity="Warn"` StepResult having `n_failed > 0` but below threshold returns exit code 1.

---

### AC5: Backward compatibility

| AC ID | Description | Test Type | Test File | Phase |
|-------|-------------|-----------|-----------|-------|
| l1-l2-checks.AC5.1 | All pre-existing validation tests pass without modification | regression | tests/ (full suite) | 1, 8 |
| l1-l2-checks.AC5.2 | Pipeline exit codes correctly reflect new check outcomes alongside existing checks | unit, integration | tests/test_l1_l2_integration.py | 8 |

**Test details:**

- **AC5.1:** Phase 1 Task 7: run `uv run pytest tests/ -v` after StepResult changes, confirm zero failures. Phase 8 Task 4: run full test suite after all phases complete, confirm zero failures. Each phase's verification step also runs the full suite.
- **AC5.2:** Phase 8 Task 2: test `compute_exit_code()` with `TableOutcome` containing new check StepResults alongside existing checks. Violations in new checks produce correct exit codes (0/1/2). Phase 8 Task 3: integration test runs full pipeline with multi-table config, calls `compute_exit_code(outcomes)`, asserts non-zero exit code when violations injected, asserts exit code 0 on clean data.

---

### AC6: Test coverage

| AC ID | Description | Test Type | Test File | Phase |
|-------|-------------|-----------|-----------|-------|
| l1-l2-checks.AC6.1 | Each of the 11 new checks has at least one passing-data and one failing-data test case | integration | tests/test_l1_l2_integration.py | 8 |

**Test details:**

- **AC6.1:** Phase 8 Task 3 creates a multi-table integration test exercising all 11 check IDs with failing data:
  - `encounter.parquet` — checks 122, 111, 226, 244, 245
  - `enrollment.parquet` — checks 215, 216, 226
  - `demographic.parquet` — checks 128, 111
  - `dispensing.parquet` — check 124
  - `cause_of_death.parquet` — checks 236, 237

  Asserts each of the 11 check IDs (`"111"`, `"122"`, `"124"`, `"128"`, `"215"`, `"216"`, `"226"`, `"236"`, `"237"`, `"244"`, `"245"`) appears in at least one StepResult with `n_failed > 0`.

  Separate clean-data integration test: all 11 checks present with `n_failed == 0`, exit code 0.

  Individual passing/failing tests per check are also covered in earlier phases:
  - Check 111: Phase 3, tests/test_global_checks.py (pass + fail)
  - Check 122: Phase 2, tests/test_l1_checks.py (pass + fail)
  - Check 124: Phase 2, tests/test_l1_checks.py (pass + fail)
  - Check 128: Phase 2, tests/test_l1_checks.py (pass + fail)
  - Check 215: Phase 6, tests/test_global_checks.py (pass + fail + DuckDB)
  - Check 216: Phase 6, tests/test_global_checks.py (pass + fail + adjacent)
  - Check 226: Phase 4, tests/test_global_checks.py (pass + fail + null dates)
  - Check 236: Phase 5, tests/test_global_checks.py (pass + fail)
  - Check 237: Phase 5, tests/test_global_checks.py (pass + fail)
  - Check 244: Phase 7, tests/test_global_checks.py (pass + fail + AV-nulls)
  - Check 245: Phase 7, tests/test_global_checks.py (above threshold + below threshold)

---

## Human Verification Required

| AC ID | Description | Justification | Verification Approach |
|-------|-------------|---------------|----------------------|
| None | All criteria have automated coverage | Every AC maps to at least one automated unit or integration test | N/A |

---

## Test File Summary

| Test File | New Test Classes / Areas | Phases |
|-----------|------------------------|--------|
| tests/test_accumulator.py | `TestAccumulatorPropagatesCheckId` (check_id=None default, check_id preserved when set) | 1 |
| tests/test_l1_checks.py | Check 122 (leading spaces), Check 124 (unexpected zeros), Check 128 (non-numeric), na_pass behaviour, clean data | 2 |
| tests/test_global_checks.py | `check_not_populated` (111), `check_date_ordering` (226), `check_cause_of_death` (236/237), `check_overlapping_spans` (215), `check_enrollment_gaps` (216), `check_enc_combinations` (244/245) | 3, 4, 5, 6, 7 |
| tests/test_reporting.py | CheckID column renders in HTML, backward compat with check_id=None | 1, 8 |
| tests/test_l1_l2_integration.py | Multi-table integration (all 11 checks), clean-data pass, exit code verification, backward compat with existing checks | 8 |

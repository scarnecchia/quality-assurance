# Test Requirements: cross-table-code-checks

Maps each acceptance criterion to specific automated tests and, where applicable, manual verification steps.

---

## Summary Table

| AC ID | Criterion | Test Type | Test File | Phase |
|-------|-----------|-----------|-----------|-------|
| AC1.1 | Cross-table rules load from JSON into frozen dataclasses | Unit | `tests/test_cross_table_checks.py` | 4 |
| AC1.2 | Check 201: PatID referential integrity | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC1.3 | Check 203: Variable length consistency | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC1.4 | Check 205: Enr_Start before Birth_Date | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC1.5 | Check 206: ADate/DDate before Birth_Date | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC1.6 | Check 227: PostalCode_Date before Birth_Date | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC1.7 | Check 209: Length excess warning | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC1.8 | Check 224: Hispanic != ImputedHispanic | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC1.9 | Cross-table HTML report + index entry | Integration | `tests/test_pipeline_phases.py`, `tests/test_reporting.py` | 7 |
| AC1.10 | Missing reference table skipped gracefully | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC1.11 | DuckDB SQL error returns error StepResult | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC1.12 | SAS7BDAT converted to temp parquet for DuckDB | Unit | `tests/test_cross_table_engine.py` | 5 |
| AC2.1 | Code check rules load from JSON into frozen dataclasses | Unit | `tests/test_code_checks.py` | 2 |
| AC2.2 | Filtering by table key returns correct subset | Unit | `tests/test_code_checks.py` | 2 |
| AC2.3 | Check 223 no_decimal: periods flagged | Unit | `tests/test_code_check_validation.py` | 3 |
| AC2.4 | Check 223 regex: CPT-4/NDC pattern violations | Unit | `tests/test_code_check_validation.py` | 3 |
| AC2.5 | Check 223 era_date: ICD-9/10 era mismatch | Unit | `tests/test_code_check_validation.py` | 3 |
| AC2.6 | Check 223 conditional_presence: PDX/EncType rules | Unit | `tests/test_code_check_validation.py` | 3 |
| AC2.7 | Check 228: Code length range violations | Unit | `tests/test_code_check_validation.py` | 3 |
| AC2.8 | Malformed code_checks.json raises ConfigError | Unit | `tests/test_code_checks.py` | 2 |
| AC2.9 | Null codetype rows skipped | Unit | `tests/test_code_check_validation.py` | 3 |
| AC3.1 | `--l1-only` skips L2 | Unit | `tests/test_cli.py`, `tests/test_pipeline_phases.py` | 1, 6 |
| AC3.2 | `--l2-only` skips L1 | Unit | `tests/test_cli.py`, `tests/test_pipeline_phases.py` | 1, 6 |
| AC3.3 | Default runs both L1 and L2 | Unit | `tests/test_cli.py`, `tests/test_pipeline_phases.py` | 1, 6 |
| AC3.4 | `--l1-only --l2-only` raises error | Unit | `tests/test_cli.py` | 1 |
| AC3.5 | TOML run_l1/run_l2 options parsed | Unit | `tests/test_config.py` | 1 |
| AC3.6 | CLI flags override TOML config | Unit | `tests/test_cli.py` | 1 |
| AC3.7 | `--table` filter scopes L2 checks | Unit | `tests/test_pipeline_phases.py` | 6 |
| AC3.8 | Exit code reflects L1 + L2 failures | Unit | `tests/test_pipeline_phases.py` | 6 |

---

## AC1: Cross-table validation phase

### cross-table-code-checks.AC1.1 — Cross-table check rules load from JSON into frozen dataclasses

> Cross-table check rules load from `cross_table_checks.json` and parse into frozen dataclasses

**Test type:** Unit
**Test file:** `tests/test_cross_table_checks.py`

**Automated tests:**

1. `test_load_cross_table_checks_returns_nonempty_tuple` — Call `load_cross_table_checks()`, assert it returns a non-empty tuple of `CrossTableCheckDef` instances.
2. `test_cross_table_check_def_is_frozen` — Construct a `CrossTableCheckDef`, attempt attribute assignment, assert `FrozenInstanceError` is raised.
3. `test_cross_table_check_fields_populated` — Load checks, pick one of each `check_type`, verify all required fields for that type are non-None and correctly typed.
4. `test_get_checks_for_table_returns_relevant` — Call `get_checks_for_table("enrollment")`, assert returned checks all have `source_table == "enrollment"` or `reference_table == "enrollment"`.
5. `test_get_checks_for_table_nonexistent_returns_empty` — Call `get_checks_for_table("nonexistent")`, assert empty tuple.
6. `test_malformed_cross_table_json_raises_config_error` — Use `monkeypatch` to point parser at a JSON file with missing required keys, assert `ConfigError` is raised.

---

### cross-table-code-checks.AC1.2 — Check 201: PatID referential integrity

> PatID in diagnosis/procedure/etc. but not in enrollment is flagged as warn

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_referential_integrity_flags_missing_patid` — Create two synthetic parquet files: source table with PatIDs ["A", "B", "C"], enrollment table with PatIDs ["A", "B"]. Run the `referential_integrity` handler. Assert `StepResult.n_failed == 1` and severity is "Warn".
2. `test_referential_integrity_all_present` — All source PatIDs exist in enrollment. Assert `StepResult.n_failed == 0`.
3. `test_referential_integrity_failing_rows_sample` — Verify `StepResult.failing_rows` contains the orphaned rows (up to `max_failing_rows`).

---

### cross-table-code-checks.AC1.3 — Check 203: Variable length consistency

> Different max string lengths for same column across table groups is flagged as fail

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_length_consistency_flags_different_max_lengths` — Create parquet files for two tables where the same column (e.g., PatID) has different max string lengths (one has max 8, other has max 10). Run the `length_consistency` handler. Assert the check flags a failure.
2. `test_length_consistency_same_lengths_pass` — Both tables have the same max length for the column. Assert check passes.

---

### cross-table-code-checks.AC1.4 — Check 205: Enr_Start before Birth_Date

> Enr_Start before Birth_Date (joined on PatID) is flagged as warn

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_cross_date_enr_start_before_birth_date_flagged` — Enrollment table has PatID "A" with Enr_Start = 1990-01-01. Demographic table has PatID "A" with Birth_Date = 2000-01-01. Run `cross_date_compare` handler. Assert `n_failed >= 1` and severity is "Warn".
2. `test_cross_date_enr_start_after_birth_date_passes` — Enr_Start after Birth_Date. Assert `n_failed == 0`.

---

### cross-table-code-checks.AC1.5 — Check 206: ADate/DDate before Birth_Date

> ADate/DDate before Birth_Date (joined on PatID) is flagged as warn

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_cross_date_adate_before_birth_date_flagged` — Encounter table has ADate before demographic Birth_Date for same PatID. Assert `n_failed >= 1`.
2. `test_cross_date_ddate_before_birth_date_flagged` — Encounter table has DDate before demographic Birth_Date for same PatID. Assert `n_failed >= 1`.
3. `test_cross_date_adate_after_birth_date_passes` — ADate after Birth_Date. Assert `n_failed == 0`.

---

### cross-table-code-checks.AC1.6 — Check 227: PostalCode_Date before Birth_Date

> PostalCode_Date before Birth_Date (joined on PatID) is flagged as warn

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_cross_date_postalcode_date_before_birth_date_flagged` — Address history table has PostalCode_Date before demographic Birth_Date. Assert `n_failed >= 1` and severity is "Warn".
2. `test_cross_date_postalcode_date_after_birth_date_passes` — PostalCode_Date after Birth_Date. Assert `n_failed == 0`.

---

### cross-table-code-checks.AC1.7 — Check 209: Length excess warning

> Actual max column length much smaller than declared schema length across tables is flagged as warn

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_length_excess_flags_undersized_column` — Create parquet file where actual max length of a column is 3 but schema declares length 20 (below 50% threshold). Run `length_excess` handler. Assert check flags a warning.
2. `test_length_excess_adequate_usage_passes` — Actual max length is 18 for a declared 20. Assert check passes.

---

### cross-table-code-checks.AC1.8 — Check 224: Hispanic != ImputedHispanic

> Hispanic != ImputedHispanic (both non-null) in demographic is flagged as note

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_column_mismatch_flags_disagreement` — Demographic table has rows where Hispanic = "Y" and ImputedHispanic = "N" (both non-null). Run `column_mismatch` handler. Assert `n_failed >= 1` and severity is "Note".
2. `test_column_mismatch_null_values_not_flagged` — Rows where one or both columns are null. Assert those rows are not counted as failures.
3. `test_column_mismatch_agreement_passes` — Hispanic == ImputedHispanic for all non-null rows. Assert `n_failed == 0`.

---

### cross-table-code-checks.AC1.9 — Cross-table HTML report + index entry

> Cross-table results produce HTML report page and appear in index summary

**Test type:** Integration
**Test file:** `tests/test_pipeline_phases.py`, `tests/test_reporting.py`

**Automated tests:**

1. `test_cross_table_report_file_created` — Run pipeline with `run_l2=True` against synthetic data. Assert `cross_table.html` exists in output directory.
2. `test_cross_table_index_entry_present` — Assert `index.html` contains a link to `cross_table.html` and shows "Cross-Table Checks" label.
3. `test_save_table_report_handles_empty_profiling` — Call `save_table_report()` with a `ProfilingResult` that has empty `columns`. Assert no error and HTML is generated.

**Human verification:**

Open a generated `cross_table.html` in a browser and verify that:
- The page title or heading shows "Cross-Table Checks"
- Validation step results are rendered in a table with check IDs, descriptions, pass/fail counts
- No broken layout from the empty profiling section
- The index page links correctly to the cross-table report

**Justification:** Visual layout correctness, readability of the report, and sensible presentation of cross-table results (which have no profiling data) cannot be fully automated.

---

### cross-table-code-checks.AC1.10 — Missing reference table skipped gracefully

> Missing reference table in config -> check skipped with log warning, no crash

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_missing_reference_table_skips_check` — Config has source table but not the reference table. Run `run_cross_table_checks()`. Assert no exception raised, the check for the missing table is absent from results.
2. `test_missing_reference_table_logs_warning` — Same setup with `caplog`. Assert a warning-level log message mentions the missing reference table and the skipped check ID.

---

### cross-table-code-checks.AC1.11 — DuckDB SQL error returns error StepResult

> DuckDB SQL error on a single check -> that check returns error StepResult, pipeline continues

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_duckdb_error_returns_error_step_result` — Register a DuckDB view that is missing a column referenced by the check SQL. Run `run_cross_table_checks()`. Assert one `StepResult` with `n_failed == 0` and description containing the error message. Assert other checks still produce results (pipeline did not abort).
2. `test_duckdb_error_does_not_crash_pipeline` — Same scenario. Assert no exception propagated; the function returns normally.

---

### cross-table-code-checks.AC1.12 — SAS7BDAT converted to temp parquet

> SAS7BDAT table files are converted to temp parquet before DuckDB registration

**Test type:** Unit
**Test file:** `tests/test_cross_table_engine.py`

**Automated tests:**

1. `test_sas_file_converted_to_parquet` — Create a small SAS7BDAT file (via pyreadstat if feasible, or use a fixture), pass it in config. Assert `_convert_sas_to_parquet()` produces a valid parquet file that DuckDB can query.
2. `test_sas_conversion_logs_warning` — Assert a warning-level log is emitted about the SAS-to-parquet conversion.

**Human verification:**

If creating a SAS fixture in CI is impractical (pyreadstat write support may be limited), verify manually by running the pipeline against a real SAS7BDAT file and confirming:
- No crash during view registration
- Cross-table checks execute and produce results
- Temp parquet file is cleaned up after the run

**Justification:** SAS file creation in test fixtures may not be trivially portable. The automated test should attempt it but a manual smoke test against an actual SAS file confirms real-world behaviour.

---

## AC2: Code/CodeType validation

### cross-table-code-checks.AC2.1 — Code check rules load from JSON into frozen dataclasses

> Code check rules load from `code_checks.json` and parse into frozen dataclasses

**Test type:** Unit
**Test file:** `tests/test_code_checks.py`

**Automated tests:**

1. `test_load_code_checks_returns_nonempty_tuples` — Call `load_code_checks()`. Assert both `format_checks` and `length_checks` tuples are non-empty.
2. `test_format_check_def_is_frozen` — Construct a `FormatCheckDef`, attempt attribute assignment, assert `FrozenInstanceError`.
3. `test_length_check_def_is_frozen` — Same for `LengthCheckDef`.
4. `test_format_check_fields_correct_types` — Load format checks, verify field types: `check_id` is str, `pattern` is str or None, `condition_values` is tuple or None, etc.
5. `test_length_check_fields_correct_types` — Load length checks, verify `min_length` and `max_length` are int, `severity` is str.

---

### cross-table-code-checks.AC2.2 — Filtering by table key returns correct subset

> Filtering by table key returns only rules for that table

**Test type:** Unit
**Test file:** `tests/test_code_checks.py`

**Automated tests:**

1. `test_get_format_checks_for_table_diagnosis` — Call `get_format_checks_for_table("diagnosis")`. Assert all returned checks have `table_key == "diagnosis"`. Assert the result is non-empty (DIA has no_decimal, era_date, conditional_presence rules).
2. `test_get_length_checks_for_table_procedure` — Call `get_length_checks_for_table("procedure")`. Assert all returned checks have `table_key == "procedure"`.
3. `test_get_format_checks_for_nonexistent_table` — Call `get_format_checks_for_table("nonexistent")`. Assert empty tuple returned.
4. `test_get_length_checks_for_nonexistent_table` — Same for length checks.

---

### cross-table-code-checks.AC2.3 — Check 223 no_decimal: periods flagged

> ICD-9/10 codes containing periods are flagged (DIA, PRO, COD)

**Test type:** Unit
**Test file:** `tests/test_code_check_validation.py`

**Automated tests:**

1. `test_no_decimal_flags_icd9_with_period` — Synthetic DataFrame with DX_CodeType="09" and DX="123.4". Run `build_validation()` + `interrogate()`. Assert the no_decimal step fails for that row.
2. `test_no_decimal_flags_icd10_with_period` — DX_CodeType="10" and DX="A12.34". Assert flagged.
3. `test_no_decimal_passes_clean_codes` — DX_CodeType="09" and DX="1234". Assert passes.
4. `test_no_decimal_applies_to_pro_table` — Same test with procedure table schema, PX column, PX_CodeType.

---

### cross-table-code-checks.AC2.4 — Check 223 regex: CPT-4/NDC pattern violations

> CPT-4 codes not matching pattern are flagged; NDC codes with non-numeric chars are flagged

**Test type:** Unit
**Test file:** `tests/test_code_check_validation.py`

**Automated tests:**

1. `test_regex_flags_invalid_cpt4` — PX_CodeType="C4", PX="ABC" (does not match `^\d{4}[AaMmUu]$|^\d{5}$`). Assert flagged.
2. `test_regex_passes_valid_cpt4_5digit` — PX_CodeType="C4", PX="12345". Assert passes.
3. `test_regex_passes_valid_cpt4_with_modifier` — PX_CodeType="C4", PX="1234A". Assert passes.
4. `test_regex_flags_ndc_with_letters` — NDC codetype, NDC="1234ABC". Assert flagged.
5. `test_regex_passes_ndc_numeric` — NDC codetype, NDC="12345678901". Assert passes.

---

### cross-table-code-checks.AC2.5 — Check 223 era_date: ICD-9/10 era mismatch

> ICD-9 codes on/after 2015-10-01 and ICD-10 codes before 2015-10-01 are flagged (DIA, PRO)

**Test type:** Unit
**Test file:** `tests/test_code_check_validation.py`

**Automated tests:**

1. `test_era_date_flags_icd9_after_transition` — DX_CodeType="09", ADate=2015-10-01. Assert flagged.
2. `test_era_date_flags_icd9_well_after_transition` — DX_CodeType="09", ADate=2020-06-15. Assert flagged.
3. `test_era_date_passes_icd9_before_transition` — DX_CodeType="09", ADate=2015-09-30. Assert passes.
4. `test_era_date_flags_icd10_before_transition` — DX_CodeType="10", ADate=2015-09-30. Assert flagged.
5. `test_era_date_passes_icd10_after_transition` — DX_CodeType="10", ADate=2015-10-01. Assert passes.
6. `test_era_date_applies_to_procedure_table` — Same patterns for procedure table with PX_CodeType.

---

### cross-table-code-checks.AC2.6 — Check 223 conditional_presence: PDX/EncType rules

> PDX null when EncType=IP/IS is flagged; PDX not-null when EncType=AV/ED/OA is flagged

**Test type:** Unit
**Test file:** `tests/test_code_check_validation.py`

**Automated tests:**

1. `test_conditional_presence_pdx_null_when_ip_flagged` — EncType="IP", PDX=None. Assert flagged (PDX should not be null for IP).
2. `test_conditional_presence_pdx_null_when_is_flagged` — EncType="IS", PDX=None. Assert flagged.
3. `test_conditional_presence_pdx_present_when_ip_passes` — EncType="IP", PDX="P". Assert passes.
4. `test_conditional_presence_pdx_present_when_av_flagged` — EncType="AV", PDX="P". Assert flagged (PDX should be null for AV).
5. `test_conditional_presence_pdx_present_when_ed_flagged` — EncType="ED", PDX="S". Assert flagged.
6. `test_conditional_presence_pdx_null_when_av_passes` — EncType="AV", PDX=None. Assert passes.

---

### cross-table-code-checks.AC2.7 — Check 228: Code length range violations

> Code lengths outside min/max range per CodeType are flagged

**Test type:** Unit
**Test file:** `tests/test_code_check_validation.py`

**Automated tests:**

1. `test_length_check_icd9_dx_too_short` — DX_CodeType="09", DX="AB" (length 2, min 3). Assert flagged.
2. `test_length_check_icd9_dx_too_long` — DX_CodeType="09", DX="ABCDEF" (length 6, max 5). Assert flagged.
3. `test_length_check_icd9_dx_within_range` — DX_CodeType="09", DX="ABCD" (length 4, range 3-5). Assert passes.
4. `test_length_check_icd10_dx_max_7` — DX_CodeType="10", DX="ABCDEFGH" (length 8, max 7). Assert flagged.
5. `test_length_check_cpt4_exact_5` — PX_CodeType="C4", PX="1234" (length 4, must be exactly 5). Assert flagged.
6. `test_length_check_ndc_range_9_to_11` — NDC codetype, NDC="12345678" (length 8, min 9). Assert flagged. NDC="12345678901" (length 11). Assert passes.

---

### cross-table-code-checks.AC2.8 — Malformed code_checks.json raises ConfigError

> Malformed `code_checks.json` raises ConfigError at parse time

**Test type:** Unit
**Test file:** `tests/test_code_checks.py`

**Automated tests:**

1. `test_malformed_json_missing_format_checks_key` — JSON file with `{"length_checks": []}` (missing `format_checks`). Assert `ConfigError`.
2. `test_malformed_json_missing_required_field` — JSON file with a format check entry missing `check_id`. Assert `ConfigError` with message identifying the missing key.
3. `test_malformed_json_invalid_json_syntax` — File with invalid JSON. Assert `ConfigError` or `json.JSONDecodeError`.

---

### cross-table-code-checks.AC2.9 — Null codetype rows skipped

> Rows where codetype column is null are skipped (not flagged)

**Test type:** Unit
**Test file:** `tests/test_code_check_validation.py`

**Automated tests:**

1. `test_null_codetype_not_flagged_format_check` — DataFrame with rows where DX_CodeType is null. Run format check 223. Assert those rows do not contribute to `n_failed`.
2. `test_null_codetype_not_flagged_length_check` — DataFrame with rows where codetype is null. Run length check 228. Assert those rows do not contribute to `n_failed`.
3. `test_null_codetype_mixed_with_valid` — DataFrame with both null and non-null codetype rows, where the non-null rows have violations. Assert only the non-null violation rows are counted in `n_failed`.

---

## AC3: CLI + config phase isolation

### cross-table-code-checks.AC3.1 — `--l1-only` skips L2

> `--l1-only` runs only per-table validation, skips cross-table

**Test type:** Unit
**Test file:** `tests/test_cli.py` (CLI flag parsing), `tests/test_pipeline_phases.py` (pipeline behaviour)

**Automated tests:**

1. `test_l1_only_flag_sets_config` — Invoke CLI with `--l1-only`. Assert config has `run_l1=True, run_l2=False`.
2. `test_l1_only_pipeline_skips_l2` — Run `run_pipeline()` with `run_l1=True, run_l2=False`. Mock `run_cross_table_checks`. Assert mock was not called. Assert outcomes contain only per-table results.

---

### cross-table-code-checks.AC3.2 — `--l2-only` skips L1

> `--l2-only` runs only cross-table validation, skips per-table

**Test type:** Unit
**Test file:** `tests/test_cli.py`, `tests/test_pipeline_phases.py`

**Automated tests:**

1. `test_l2_only_flag_sets_config` — Invoke CLI with `--l2-only`. Assert config has `run_l1=False, run_l2=True`.
2. `test_l2_only_pipeline_skips_l1` — Run `run_pipeline()` with `run_l1=False, run_l2=True`. Mock the per-table processing function. Assert mock was not called. Assert outcomes contain only "cross_table" result.

---

### cross-table-code-checks.AC3.3 — Default runs both L1 and L2

> Default (no flags) runs both L1 and L2

**Test type:** Unit
**Test file:** `tests/test_cli.py`, `tests/test_pipeline_phases.py`

**Automated tests:**

1. `test_no_flags_defaults_both_true` — Invoke CLI with no phase flags. Assert config has `run_l1=True, run_l2=True`.
2. `test_default_pipeline_runs_both` — Run `run_pipeline()` with both True. Mock both L1 processing and `run_cross_table_checks`. Assert both mocks were called. Assert outcomes contain per-table and "cross_table" results.

---

### cross-table-code-checks.AC3.4 — `--l1-only --l2-only` raises error

> `--l1-only --l2-only` together raises error

**Test type:** Unit
**Test file:** `tests/test_cli.py`

**Automated tests:**

1. `test_both_flags_raises_error` — Invoke CLI with `--l1-only --l2-only`. Assert exit code is 2. Assert stderr contains error message about mutual exclusion.

---

### cross-table-code-checks.AC3.5 — TOML run_l1/run_l2 options parsed

> TOML `run_l1`/`run_l2` options control phase execution

**Test type:** Unit
**Test file:** `tests/test_config.py`

**Automated tests:**

1. `test_toml_run_l1_false` — TOML with `run_l1 = false`. Assert `config.run_l1 == False`.
2. `test_toml_run_l2_false` — TOML with `run_l2 = false`. Assert `config.run_l2 == False`.
3. `test_toml_defaults_both_true` — TOML with no `run_l1`/`run_l2` keys. Assert both default to `True`.
4. `test_toml_both_explicitly_set` — TOML with `run_l1 = true, run_l2 = false`. Assert values match.

---

### cross-table-code-checks.AC3.6 — CLI flags override TOML config

> CLI flags override TOML config values

**Test type:** Unit
**Test file:** `tests/test_cli.py`

**Automated tests:**

1. `test_cli_l1_only_overrides_toml_both_true` — TOML sets `run_l1 = true, run_l2 = true`. CLI passes `--l1-only`. Assert the config passed to `run_pipeline` has `run_l2=False`. Use mock/spy on `run_pipeline` to inspect the config.
2. `test_cli_l2_only_overrides_toml_both_true` — Same with `--l2-only`. Assert `run_l1=False`.

---

### cross-table-code-checks.AC3.7 — `--table` filter scopes L2 checks

> `--table` filter with L2 only runs cross-table checks involving that table

**Test type:** Unit
**Test file:** `tests/test_pipeline_phases.py`

**Automated tests:**

1. `test_table_filter_with_l2_scopes_checks` — Run pipeline with `table_filter="diagnosis"` and `run_l2=True`. Mock `run_cross_table_checks`. Assert the `checks` argument passed to the mock only includes checks where "diagnosis" is source or reference table.
2. `test_table_filter_no_cross_table_checks_skips_l2` — Run with `table_filter="some_table_not_in_cross_checks"`. Assert `run_cross_table_checks` is not called (or called with empty checks tuple, resulting in no "cross_table" outcome).

---

### cross-table-code-checks.AC3.8 — Exit code reflects L1 + L2 failures

> Exit code reflects failures from both L1 and L2 results

**Test type:** Unit
**Test file:** `tests/test_pipeline_phases.py`

**Automated tests:**

1. `test_exit_code_l2_warn_returns_1` — Synthetic "cross_table" `TableOutcome` with Warn-severity failures. Call `compute_exit_code()`. Assert returns 1.
2. `test_exit_code_l2_fail_above_threshold_returns_2` — Synthetic "cross_table" `TableOutcome` with Fail-severity results exceeding error threshold. Assert returns 2.
3. `test_exit_code_l2_note_only_returns_0` — Synthetic "cross_table" `TableOutcome` with only Note-severity results. Assert returns 0.
4. `test_exit_code_combined_l1_pass_l2_warn` — Per-table outcomes all pass, but "cross_table" outcome has warnings. Assert returns 1 (worst of both levels).
5. `test_exit_code_combined_l1_warn_l2_fail` — Per-table outcomes have warnings, "cross_table" has failures. Assert returns 2.

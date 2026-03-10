# Cross-Table Checks & Code/CodeType Validation — Implementation Plan

**Goal:** Extend the SCDM QA pipeline with L1 code/codetype checks and L2 cross-table validation via DuckDB.

**Architecture:** Two-level validation pipeline. L1 adds code format/length checks (223, 228) to the existing per-chunk pointblank chain. L2 adds a new DuckDB-based cross-table phase that runs after all L1 processing. Both levels independently controllable via CLI flags and TOML config.

**Tech Stack:** Python 3.12+, Polars, pointblank, DuckDB, Typer, pytest

**Scope:** 7 phases from original design (phases 1–7)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### cross-table-code-checks.AC2: Code/CodeType validation
- **cross-table-code-checks.AC2.3 Success:** Check 223 no_decimal: ICD-9/10 codes containing periods are flagged (DIA, PRO, COD)
- **cross-table-code-checks.AC2.4 Success:** Check 223 regex: CPT-4 codes not matching `^\d{4}[AaMmUu]$|^\d{5}$` are flagged; NDC codes with non-numeric chars are flagged
- **cross-table-code-checks.AC2.5 Success:** Check 223 era_date: ICD-9 codes on/after 2015-10-01 and ICD-10 codes before 2015-10-01 are flagged (DIA, PRO)
- **cross-table-code-checks.AC2.6 Success:** Check 223 conditional_presence: PDX null when EncType=IP/IS is flagged; PDX not-null when EncType=AV/ED/OA is flagged; DDate/Discharge fields follow EncType rules
- **cross-table-code-checks.AC2.7 Success:** Check 228: Code lengths outside min/max range per CodeType are flagged (ICD-9 DX 3-5, ICD-10 DX 3-7, NDC 9-11, CPT 5, etc.)
- **cross-table-code-checks.AC2.9 Edge:** Rows where codetype column is null are skipped (not flagged)

---

## Phase 3: Code Check Validation Integration

This phase wires the format (223) and length (228) code check rules from Phase 2 into the existing per-chunk pointblank validation chain in `build_validation()`.

**Existing patterns to follow:**
- `build_validation()` in `src/scdm_qa/schemas/validation.py` chains pointblank assertions
- L1 checks (122, 124, 128) at lines 72-98 show the pattern: iterate check defs, skip missing columns, dispatch by check_type
- `pre=` lambda pattern at line 67 shows how to filter rows before assertion
- `na_pass=True` is used throughout for nullable columns

**pointblank API (confirmed via research):**
- `col_vals_regex(columns, pattern, na_pass, pre)` — regex matching
- `col_vals_not_null(columns, pre)` — non-null assertion
- `col_vals_null(columns, pre)` — null assertion (for conditional_presence "must be null" rules)
- `pre=` takes a callable: `lambda df: df.filter(...)`, returns filtered DataFrame

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Add format check (223) assertions to build_validation()

**Verifies:** cross-table-code-checks.AC2.3, cross-table-code-checks.AC2.4, cross-table-code-checks.AC2.5, cross-table-code-checks.AC2.9

**Files:**
- Modify: `src/scdm_qa/schemas/validation.py:1-100`
- Test: `tests/test_code_check_validation.py` (new file)

**Implementation:**

At the top of `validation.py`, add import:
```python
from scdm_qa.schemas.code_checks import get_format_checks_for_table, get_length_checks_for_table
```

After the existing L1 per-chunk checks loop (after line 98), add a new section for format checks:

```python
# Code format checks (223)
for fmt_check in get_format_checks_for_table(schema.table_key):
    if fmt_check.column not in present_columns:
        continue
    if fmt_check.codetype_column not in present_columns:
        continue

    # pre= filter: only rows where codetype matches AND codetype is not null
    codetype_pre = lambda df, ct_col=fmt_check.codetype_column, ct_val=fmt_check.codetype_value: (
        df.filter(pl.col(ct_col).is_not_null() & (pl.col(ct_col) == ct_val))
    )

    if fmt_check.check_subtype == "no_decimal":
        # Code must not contain a period
        validation = validation.col_vals_regex(
            columns=fmt_check.column,
            pattern=r"^[^.]*$",
            na_pass=True,
            pre=codetype_pre,
        )

    elif fmt_check.check_subtype == "regex":
        # Code must match the specified pattern
        validation = validation.col_vals_regex(
            columns=fmt_check.column,
            pattern=fmt_check.pattern,
            na_pass=True,
            pre=codetype_pre,
        )

    elif fmt_check.check_subtype == "era_date":
        # Era date check: filter to "bad" rows (wrong codetype for the era),
        # then assert code column is null in that set. Any non-null rows fail.
        #
        # For ICD-9 (codetype "09"): rows with ADate >= 2015-10-01 are violations
        # For ICD-10 (codetype "10"): rows with ADate < 2015-10-01 are violations
        #
        # The date_column and era_boundary are on the FormatCheckDef.
        # The codetype_value determines the direction:
        #   "09" → violations are date >= boundary
        #   "10" → violations are date < boundary
        if fmt_check.date_column not in present_columns:
            continue

        if fmt_check.codetype_value == "09":
            # ICD-9 after transition date is a violation
            era_pre = lambda df, ct_col=fmt_check.codetype_column, ct_val=fmt_check.codetype_value, d_col=fmt_check.date_column, boundary=fmt_check.era_boundary: (
                df.filter(
                    pl.col(ct_col).is_not_null()
                    & (pl.col(ct_col) == ct_val)
                    & (pl.col(d_col) >= pl.lit(boundary).str.to_date("%Y-%m-%d"))
                )
            )
        else:
            # ICD-10 before transition date is a violation
            era_pre = lambda df, ct_col=fmt_check.codetype_column, ct_val=fmt_check.codetype_value, d_col=fmt_check.date_column, boundary=fmt_check.era_boundary: (
                df.filter(
                    pl.col(ct_col).is_not_null()
                    & (pl.col(ct_col) == ct_val)
                    & (pl.col(d_col) < pl.lit(boundary).str.to_date("%Y-%m-%d"))
                )
            )

        # Assert that code column is null in the violation set.
        # Any non-null codes in these rows are violations.
        validation = validation.col_vals_null(
            columns=fmt_check.column,
            pre=era_pre,
        )

    elif fmt_check.check_subtype == "conditional_presence":
        # Conditional presence: filter by condition_column values,
        # then assert target column is null or not-null based on expect_null.
        # Fields condition_column, condition_values, expect_null are on FormatCheckDef.
        if fmt_check.condition_column not in present_columns:
            continue

        cond_pre = lambda df, cc=fmt_check.condition_column, cv=list(fmt_check.condition_values): (
            df.filter(pl.col(cc).is_in(cv))
        )
        if fmt_check.expect_null:
            validation = validation.col_vals_null(
                columns=fmt_check.column,
                pre=cond_pre,
            )
        else:
            validation = validation.col_vals_not_null(
                columns=fmt_check.column,
                pre=cond_pre,
            )
```

**Era date strategy (AC2.5):**

The approach is: use `pre=` to filter to violation rows (wrong codetype for the date era), then assert `col_vals_null` on the code column. Since code columns are non-null for actual data rows, any rows that pass the `pre=` filter will fail the null assertion — which flags them as violations. This works because if the filter produces an empty DataFrame (no violations), the assertion trivially passes.

The `FormatCheckDef` carries `date_column` (e.g., "ADate"), `era_boundary` (e.g., "2015-10-01"), and `codetype_value` (determines direction: "09" = after boundary is bad, "10" = before boundary is bad). These fields were added in Phase 2 Task 1.

**Conditional presence strategy (AC2.6):**

The `FormatCheckDef` carries `condition_column`, `condition_values`, and `expect_null` (from Phase 2 Task 1). The pre= filter selects rows matching the condition, then either `col_vals_null` or `col_vals_not_null` asserts the expected state.

Note: DDate/Discharge conditional presence rules based on EncType are already handled by existing checks 244/245 in `checks.py` and do not need new entries here.

**AC2.9 (null codetype rows skipped):** The `codetype_pre` lambda already handles this by filtering `pl.col(ct_col).is_not_null()`.

**Testing:**

Tests must verify:
- AC2.3: DataFrame with ICD-9 codes containing periods in DX column → flagged after validation
- AC2.4: DataFrame with CPT-4 codes not matching pattern → flagged; NDC with letters → flagged
- AC2.5: DataFrame with ICD-9 codes after 2015-10-01 → flagged; ICD-10 before → flagged
- AC2.9: DataFrame with null codetype values → those rows are not flagged

Create synthetic Polars DataFrames with known good and bad values, run `build_validation()`, call `interrogate()`, and check step results.

**Verification:**

Run: `uv run pytest tests/test_code_check_validation.py -v`
Expected: All tests pass.

**Commit:** `feat: wire format check 223 assertions into validation chain`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Add length check (228) assertions to build_validation()

**Verifies:** cross-table-code-checks.AC2.7, cross-table-code-checks.AC2.9

**Files:**
- Modify: `src/scdm_qa/schemas/validation.py`
- Test: `tests/test_code_check_validation.py` (extend)

**Implementation:**

After the format checks loop, add a length checks loop:

```python
# Code length checks (228)
for len_check in get_length_checks_for_table(schema.table_key):
    if len_check.column not in present_columns:
        continue
    if len_check.codetype_column not in present_columns:
        continue

    codetype_pre = lambda df, ct_col=len_check.codetype_column, ct_val=len_check.codetype_value: (
        df.filter(pl.col(ct_col).is_not_null() & (pl.col(ct_col) == ct_val))
    )

    # Code length must be between min_length and max_length (inclusive)
    # Use regex: ^.{min,max}$
    length_pattern = f"^.{{{len_check.min_length},{len_check.max_length}}}$"
    validation = validation.col_vals_regex(
        columns=len_check.column,
        pattern=length_pattern,
        na_pass=True,
        pre=codetype_pre,
    )
```

**Testing:**

Tests must verify:
- AC2.7: ICD-9 DX code with 2 chars (below min 3) → flagged. ICD-9 DX code with 6 chars (above max 5) → flagged. Code with 4 chars (within range) → passes.
- AC2.9: Rows with null codetype → not flagged.

**Verification:**

Run: `uv run pytest tests/test_code_check_validation.py -v`
Expected: All tests pass.

**Commit:** `feat: wire length check 228 assertions into validation chain`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Add conditional_presence (223) assertions to build_validation()

**Verifies:** cross-table-code-checks.AC2.6

**Files:**
- Modify: `src/scdm_qa/schemas/validation.py`
- Test: `tests/test_code_check_validation.py` (extend)

**Implementation:**

The `conditional_presence` subtype in the format checks loop needs dedicated handling. For each conditional_presence rule:

1. Filter rows by the condition column's values using `pre=`
2. Assert that the target column is null or not-null as specified

This requires the `FormatCheckDef` to carry `condition_column`, `condition_values`, and `expect_null` fields (added in Phase 2 Task 1 model extension or Phase 3 Task 1).

```python
elif fmt_check.check_subtype == "conditional_presence":
    cond_pre = lambda df, cc=fmt_check.condition_column, cv=fmt_check.condition_values: (
        df.filter(pl.col(cc).is_in(list(cv)))
    )
    if fmt_check.expect_null:
        validation = validation.col_vals_null(
            columns=fmt_check.column,
            pre=cond_pre,
        )
    else:
        validation = validation.col_vals_not_null(
            columns=fmt_check.column,
            pre=cond_pre,
        )
```

**Testing:**

Tests must verify:
- AC2.6: PDX is null when EncType=IP → flagged (should not be null). PDX is not null when EncType=AV → flagged (should be null). Valid combinations pass.

**Verification:**

Run: `uv run pytest tests/test_code_check_validation.py -v`
Expected: All tests pass.

**Commit:** `feat: wire conditional_presence check 223 into validation chain`

<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

<!-- START_TASK_4 -->
### Task 4: Full test suite verification

**Verification:**

Run: `uv run pytest`
Expected: All tests pass. No regressions from new validation assertions.

<!-- END_TASK_4 -->

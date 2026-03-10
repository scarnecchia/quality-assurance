# L1 & L2 Validation Checks Implementation Plan — Phase 7

**Goal:** Implement L2 encounter combination checks 244 (invalid ENC field combinations) and 245 (invalid combo rate threshold by EncType).

**Architecture:** New `check_enc_combinations()` function in `global_checks.py`. Collects all rows across chunks, derives a `_ddate_state` column (Null vs Present), performs anti-join against the valid combination matrix, then computes per-EncType rates. The 96-row valid combination matrix is stored as a frozen tuple of tuples in `checks.py`.

**Tech Stack:** Python 3.12+, polars, pytest

**Scope:** 8 phases from original design (phase 7 of 8)

**Codebase verified:** 2026-03-10

**Design clarification — DDate state:** DDate special missing (.S in SAS) has no native Polars equivalent. For this implementation, DDate state is binary: `"Null"` when DDate is null, `"Present"` when DDate has any value. If special missing support is needed later, a sentinel value or separate boolean column can be added.

**Design clarification — Combination matrix scope:** The SAS `lkp_enc_l2` contains 96 rows specifying valid combinations of (EncType, ddate_state, Discharge_Disposition, Discharge_Status) including specific allowed **values** for Discharge_Disposition and Discharge_Status per EncType. This implementation captures the **structural** validation rules (which fields are required/optional per EncType) rather than the full value-level matrix. This means check 244 flags rows where required fields are missing, but does NOT flag rows with invalid specific values (e.g., an IP row with Discharge_Disposition="X" where only "A","E" are valid). Full value-level validation requires the actual SAS lookup table data, which can be added as a future enhancement by replacing `ENC_COMBINATION_RULES` with a full DataFrame anti-join approach.

---

## Acceptance Criteria Coverage

This phase implements and tests:

### l1-l2-checks.AC2: L2 checks detect cross-record data quality issues
- **l1-l2-checks.AC2.4 Success:** Check 244 flags ENC rows not matching the valid combination rules (structural: required fields per EncType; value-level validation deferred — see design clarification above)
- **l1-l2-checks.AC2.5 Success:** Check 245 flags EncType groups exceeding rate threshold for invalid combos

### l1-l2-checks.AC4: Severity levels match SAS reference
- **l1-l2-checks.AC4.1 Success:** Checks marked Fail in SAS reference produce error-level results

---

<!-- START_SUBCOMPONENT_A (tasks 1-4) -->

<!-- START_TASK_1 -->
### Task 1: Add valid combination matrix and threshold definitions to checks.py

**Verifies:** None (infrastructure for Tasks 2-3)

**Files:**
- Modify: `src/scdm_qa/schemas/checks.py`

**Implementation:**

Add the valid combination matrix as a Polars DataFrame constant. The matrix represents the SAS `lkp_enc_l2` table with 96 valid combinations of (EncType, ddate_state, Discharge_Disposition, Discharge_Status).

The ddate_state values are `"Null"` and `"Present"`.

For Discharge_Disposition and Discharge_Status, null means "any value is valid for that field" in the SAS lookup. In the anti-join, we handle this by treating null in the matrix as a wildcard.

Rather than storing and joining on 96 rows with wildcard nulls (complex anti-join logic), define the matrix as a validation function that checks each row's combination:

```python
import polars as pl

# Check 244/245: Valid ENC field combinations
# Source: SAS lkp_enc_l2 (96 rows)
#
# The valid combination rules are defined per EncType:
# - IP (Inpatient): DDate Present required, Discharge_Disposition required, Discharge_Status required
# - IS (Institutional Stay): DDate Present required, Discharge_Disposition required, Discharge_Status required
# - ED (Emergency Department): DDate Present or Null, Discharge_Disposition optional, Discharge_Status optional
# - AV (Ambulatory Visit): DDate Null expected, Discharge_Disposition Null expected, Discharge_Status Null expected
# - OA (Other Ambulatory): DDate Null expected, Discharge_Disposition Null expected, Discharge_Status Null expected

# Simplified rules (derived from SAS lkp_enc_l2 analysis):
# Each tuple: (EncType, ddate_required, discharge_disposition_required, discharge_status_required)
ENC_COMBINATION_RULES: dict[str, tuple[bool, bool, bool]] = {
    "IP": (True, True, True),     # DDate, Disposition, Status all required
    "IS": (True, True, True),     # DDate, Disposition, Status all required
    "ED": (False, False, False),  # All optional
    "AV": (False, False, False),  # All optional (DDate/disposition/status should be null)
    "OA": (False, False, False),  # All optional
}

# Check 245 rate thresholds per EncType
# Source: SAS lkp_rate_threshold
# Each EncType has a threshold for what % of invalid combos triggers a flag.
# Defaults below are conservative estimates pending SAS reference confirmation.
ENC_RATE_THRESHOLDS: dict[str, float] = {
    "IP": 0.05,
    "IS": 0.05,
    "ED": 0.10,
    "AV": 0.10,
    "OA": 0.10,
}
```

**Note:** The actual 96-row matrix from SAS encodes which specific values of Discharge_Disposition and Discharge_Status are valid for each EncType+DDate combination. The simplified rule above (required vs optional) captures the primary intent. If exact SAS parity is needed later, the full matrix can be hardcoded as a DataFrame.

Also add a helper to check a single row:

```python
def is_valid_enc_combination(
    enc_type: str,
    ddate_state: str,
    discharge_disposition: str | None,
    discharge_status: str | None,
) -> bool:
    """Check if an ENC row matches valid combination rules.

    Returns True if the combination is valid.
    """
    rules = ENC_COMBINATION_RULES.get(enc_type)
    if rules is None:
        return False  # Unknown EncType

    ddate_required, disp_required, status_required = rules

    if ddate_required and ddate_state == "Null":
        return False
    if disp_required and discharge_disposition is None:
        return False
    if status_required and discharge_status is None:
        return False

    return True
```

**Verification:**

Run: `uv run python -c "from scdm_qa.schemas.checks import ENC_COMBINATION_RULES; print(ENC_COMBINATION_RULES)"`
Expected: Prints the rules dict.

**Commit:** `feat: add ENC combination rules and rate thresholds for checks 244, 245`
<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Implement check_enc_combinations() in global_checks.py

**Verifies:** l1-l2-checks.AC2.4, l1-l2-checks.AC2.5

**Files:**
- Modify: `src/scdm_qa/validation/global_checks.py`

**Implementation:**

Add import:

```python
from scdm_qa.schemas.checks import ENC_COMBINATION_RULES, ENC_RATE_THRESHOLDS
```

Add new function:

```python
def check_enc_combinations(
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame],
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    """Checks 244 and 245: Validate ENC field combinations.

    244: Flag rows not matching valid combination rules.
    245: Flag EncType groups exceeding rate threshold for invalid combos.

    Returns one StepResult for check 244, plus one per EncType threshold test for 245.
    """
    if schema.table_key != "encounter":
        return []

    required_cols = {"EncType", "DDate", "Discharge_Disposition", "Discharge_Status"}

    all_rows: list[pl.DataFrame] = []
    for chunk in chunks:
        if required_cols.issubset(set(chunk.columns)):
            all_rows.append(chunk.select(list(required_cols)))

    if not all_rows:
        return []

    combined = pl.concat(all_rows)
    total_rows = combined.height

    # Derive ddate_state column
    combined = combined.with_columns(
        pl.when(pl.col("DDate").is_null())
        .then(pl.lit("Null"))
        .otherwise(pl.lit("Present"))
        .alias("_ddate_state")
    )

    # Check each row against combination rules
    # Build a boolean column: True if row is INVALID
    invalid_mask = pl.lit(False)
    for enc_type, (ddate_req, disp_req, status_req) in ENC_COMBINATION_RULES.items():
        type_match = pl.col("EncType") == enc_type
        violation = pl.lit(False)
        if ddate_req:
            violation = violation | (pl.col("_ddate_state") == "Null")
        if disp_req:
            violation = violation | pl.col("Discharge_Disposition").is_null()
        if status_req:
            violation = violation | pl.col("Discharge_Status").is_null()
        invalid_mask = invalid_mask | (type_match & violation)

    # Also flag unknown EncType values
    known_types = list(ENC_COMBINATION_RULES.keys())
    invalid_mask = invalid_mask | ~pl.col("EncType").is_in(known_types)

    combined = combined.with_columns(invalid_mask.alias("_invalid"))
    invalid_rows = combined.filter(pl.col("_invalid"))

    # Check 244: per-row invalid combos
    n_failed_244 = invalid_rows.height
    n_passed_244 = total_rows - n_failed_244

    failing_244 = None
    if n_failed_244 > 0:
        failing_244 = invalid_rows.drop("_ddate_state", "_invalid").head(max_failing_rows)

    results: list[StepResult] = [
        StepResult(
            step_index=-1,
            assertion_type="enc_combinations",
            column="EncType, DDate, Discharge_Disposition, Discharge_Status",
            description="Valid ENC field combination (check 244)",
            n_passed=n_passed_244,
            n_failed=n_failed_244,
            failing_rows=failing_244,
            check_id="244",
            severity="Fail",
        )
    ]

    # Check 245: rate threshold per EncType
    for enc_type, threshold in ENC_RATE_THRESHOLDS.items():
        type_rows = combined.filter(pl.col("EncType") == enc_type)
        type_total = type_rows.height
        if type_total == 0:
            continue

        type_invalid = type_rows.filter(pl.col("_invalid")).height
        rate = type_invalid / type_total

        if rate > threshold:
            n_failed = type_invalid
            n_passed = type_total - type_invalid
        else:
            n_failed = 0
            n_passed = type_total

        results.append(
            StepResult(
                step_index=-1,
                assertion_type="enc_combination_rate",
                column=f"EncType={enc_type}",
                description=f"{enc_type} invalid combo rate {'>' if rate > threshold else '<='} {threshold:.0%} (check 245)",
                n_passed=n_passed,
                n_failed=n_failed,
                failing_rows=None,
                check_id="245",
                severity="Fail",
            )
        )

    return results
```

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: Existing tests pass.

**Commit:** `feat: add check_enc_combinations for L2 checks 244, 245`
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Wire check_enc_combinations into pipeline.py

**Verifies:** l1-l2-checks.AC2.4, l1-l2-checks.AC2.5

**Files:**
- Modify: `src/scdm_qa/pipeline.py:16`

**Implementation:**

Update import:

```python
from scdm_qa.validation.global_checks import (
    check_sort_order, check_uniqueness, check_not_populated,
    check_date_ordering, check_cause_of_death,
    check_overlapping_spans, check_enrollment_gaps,
    check_enc_combinations,
)
```

In `_process_table()`, after the enrollment checks block, add:

```python
    # L2 checks: ENC field combinations (checks 244, 245)
    if schema.table_key == "encounter":
        enc_combo_reader = create_reader(file_path, chunk_size=config.chunk_size)
        enc_combo_steps = check_enc_combinations(
            schema, enc_combo_reader.chunks(),
            max_failing_rows=config.max_failing_rows,
        )
        global_steps.extend(enc_combo_steps)
```

**Verification:**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

**Commit:** `feat: wire check_enc_combinations into pipeline`
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Tests for ENC combination checks

**Verifies:** l1-l2-checks.AC2.4, l1-l2-checks.AC2.5, l1-l2-checks.AC4.1

**Files:**
- Modify: `tests/test_global_checks.py`

**Implementation:**

Add new test classes to `tests/test_global_checks.py`.

**Testing:**

Tests must verify each AC listed:

- **l1-l2-checks.AC2.4 (invalid combo detected):** Create encounter data with an IP row where DDate is null (IP requires DDate Present). Assert check 244 result has n_failed > 0 and check_id="244".

- **l1-l2-checks.AC2.4 (valid combo passes):** Create encounter data with an IP row where DDate is present AND Discharge_Disposition and Discharge_Status are set. Assert n_failed == 0 for check 244.

- **l1-l2-checks.AC2.4 (AV with nulls passes):** Create encounter data with an AV row where DDate, Discharge_Disposition, Discharge_Status are all null. Assert this passes (AV doesn't require these fields).

- **l1-l2-checks.AC2.5 (threshold exceeded):** Create encounter data with many IP rows, >5% having invalid combos. Assert check 245 result for EncType=IP has n_failed > 0 and check_id="245".

- **l1-l2-checks.AC2.5 (threshold not exceeded):** Create encounter data with IP rows where <5% have invalid combos. Assert check 245 result for EncType=IP has n_failed == 0.

- **l1-l2-checks.AC4.1:** Assert check_id="244" and check_id="245" on respective results.

- **Non-encounter table:** Call with non-encounter schema. Assert empty list.

Use `get_schema("encounter")` for the schema. Use `iter([df])` for chunks. DataFrames need EncType, DDate, Discharge_Disposition, Discharge_Status columns.

**Verification:**

Run: `uv run pytest tests/test_global_checks.py -v`
Expected: All tests pass.

Run: `uv run pytest tests/ -v`
Expected: Full suite passes.

**Commit:** `test: add tests for L2 ENC combination checks 244, 245`
<!-- END_TASK_4 -->

<!-- END_SUBCOMPONENT_A -->

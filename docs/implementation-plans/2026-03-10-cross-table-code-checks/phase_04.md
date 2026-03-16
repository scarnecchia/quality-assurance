# Cross-Table Checks & Code/CodeType Validation — Implementation Plan

**Goal:** Extend the SCDM QA pipeline with L1 code/codetype checks and L2 cross-table validation via DuckDB.

**Architecture:** Two-level validation pipeline. L1 adds code format/length checks (223, 228) to the existing per-chunk pointblank chain. L2 adds a new DuckDB-based cross-table phase that runs after all L1 processing. Both levels independently controllable via CLI flags and TOML config.

**Tech Stack:** Python 3.12+, Polars, pointblank, DuckDB, Typer, pytest

**Scope:** 7 phases from original design (phases 1–7)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### cross-table-code-checks.AC1: Cross-table validation phase
- **cross-table-code-checks.AC1.1 Success:** Cross-table check rules load from `cross_table_checks.json` and parse into frozen dataclasses

---

## Phase 4: Cross-Table Check Spec File + Parser

This phase creates the JSON spec file defining cross-table check rules and a parser module that loads them into frozen dataclasses. It follows the same pattern established in Phase 2 for code checks.

**Existing patterns to follow:**
- `src/scdm_qa/schemas/models.py` — all frozen dataclasses
- `src/scdm_qa/schemas/checks.py` — tuple registries with `table_key` filtering functions
- Phase 2's `code_checks.py` — lazy-loaded JSON parsing with `ConfigError` on malformed input

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Add CrossTableCheckDef to models.py

**Verifies:** cross-table-code-checks.AC1.1

**Files:**
- Modify: `src/scdm_qa/schemas/models.py` (after LengthCheckDef from Phase 2)
- Test: `tests/test_cross_table_checks.py` (new file)

**Implementation:**

Add a new frozen dataclass for cross-table check definitions. Cross-table checks have different types (referential_integrity, length_consistency, cross_date_compare, length_excess, column_mismatch), each with different parameters. Use a discriminated union via the `check_type` field:

```python
@dataclass(frozen=True)
class CrossTableCheckDef:
    check_id: str  # e.g. "201", "203", "205"
    check_type: str  # "referential_integrity" | "length_consistency" | "cross_date_compare" | "length_excess" | "column_mismatch"
    severity: str  # "Fail" | "Warn" | "Note"
    description: str

    # Tables involved
    source_table: str  # primary table for this check
    reference_table: str | None  # join target (None for single-table checks like 224)

    # Column configuration (interpretation depends on check_type)
    source_column: str | None  # e.g. "PatID" for referential_integrity
    reference_column: str | None  # e.g. "PatID" for join column in reference table
    target_column: str | None  # secondary column (e.g. date column for cross_date_compare)

    # For column_mismatch (check 224): compare two columns in same table
    column_a: str | None  # e.g. "Hispanic"
    column_b: str | None  # e.g. "ImputedHispanic"

    # For length_consistency (check 203): compare same column across multiple tables
    table_group: tuple[str, ...] | None = None  # e.g. ("diagnosis", "procedure", "encounter")
```

Note: Many fields are `None` depending on `check_type`. The parser validates that required fields for each check_type are present.

For check 203 (length_consistency), `source_table` is used as the column name to compare and `table_group` lists all tables in the comparison group. The `source_table` field is set to the first table in the group for identification purposes.

**Testing:**

- AC1.1: Construct `CrossTableCheckDef` instances for each check_type, verify all fields accessible and dataclass is frozen.

**Verification:**

Run: `uv run pytest tests/test_cross_table_checks.py -v`
Expected: Tests pass.

**Commit:** `feat: add CrossTableCheckDef model`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create cross_table_checks.json spec file

**Files:**
- Create: `src/scdm_qa/schemas/cross_table_checks.json`

**Implementation:**

Create the JSON spec file with all cross-table check definitions. Structure:

```json
{
  "checks": [
    {
      "check_id": "201",
      "check_type": "referential_integrity",
      "severity": "Warn",
      "description": "PatID in source table but not in enrollment",
      "source_table": "<table_key>",
      "reference_table": "enrollment",
      "source_column": "PatID",
      "reference_column": "PatID",
      "target_column": null,
      "column_a": null,
      "column_b": null,
      "table_group": null
    }
  ]
}
```

**Checks to define:**

**Check 201 — Referential integrity (PatID):**
One entry per source table that should reference enrollment. Source tables: "diagnosis", "procedure", "encounter", "dispensing", "cause_of_death", "inpatient_pharmacy", "lab_result", "prescribing", "vital_signs", "tranx", "provider_specialty", "facility", "benefit". Each checks `PatID NOT IN enrollment.PatID`.

**Check 203 — Variable length consistency:**
Compare max string lengths for the same column across table groups. Uses `table_group` field to list all tables in the comparison. One entry per column-group combination:
- "PatID" across table_group: ("enrollment", "demographic", "diagnosis", "procedure", "encounter", "dispensing", "cause_of_death", "inpatient_pharmacy", "lab_result", "prescribing", "vital_signs", "tranx", "provider_specialty", "facility", "benefit")
- "ProviderID" across table_group: ("diagnosis", "procedure", "encounter", "dispensing", "inpatient_pharmacy", "lab_result", "prescribing")
- source_column is the column name being compared (e.g., "PatID"), source_table is the first table in the group

**Check 205 — Enr_Start before Birth_Date:**
source_table: "enrollment", reference_table: "demographic", source_column: "PatID", reference_column: "PatID", target_column: "Enr_Start" (compared against demographic.Birth_Date).

**Check 206 — ADate/DDate before Birth_Date:**
Multiple entries: one for encounter ADate, one for encounter DDate, one for diagnosis ADate, etc. source_table varies, reference_table: "demographic", join on PatID.

**Check 227 — PostalCode_Date before Birth_Date:**
source_table: "address_history", reference_table: "demographic", join on PatID, compare PostalCode_Date.

**Check 209 — Variable length excess:**
Actual max column length much smaller than declared schema length. source_table varies (one per table+column combo where this is relevant).

**Check 224 — Hispanic ≠ ImputedHispanic:**
Single-table check. source_table: "demographic", reference_table: null, column_a: "Hispanic", column_b: "ImputedHispanic".

**Verification:**

Run: `python -c "import json; json.load(open('src/scdm_qa/schemas/cross_table_checks.json'))"`
Expected: Parses without error.

**Commit:** `feat: add cross_table_checks.json spec file`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Create cross_table_checks.py parser module

**Verifies:** cross-table-code-checks.AC1.1

**Files:**
- Create: `src/scdm_qa/schemas/cross_table_checks.py`
- Test: `tests/test_cross_table_checks.py` (extend)

**Implementation:**

Create a parser module following the same pattern as `code_checks.py` from Phase 2:

1. `load_cross_table_checks()` — reads `cross_table_checks.json` from the package directory, parses JSON, validates required fields per check_type, constructs `CrossTableCheckDef` instances. Returns `tuple[CrossTableCheckDef, ...]`.

2. Validation per check_type:
   - `referential_integrity`: requires `source_column`, `reference_column`, `reference_table`
   - `length_consistency`: requires `source_column` (the column name to compare across tables)
   - `cross_date_compare`: requires `source_column` (join key), `reference_column` (join key), `reference_table`, `target_column` (date column in source)
   - `length_excess`: requires `source_column`
   - `column_mismatch`: requires `column_a`, `column_b`, `reference_table` must be None

3. Module-level lazy-loaded registry:
   ```python
   _CROSS_TABLE_CHECKS: tuple[CrossTableCheckDef, ...] | None = None
   ```

4. `get_cross_table_checks() -> tuple[CrossTableCheckDef, ...]` — returns all checks.

5. `get_checks_for_table(table_key: str) -> tuple[CrossTableCheckDef, ...]` — returns checks where `source_table == table_key` OR `reference_table == table_key`.

**Testing:**

Tests must verify:
- AC1.1: `load_cross_table_checks()` returns non-empty tuple of `CrossTableCheckDef` instances. All fields are correct.
- Filtering: `get_checks_for_table("enrollment")` returns checks where enrollment is source or reference. `get_checks_for_table("nonexistent")` returns empty tuple.
- Malformed JSON (missing required field for check_type) raises `ConfigError`.

**Verification:**

Run: `uv run pytest tests/test_cross_table_checks.py -v`
Expected: All tests pass.

**Commit:** `feat: add cross_table_checks.py parser with lazy-loaded registry`

<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

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
- **cross-table-code-checks.AC2.1 Success:** Code check rules load from `code_checks.json` and parse into frozen dataclasses
- **cross-table-code-checks.AC2.2 Success:** Filtering by table key returns only rules for that table
- **cross-table-code-checks.AC2.8 Failure:** Malformed `code_checks.json` raises ConfigError at parse time

---

## Phase 2: Code Check Spec File + Parser

This phase creates the JSON spec file defining code format (check 223) and length (check 228) rules per CodeType per table, plus a parser module that loads them into frozen dataclasses.

**Existing patterns to follow:**
- Check definitions in `src/scdm_qa/schemas/checks.py` use frozen dataclass tuples with `table_key` filtering
- Models in `src/scdm_qa/schemas/models.py` are all `@dataclass(frozen=True)`
- `ConfigError` in `src/scdm_qa/config.py` is used for parse-time validation errors

<!-- START_SUBCOMPONENT_A (tasks 1-3) -->
<!-- START_TASK_1 -->
### Task 1: Add FormatCheckDef and LengthCheckDef to models.py

**Verifies:** cross-table-code-checks.AC2.1

**Files:**
- Modify: `src/scdm_qa/schemas/models.py:41-42` (after `DateOrderingDef`)
- Test: `tests/test_code_checks.py` (new file)

**Implementation:**

Add two new frozen dataclasses after `DateOrderingDef` in `models.py`:

```python
@dataclass(frozen=True)
class FormatCheckDef:
    check_id: str  # "223"
    table_key: str  # e.g. "diagnosis"
    column: str  # target column, e.g. "DX"
    codetype_column: str  # e.g. "DX_CodeType"
    codetype_value: str  # e.g. "09" for ICD-9
    check_subtype: str  # "no_decimal" | "regex" | "era_date" | "conditional_presence"
    severity: str  # "Fail" | "Warn" | "Note"
    pattern: str | None  # regex pattern for "regex" subtype, None otherwise
    description: str
    # era_date subtype fields (None for other subtypes)
    date_column: str | None = None  # e.g. "ADate" — the date column to compare against era boundary
    era_boundary: str | None = None  # e.g. "2015-10-01" — the ICD-9/ICD-10 transition date
    # conditional_presence subtype fields (None for other subtypes)
    condition_column: str | None = None  # e.g. "EncType" — column whose value drives the rule
    condition_values: tuple[str, ...] | None = None  # e.g. ("IP", "IS") — values that trigger the rule
    expect_null: bool = False  # if True, target column should be null; if False, should not be null


@dataclass(frozen=True)
class LengthCheckDef:
    check_id: str  # "228"
    table_key: str  # e.g. "diagnosis"
    column: str  # target column, e.g. "DX"
    codetype_column: str  # e.g. "DX_CodeType"
    codetype_value: str  # e.g. "09" for ICD-9
    min_length: int
    max_length: int
    severity: str  # "Warn"
    description: str
```

These follow the existing `L1CheckDef` and `DateOrderingDef` conventions: frozen, string fields for IDs/keys/columns, a `table_key` field for filtering.

**Testing:**

- cross-table-code-checks.AC2.1: Construct `FormatCheckDef` and `LengthCheckDef` instances, verify all fields are accessible and the dataclasses are frozen (assignment raises `AttributeError`).

**Verification:**

Run: `uv run pytest tests/test_code_checks.py -v`
Expected: Tests pass.

**Commit:** `feat: add FormatCheckDef and LengthCheckDef models`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create code_checks.json spec file

**Files:**
- Create: `src/scdm_qa/schemas/code_checks.json`

**Implementation:**

Create the JSON spec file with format (223) and length (228) rules. The structure should be:

```json
{
  "format_checks": [
    {
      "check_id": "223",
      "table_key": "<table>",
      "column": "<code_column>",
      "codetype_column": "<codetype_column>",
      "codetype_value": "<value>",
      "check_subtype": "<subtype>",
      "severity": "<severity>",
      "pattern": "<regex_or_null>",
      "description": "<human readable>",
      "date_column": "<date_col_or_null>",
      "era_boundary": "<YYYY-MM-DD_or_null>",
      "condition_column": "<col_or_null>",
      "condition_values": ["<val1>", "<val2>"],
      "expect_null": false
    }
  ],
  "length_checks": [
    {
      "check_id": "228",
      "table_key": "<table>",
      "column": "<code_column>",
      "codetype_column": "<codetype_column>",
      "codetype_value": "<value>",
      "min_length": <int>,
      "max_length": <int>,
      "severity": "Warn",
      "description": "<human readable>"
    }
  ]
}
```

**Rules to include (derived from SAS lkp_all_l2 and SCDM documentation):**

**Format checks (223):**

`no_decimal` subtype — codes must not contain periods:
- DIA table: DX column, DX_CodeType "09" and "10"
- PRO table: PX column, PX_CodeType "09" and "10"
- COD table: COD column, COD_CodeType "09" and "10"

`regex` subtype — codes must match pattern:
- PRO table: PX column, PX_CodeType "C4" (CPT-4), pattern `^\\d{4}[AaMmUu]$|^\\d{5}$`
- DIS table: NDC column, codetype "ND" (NDC), pattern `^\\d+$` (numeric only)
- INP table: NDC column, codetype "ND", pattern `^\\d+$`

`era_date` subtype — codetype/date consistency (include `date_column` and `era_boundary` in JSON):
- diagnosis table: DX_CodeType "09", date_column "ADate", era_boundary "2015-10-01" — ICD-9 on/after transition flagged
- diagnosis table: DX_CodeType "10", date_column "ADate", era_boundary "2015-10-01" — ICD-10 before transition flagged
- procedure table: PX_CodeType "09", date_column "ADate", era_boundary "2015-10-01" — ICD-9 on/after transition flagged
- procedure table: PX_CodeType "10", date_column "ADate", era_boundary "2015-10-01" — ICD-10 before transition flagged

`conditional_presence` subtype — field presence depends on another column (include `condition_column`, `condition_values`, `expect_null` in JSON):
- diagnosis table: PDX must not be null when EncType IN ("IP", "IS") → condition_column="EncType", condition_values=["IP","IS"], expect_null=false
- diagnosis table: PDX must be null when EncType IN ("AV", "ED", "OA") → condition_column="EncType", condition_values=["AV","ED","OA"], expect_null=true

Note: DDate/Discharge conditional presence rules based on EncType are already covered by existing checks 244/245 in `src/scdm_qa/schemas/checks.py` (ENC_COMBINATION_RULES). AC2.6 mentions these for completeness, but they do not need new entries in `code_checks.json`.

**Length checks (228):**
- DIA: DX, DX_CodeType "09", min 3, max 5
- DIA: DX, DX_CodeType "10", min 3, max 7
- PRO: PX, PX_CodeType "09", min 3, max 4
- PRO: PX, PX_CodeType "10", min 3, max 7
- PRO: PX, PX_CodeType "C4", min 5, max 5
- PRO: PX, PX_CodeType "H3", min 5, max 5
- PRO: PX, PX_CodeType "RE", min 4, max 4
- DIS: NDC, codetype "ND", min 9, max 11
- INP: NDC, codetype "ND", min 9, max 11

Note: The exact column names for codetype in each table vary (DX_CodeType in DIA, PX_CodeType in PRO, etc.). The implementor should cross-reference `tables_documentation.json` to confirm exact column names per table. Use the `table_key` values matching the existing normalised keys (e.g., "diagnosis", "procedure", "dispensing", "inpatient_pharmacy").

**Verification:**

Run: `python -c "import json; json.load(open('src/scdm_qa/schemas/code_checks.json'))"`
Expected: Parses without error.

**Commit:** `feat: add code_checks.json spec file with 223/228 rules`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Create code_checks.py parser module

**Verifies:** cross-table-code-checks.AC2.1, cross-table-code-checks.AC2.2, cross-table-code-checks.AC2.8

**Files:**
- Create: `src/scdm_qa/schemas/code_checks.py`
- Test: `tests/test_code_checks.py` (extend from Task 1)

**Implementation:**

Create a parser module that loads the JSON spec and produces tuples of frozen dataclasses. Follow the pattern in `checks.py` — tuple registries with table_key filtering functions.

The module should contain:

1. `load_code_checks()` — reads `code_checks.json` from the package directory (using `Path(__file__).parent / "code_checks.json"`), parses JSON, validates required fields, constructs `FormatCheckDef` and `LengthCheckDef` instances. Returns a tuple of `(tuple[FormatCheckDef, ...], tuple[LengthCheckDef, ...])`.

2. Validation: If a required key is missing from any check entry, raise `ConfigError` with a descriptive message identifying which entry and which key.

3. Module-level lazy-loaded registries (following the pattern of `ALL_L1_CHECKS` in `checks.py`):
   ```python
   _FORMAT_CHECKS: tuple[FormatCheckDef, ...] | None = None
   _LENGTH_CHECKS: tuple[LengthCheckDef, ...] | None = None
   ```
   Load lazily on first call to accessor functions.

4. `get_format_checks_for_table(table_key: str) -> tuple[FormatCheckDef, ...]` — returns format check rules for the given table.

5. `get_length_checks_for_table(table_key: str) -> tuple[LengthCheckDef, ...]` — returns length check rules for the given table.

**Testing:**

Tests must verify:
- cross-table-code-checks.AC2.1: `load_code_checks()` returns non-empty tuples of correct types. All fields are populated correctly from JSON.
- cross-table-code-checks.AC2.2: `get_format_checks_for_table("diagnosis")` returns only DIA rules. `get_format_checks_for_table("nonexistent")` returns empty tuple.
- cross-table-code-checks.AC2.8: Loading from a malformed JSON file (missing required keys) raises `ConfigError`.

For the AC2.8 test, use `monkeypatch` or `tmp_path` to point the parser at a malformed JSON file.

**Verification:**

Run: `uv run pytest tests/test_code_checks.py -v`
Expected: All tests pass.

**Commit:** `feat: add code_checks.py parser with lazy-loaded registries`

<!-- END_TASK_3 -->
<!-- END_SUBCOMPONENT_A -->

# Cross-Table Checks & Code/CodeType Validation Design

## Summary

This design extends the SCDM QA pipeline from single-table validation to a two-level system. Level 1 (L1) is the existing per-table, per-chunk validation loop — expanded here to include code and CodeType checks (checks 223 and 228) that validate medical coding formats, lengths, era-date consistency, and conditional field presence rules. Level 2 (L2) is a new cross-table validation phase that runs after all L1 processing completes and checks relationships between tables: referential integrity, date ordering across joins, variable length consistency, and demographic field agreement.

The implementation follows existing codebase conventions throughout: check rules live in JSON spec files that are parsed into frozen dataclasses, SQL-heavy cross-table logic delegates to DuckDB (which becomes a required dependency), and both L1 and L2 results produce `StepResult` objects that flow into the existing reporting and exit code machinery without modification. A synthetic `"cross_table"` table outcome keeps the data model intact. Both levels are independently runnable via CLI flags (`--l1-only`, `--l2-only`) or TOML config, with seven implementation phases ordered by dependency.

## Definition of Done

**Deliverable 1: Cross-table validation phase**
- A second pipeline phase that runs after all single-table checks complete
- Implements checks 201 (PatID referential integrity), 203 (variable length consistency), 205/206/227 (cross-table date vs Birth_Date), 209 (variable length > needed), and 224 (Hispanic ≠ ImputedHispanic)
- Cross-table relationships defined in a JSON spec file (not hardcoded)
- Runnable independently via CLI flags and TOML config
- Results feed into existing reporting and exit code logic

**Deliverable 2: Code/CodeType validation (checks 223, 228)**
- Format/pattern rules (check 223) and length rules (check 228) per CodeType per table
- Rules defined in a JSON spec file, derived from SAS lookup data during development (no runtime SAS dependency)
- Runs within the per-table validation loop (per-chunk checks)

**Deliverable 3: CLI + config**
- Flags on `scdm-qa run` to run single-table only, cross-table only, or both (default)
- TOML config equivalents for the same

**Out of scope:** Lab checks (Tier 3), L1 checks, external code vocabulary files (ICD/CPT/NDC code sets), runtime dependency on SAS lookup files

## Acceptance Criteria

### cross-table-code-checks.AC1: Cross-table validation phase
- **AC1.1** Cross-table check rules load from `cross_table_checks.json` and parse into frozen dataclasses
- **AC1.2** Check 201: PatID in diagnosis/procedure/etc. but not in enrollment is flagged as warn
- **AC1.3** Check 203: Different max string lengths for same column across table groups is flagged as fail
- **AC1.4** Check 205: Enr_Start before Birth_Date (joined on PatID) is flagged as warn
- **AC1.5** Check 206: ADate/DDate before Birth_Date (joined on PatID) is flagged as warn
- **AC1.6** Check 227: PostalCode_Date before Birth_Date (joined on PatID) is flagged as warn
- **AC1.7** Check 209: Actual max column length much smaller than declared schema length across tables is flagged as warn
- **AC1.8** Check 224: Hispanic ≠ ImputedHispanic (both non-null) in demographic is flagged as note
- **AC1.9** Cross-table results produce HTML report page and appear in index summary
- **AC1.10 Failure:** Missing reference table in config → check skipped with log warning, no crash
- **AC1.11 Failure:** DuckDB SQL error on a single check → that check returns error StepResult, pipeline continues
- **AC1.12 Edge:** SAS7BDAT table files are converted to temp parquet before DuckDB registration

### cross-table-code-checks.AC2: Code/CodeType validation
- **AC2.1** Code check rules load from `code_checks.json` and parse into frozen dataclasses
- **AC2.2** Filtering by table key returns only rules for that table
- **AC2.3** Check 223 no_decimal: ICD-9/10 codes containing periods are flagged (DIA, PRO, COD)
- **AC2.4** Check 223 regex: CPT-4 codes not matching `^\d{4}[AaMmUu]$|^\d{5}$` are flagged; NDC codes with non-numeric chars are flagged
- **AC2.5** Check 223 era_date: ICD-9 codes on/after 2015-10-01 and ICD-10 codes before 2015-10-01 are flagged (DIA, PRO)
- **AC2.6** Check 223 conditional_presence: PDX null when EncType=IP/IS is flagged; PDX not-null when EncType=AV/ED/OA is flagged; DDate/Discharge fields follow EncType rules
- **AC2.7** Check 228: Code lengths outside min/max range per CodeType are flagged (ICD-9 DX 3-5, ICD-10 DX 3-7, NDC 9-11, CPT 5, etc.)
- **AC2.8 Failure:** Malformed `code_checks.json` raises ConfigError at parse time
- **AC2.9 Edge:** Rows where codetype column is null are skipped (not flagged)

### cross-table-code-checks.AC3: CLI + config phase isolation
- **AC3.1** `--l1-only` runs only per-table validation, skips cross-table
- **AC3.2** `--l2-only` runs only cross-table validation, skips per-table
- **AC3.3** Default (no flags) runs both L1 and L2
- **AC3.4** `--l1-only --l2-only` together raises error
- **AC3.5** TOML `run_l1`/`run_l2` options control phase execution
- **AC3.6** CLI flags override TOML config values
- **AC3.7** `--table` filter with L2 only runs cross-table checks involving that table
- **AC3.8** Exit code reflects failures from both L1 and L2 results

## Glossary

- **SCDM**: Sentinel Common Data Model — a standardised data model used by the FDA Sentinel System for analysing healthcare claims data. Tables like DIA (diagnoses), PRO (procedures), and ENR (enrolment) are SCDM table keys.
- **L1 / L2**: Level 1 and Level 2 validation phases. L1 = per-table, per-chunk checks. L2 = cross-table checks that require joining or comparing across multiple tables.
- **CodeType**: A column identifying the coding system used for a medical code value (e.g., ICD-9, ICD-10, CPT-4, NDC). Code checks apply rules conditionally based on this column's value.
- **ICD-9 / ICD-10**: International Classification of Diseases, 9th and 10th revisions — diagnosis and procedure coding systems. ICD-10 replaced ICD-9 in the US on 2015-10-01, which is the era-date boundary used in check 223.
- **CPT-4**: Current Procedural Terminology, 4th edition — a procedure coding system with fixed-length numeric codes (5 digits) and modifier suffixes.
- **NDC**: National Drug Code — an 11-digit numeric identifier for drug products.
- **PDX**: Principal Diagnosis — a field that is conditionally required or prohibited depending on the encounter type (`EncType`).
- **EncType**: Encounter Type — a field indicating care setting (e.g., IP = inpatient, AV = ambulatory visit, ED = emergency department). Drives conditional presence rules in check 223.
- **PatID**: Patient identifier — the primary key used to join records across SCDM tables in cross-table checks.
- **Referential integrity**: A database constraint concept — every PatID that appears in a detail table (e.g., diagnoses) must also exist in a reference table (e.g., enrolment). Check 201 enforces this.
- **DuckDB**: An in-process analytical SQL database. Used here to execute cross-table SQL queries against parquet (and SAS-converted) files without loading entire datasets into memory.
- **pointblank**: A Python data validation library. Used to express per-column, per-chunk validation rules as a chainable `Validate` object. Code checks in L1 are appended to its assertion chain.
- **Polars**: A DataFrame library used throughout the pipeline. Chunks are `polars.DataFrame` objects passed through the validation loop.
- **SAS7BDAT**: A proprietary SAS binary data format. Some SCDM sites deliver tables in this format; DuckDB cannot read it natively, so it is converted to parquet before L2 registration.
- **StepResult**: The internal result object produced by each validation check — carries pass/fail/warn/note status, row counts, and (pending) a `check_id`. Both L1 and L2 produce these.
- **TableOutcome**: An aggregated result per table (or the synthetic `"cross_table"` entry) that wraps a list of `StepResult` objects for reporting and exit code logic.
- **Frozen dataclass**: A Python `dataclass(frozen=True)` — immutable after construction. Used for all data models in the codebase, including new check definition models.
- **`pre=` filter**: A pointblank parameter that filters a DataFrame before a validation assertion runs. Used to apply code checks only to rows with a matching CodeType value.
- **check_id**: A string field on `StepResult` (proposed in the companion L1/L2 design plan) used to trace results back to their originating check specification.

## Architecture

Two-level validation pipeline with level isolation. Level 1 (L1) runs per-table, per-chunk validation including new code/codetype checks. Level 2 (L2) runs cross-table checks via DuckDB after all L1 processing completes. Each level is independently runnable via CLI flags or TOML config.

DuckDB moves from optional to required dependency. Cross-table checks register all configured table files as DuckDB views and execute SQL queries for each check. This handles datasets that don't fit in memory without requiring custom out-of-core logic.

Check rules live in two JSON spec files shipped with the package:
- `src/scdm_qa/schemas/code_checks.json` — format (223) and length (228) rules per CodeType
- `src/scdm_qa/schemas/cross_table_checks.json` — cross-table relationship checks (201, 203, 205, 206, 209, 224, 227)

Results from both levels produce `StepResult` objects and feed into the existing reporting and exit code logic unchanged.

### L1: Code/CodeType Checks (223, 228)

These run inside the existing per-chunk validation loop in `build_validation()` (`src/scdm_qa/schemas/validation.py`). For each chunk, the code check rules matching the current table are appended to the pointblank `Validate` chain.

Each rule applies only to rows where the codetype column matches a specific value, using the existing `pre=` filter pattern (same as conditional rules today).

Check 223 has four sub-types:
- **no_decimal**: Code must not contain a period (ICD-9/10 in DIA, PRO, COD)
- **regex**: Code must match a pattern (CPT-4 format, NDC numeric-only, etc.)
- **era_date**: CodeType must be consistent with service date (ICD-9 before Oct 2015, ICD-10 after)
- **conditional_presence**: Field presence depends on another field's value (PDX vs EncType, DDate vs EncType, Discharge fields vs EncType, Facility fields)

Check 228 is uniform: code string length must fall within a min/max range for the given CodeType.

### L2: Cross-Table Checks

A new module `src/scdm_qa/validation/cross_table.py` orchestrates the DuckDB-based cross-table phase:

1. Open a single DuckDB connection
2. Register each configured table file as a view (`CREATE VIEW {table_key} AS SELECT * FROM read_parquet('{path}')`)
3. For SAS files: convert to temp parquet via existing chunked reader before registering
4. Dispatch each check to a type-specific SQL handler
5. Collect `StepResult` objects
6. Close connection

Check-type SQL handlers:
- **referential_integrity (201)**: `WHERE source.PatID NOT IN (SELECT PatID FROM reference)` — one StepResult per source×reference table pair
- **length_consistency (203)**: Compare `MAX(LENGTH(CAST(col AS VARCHAR)))` across table groups — flags when max lengths differ
- **cross_date_compare (205, 206, 227)**: JOIN on PatID, compare dates across tables
- **length_excess (209)**: Compare actual max length vs declared schema length across tables
- **column_mismatch (224)**: Single-table check — `WHERE col_a != col_b AND both NOT NULL`

### Pipeline Integration

`run_pipeline()` in `src/scdm_qa/pipeline.py` gains conditional two-level execution:

1. If `config.run_l1`: execute existing per-table loop (with code checks now in the chain), collect `TableOutcome` list
2. If `config.run_l2`: call `run_cross_table_checks()`, wrap results in a synthetic `TableOutcome` with `table_key="cross_table"`
3. Append L2 outcome to the outcomes list
4. `compute_exit_code()` processes L2 results identically — no changes to exit code logic

The synthetic `"cross_table"` outcome keeps the existing data model intact. Reporting generates a cross-table report page alongside per-table pages.

### CLI & Config

Two new fields on `QAConfig` (`src/scdm_qa/config.py`):
- `run_l1: bool = True`
- `run_l2: bool = True`

TOML config:
```toml
[options]
run_l1 = true
run_l2 = false
```

Two new CLI flags on `run` command (`src/scdm_qa/cli.py`):
- `--l1-only`: sets `run_l1=True, run_l2=False`
- `--l2-only`: sets `run_l1=False, run_l2=True`

Without flags, both default `True`. Specifying both `--l1-only` and `--l2-only` is an error. CLI flags override TOML config.

When `--table` is used with L2 enabled, cross-table checks only run for checks involving the filtered table. If the table isn't in any cross-table check, L2 is silently skipped.

## Existing Patterns

Investigation found these relevant patterns:

**DuckDB integration** (`src/scdm_qa/validation/global_checks.py`): Existing `_uniqueness_duckdb()` uses try-except import with `None` return fallback. Since DuckDB becomes required, cross-table checks can import directly without the fallback pattern. The existing uniqueness check's fallback can remain for backwards compatibility.

**Global checks pattern** (`src/scdm_qa/validation/global_checks.py`): Functions take file paths, create fresh readers, return `StepResult` objects. Cross-table checks follow the same contract — take config + check definitions, return `StepResult` list.

**Conditional rule filtering** (`src/scdm_qa/schemas/validation.py`): The `pre=` parameter on pointblank assertions filters chunks before validation. Code checks use the same pattern: `pre=lambda df: df.filter(pl.col(codetype_col).is_in([codetype_value]))`.

**JSON spec parsing** (`src/scdm_qa/schemas/parser.py`): Existing parser reads `tables_documentation.json` and produces frozen dataclasses. Code check and cross-table check parsers follow the same approach — JSON → frozen dataclasses with validation.

**Frozen dataclass models** (`src/scdm_qa/schemas/models.py`): All data models are frozen dataclasses. New check definition models follow the same convention.

No pattern divergence introduced.

## Implementation Phases

<!-- START_PHASE_1 -->
### Phase 1: DuckDB Required + Config/CLI Extensions

**Goal:** Make DuckDB a required dependency and add L1/L2 phase isolation to config and CLI.

**Components:**
- `pyproject.toml` — move duckdb from `[project.optional-dependencies]` to `[project.dependencies]`
- `QAConfig` in `src/scdm_qa/config.py` — add `run_l1: bool = True` and `run_l2: bool = True` fields
- `load_config()` in `src/scdm_qa/config.py` — parse `run_l1` and `run_l2` from TOML `[options]`
- `run` command in `src/scdm_qa/cli.py` — add `--l1-only` and `--l2-only` flags, validate mutual exclusion, override config

**Dependencies:** None

**Done when:** `uv sync` installs duckdb, config parses L1/L2 options, CLI flags override config, `--l1-only --l2-only` raises error, existing tests pass
<!-- END_PHASE_1 -->

<!-- START_PHASE_2 -->
### Phase 2: Code Check Spec File + Parser

**Goal:** Define code/codetype validation rules in JSON and parse them into frozen dataclasses.

**Components:**
- `src/scdm_qa/schemas/code_checks.json` — format check (223) and length check (228) rule definitions, derived from SAS `lkp_all_l2.sas7bdat` during development
- `src/scdm_qa/schemas/code_checks.py` — parser module with `FormatCheck` and `LengthCheck` frozen dataclasses, `load_code_checks()` function, `get_code_checks(table_key)` filter function

**Dependencies:** None (can run in parallel with Phase 1)

**Done when:** JSON spec contains all 223/228 rules from SAS lookup, parser produces correct dataclasses, filtering by table key returns correct subset, malformed JSON raises `ConfigError`

**Covers:** cross-table-code-checks.AC2.1, cross-table-code-checks.AC2.2
<!-- END_PHASE_2 -->

<!-- START_PHASE_3 -->
### Phase 3: Code Check Validation Integration

**Goal:** Wire code check rules into the per-chunk validation chain.

**Components:**
- `build_validation()` in `src/scdm_qa/schemas/validation.py` — after existing column rules, append code check assertions using parsed `FormatCheck` and `LengthCheck` rules
- Check-type dispatchers: `no_decimal` → regex without period, `regex` → pattern match, `no_non_numeric` → digits-only regex, `era_date` → date comparison with codetype filter, `conditional_presence` → null/not-null with cross-column filter, `length` → length range regex

**Dependencies:** Phase 2 (parser available)

**Done when:** Synthetic data with known bad codes (decimals in ICD, wrong lengths, NDC with letters, wrong-era ICD codes, incorrect PDX/EncType combos) produces expected `StepResult` failures. All check 223 sub-types and check 228 rules verified.

**Covers:** cross-table-code-checks.AC2.3, cross-table-code-checks.AC2.4, cross-table-code-checks.AC2.5, cross-table-code-checks.AC2.6, cross-table-code-checks.AC2.7
<!-- END_PHASE_3 -->

<!-- START_PHASE_4 -->
### Phase 4: Cross-Table Check Spec File + Parser

**Goal:** Define cross-table check rules in JSON and parse them into frozen dataclasses.

**Components:**
- `src/scdm_qa/schemas/cross_table_checks.json` — check definitions for 201, 203, 205, 206, 209, 224, 227
- `src/scdm_qa/schemas/cross_table_checks.py` — parser module with `CrossTableCheck` frozen dataclass (with check_type discriminator), `load_cross_table_checks()` function, `get_checks_for_table(table_key)` filter function

**Dependencies:** None (can run in parallel with Phases 1-3)

**Done when:** JSON spec contains all cross-table check definitions, parser produces correct dataclasses, filtering by table key returns checks involving that table, malformed JSON raises `ConfigError`

**Covers:** cross-table-code-checks.AC1.1
<!-- END_PHASE_4 -->

<!-- START_PHASE_5 -->
### Phase 5: DuckDB Cross-Table Engine

**Goal:** Implement the DuckDB-based cross-table validation orchestrator and SQL handlers.

**Components:**
- `src/scdm_qa/validation/cross_table.py` — `run_cross_table_checks(config, checks)` orchestrator, DuckDB view registration, SAS-to-temp-parquet conversion, check-type SQL handlers (referential_integrity, length_consistency, cross_date_compare, length_excess, column_mismatch)

**Dependencies:** Phase 4 (cross-table check definitions available)

**Done when:** Each check type produces correct `StepResult` objects against synthetic multi-table parquet files. Handles missing tables gracefully (skip with warning). DuckDB errors don't crash pipeline.

**Covers:** cross-table-code-checks.AC1.2, cross-table-code-checks.AC1.3, cross-table-code-checks.AC1.4, cross-table-code-checks.AC1.5, cross-table-code-checks.AC1.6, cross-table-code-checks.AC1.7, cross-table-code-checks.AC1.8
<!-- END_PHASE_5 -->

<!-- START_PHASE_6 -->
### Phase 6: Pipeline Two-Level Orchestration

**Goal:** Wire L1 and L2 into `run_pipeline()` with phase isolation.

**Components:**
- `run_pipeline()` in `src/scdm_qa/pipeline.py` — conditional L1 execution (existing loop), conditional L2 execution (cross-table engine), synthetic `"cross_table"` TableOutcome, `--table` filter interaction with L2

**Dependencies:** Phase 1 (config/CLI), Phase 3 (code checks wired), Phase 5 (cross-table engine)

**Done when:** `--l1-only` skips L2, `--l2-only` skips L1, default runs both, cross-table results appear in outcomes, exit codes reflect L2 failures, `--table` filters L2 checks to relevant table

**Covers:** cross-table-code-checks.AC3.1, cross-table-code-checks.AC3.2, cross-table-code-checks.AC3.3, cross-table-code-checks.AC3.4
<!-- END_PHASE_6 -->

<!-- START_PHASE_7 -->
### Phase 7: Reporting Integration

**Goal:** Cross-table results appear in HTML reports alongside per-table reports.

**Components:**
- `save_table_report()` in `src/scdm_qa/reporting/builder.py` — handle `"cross_table"` table_key (may need adjusted heading/layout)
- `save_index()` in `src/scdm_qa/reporting/index.py` — include cross-table summary in index page

**Dependencies:** Phase 6 (cross-table outcome available)

**Done when:** Cross-table report page is generated, index page includes cross-table summary, existing per-table reports unchanged

**Covers:** cross-table-code-checks.AC1.9
<!-- END_PHASE_7 -->

## Additional Considerations

**SAS file handling in L2:** DuckDB cannot read SAS7BDAT natively. When a configured table file is SAS, the cross-table engine converts it to a temporary parquet file via the existing chunked reader before registering with DuckDB. A warning is logged about potential performance impact for large SAS files.

**Missing tables in L2:** Not all SCDM sites provide all 19 tables. If a cross-table check references a table not in the user's config, that check is skipped with a log warning. This is expected behaviour, not an error.

**StepResult.check_id:** The L1/L2 design plan (`2026-03-10-l1-l2-checks.md`) already proposes adding `check_id: str | None` to `StepResult`. This design depends on that field for check traceability. If the L1/L2 plan hasn't been implemented yet, Phase 2 or 3 should add the field.

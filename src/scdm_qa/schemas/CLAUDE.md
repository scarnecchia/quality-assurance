# Schemas Domain

Last verified: 2026-03-10

## Purpose
Parses the upstream SCDM specification into typed data models and builds pointblank validation plans from them. This is the single source of truth for what constitutes a valid SCDM table.

## Contracts
- **Exposes**: `get_schema(table_key) -> TableSchema`, `list_table_keys()`, `build_validation(schema, df) -> Validate`, `parse_spec() -> list[TableSchema]`, `L1_CHECK_REGISTRY`, `DATE_ORDERING_CHECKS`, `ENC_COMBINATION_RULES`, `ENC_RATE_THRESHOLDS`, `L1CheckDef`, `DateOrderingDef`, `FormatCheckDef`, `LengthCheckDef`, `CrossTableCheckDef`, `get_format_checks_for_table()`, `get_length_checks_for_table()`, `get_cross_table_checks()`, `get_checks_for_table()`
- **Guarantees**: Lazy-loaded registry caches parsed schemas. All 19 SCDM tables are parsed from `tables_documentation.json`. `build_validation` produces a pointblank `Validate` object with nullability, enum, length, conditional rules, L1 per-chunk checks (122, 124, 128), code format checks (223), and code length checks (228). Check registries (`L1_CHECK_REGISTRY`, `DATE_ORDERING_CHECKS`) map table keys to their applicable checks. ENC combination rules define valid field combinations per EncType for checks 244/245. Code checks are loaded from `code_checks.json`. Cross-table checks are loaded from `cross_table_checks.json`.
- **Expects**: `tables_documentation.json`, `code_checks.json`, and `cross_table_checks.json` are present and structurally valid. pointblank is installed.

## Dependencies
- **Uses**: pointblank (validation builder), polars (DataFrame for validation)
- **Used by**: pipeline, validation/runner, CLI (schema command)
- **Boundary**: Does not read files or run validation -- only defines rules

## Key Decisions
- JSON spec over hardcoded definitions: Upstream SCDM spec is authoritative; parser extracts structure
- Lazy registry singleton: Avoids parsing 3k-line JSON on import
- Custom rules via `load_custom_rules()`: Users extend validation by dropping Python files in a directory

## Invariants
- `TableSchema`, `ColumnDef`, `ConditionalRule`, `L1CheckDef`, `DateOrderingDef`, `FormatCheckDef`, `LengthCheckDef`, `CrossTableCheckDef` are all frozen dataclasses
- `table_key` is the normalised lowercase key used everywhere as the canonical table identifier
- `tables_documentation.json`, `code_checks.json`, `cross_table_checks.json` must not be edited (upstream artifacts)

## Key Files
- `models.py` - `TableSchema`, `ColumnDef`, `ConditionalRule`, `L1CheckDef`, `DateOrderingDef`, `FormatCheckDef`, `LengthCheckDef`, `CrossTableCheckDef` dataclasses
- `parser.py` - JSON spec parser
- `validation.py` - pointblank validation builder (includes code format 223 and length 228 checks)
- `custom_rules.py` - User-provided rule extension loader
- `checks.py` - L1 check registry, L2 date ordering defs, L2 ENC combination rules and thresholds
- `code_checks.py` - Parser for `code_checks.json` (format 223, length 228 check definitions)
- `cross_table_checks.py` - Parser for `cross_table_checks.json` (cross-table check definitions)
- `code_checks.json` - Code/codetype validation rules (do not edit)
- `cross_table_checks.json` - Cross-table validation rules (do not edit)

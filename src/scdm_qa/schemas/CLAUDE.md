# Schemas Domain

Last verified: 2026-03-10

## Purpose
Parses the upstream SCDM specification into typed data models and builds pointblank validation plans from them. This is the single source of truth for what constitutes a valid SCDM table.

## Contracts
- **Exposes**: `get_schema(table_key) -> TableSchema`, `list_table_keys()`, `build_validation(schema, df) -> Validate`, `parse_spec() -> list[TableSchema]`, `ENC_COMBINATION_RULES`, `ENC_RATE_THRESHOLDS`
- **Guarantees**: Lazy-loaded registry caches parsed schemas. All 19 SCDM tables are parsed from `tables_documentation.json`. `build_validation` produces a pointblank `Validate` object with nullability, enum, length, and conditional rules. ENC combination rules define valid field combinations per EncType for checks 244/245.
- **Expects**: `tables_documentation.json` is present and structurally valid. pointblank is installed.

## Dependencies
- **Uses**: pointblank (validation builder), polars (DataFrame for validation)
- **Used by**: pipeline, validation/runner, CLI (schema command)
- **Boundary**: Does not read files or run validation -- only defines rules

## Key Decisions
- JSON spec over hardcoded definitions: Upstream SCDM spec is authoritative; parser extracts structure
- Lazy registry singleton: Avoids parsing 3k-line JSON on import
- Custom rules via `load_custom_rules()`: Users extend validation by dropping Python files in a directory

## Invariants
- `TableSchema`, `ColumnDef`, `ConditionalRule` are all frozen dataclasses
- `table_key` is the normalised lowercase key used everywhere as the canonical table identifier
- `tables_documentation.json` must not be edited (upstream artifact)

## Key Files
- `models.py` - `TableSchema`, `ColumnDef`, `ConditionalRule` dataclasses
- `parser.py` - JSON spec parser
- `validation.py` - pointblank validation builder
- `custom_rules.py` - User-provided rule extension loader
- `checks.py` - L1 check registry, L2 date ordering defs, L2 ENC combination rules and thresholds

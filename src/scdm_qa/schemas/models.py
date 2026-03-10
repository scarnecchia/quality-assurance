from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConditionalRule:
    target_column: str
    condition_column: str
    condition_values: frozenset[str]
    description: str


@dataclass(frozen=True)
class ColumnDef:
    name: str
    col_type: str  # "Numeric" or "Character"
    missing_allowed: bool
    length: int | None  # None for "variable" or Numeric types
    allowed_values: frozenset[str] | None  # None if not enumerated
    definition: str
    example: str


@dataclass(frozen=True)
class L1CheckDef:
    check_id: str  # SAS CheckID, e.g. "122"
    table_key: str  # normalised table key, e.g. "dispensing"
    column: str  # target column name, e.g. "NDC"
    check_type: str  # "leading_spaces" | "unexpected_zeros" | "non_numeric" | "not_populated"
    severity: str  # "Fail" | "Warn" | "Note"


@dataclass(frozen=True)
class DateOrderingDef:
    check_id: str  # "226"
    table_key: str  # e.g. "encounter"
    date_a: str  # column that should be <= date_b
    date_b: str  # column that should be >= date_a
    severity: str  # "Fail" | "Warn"
    description: str  # human-readable, e.g. "ADate <= DDate"


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


@dataclass(frozen=True)
class TableSchema:
    table_name: str
    table_key: str  # normalised key, e.g. "enrollment"
    description: str
    sort_order: tuple[str, ...]
    unique_row: tuple[str, ...]
    columns: tuple[ColumnDef, ...]
    conditional_rules: tuple[ConditionalRule, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def get_column(self, name: str) -> ColumnDef | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None

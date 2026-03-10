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

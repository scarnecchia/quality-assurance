from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnProfile:
    name: str
    col_type: str  # "Numeric" or "Character"
    total_count: int
    null_count: int
    distinct_count: int
    min_value: str | None  # string representation for display
    max_value: str | None
    value_frequencies: dict[str, int] | None  # for enumerated columns only

    @property
    def completeness(self) -> float:
        if self.total_count == 0:
            return 0.0
        return (self.total_count - self.null_count) / self.total_count

    @property
    def completeness_pct(self) -> float:
        return self.completeness * 100


@dataclass(frozen=True)
class ProfilingResult:
    table_key: str
    table_name: str
    total_rows: int
    columns: tuple[ColumnProfile, ...]

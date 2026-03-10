from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl


@dataclass(frozen=True)
class StepResult:
    step_index: int
    assertion_type: str  # e.g. "col_vals_not_null", "col_vals_in_set"
    column: str
    description: str
    n_passed: int
    n_failed: int
    failing_rows: pl.DataFrame | None  # bounded sample
    check_id: str | None = None
    severity: str | None = None  # "Fail" | "Warn" | "Note" | None

    @property
    def n_total(self) -> int:
        return self.n_passed + self.n_failed

    @property
    def f_passed(self) -> float:
        return self.n_passed / self.n_total if self.n_total > 0 else 1.0

    @property
    def f_failed(self) -> float:
        return self.n_failed / self.n_total if self.n_total > 0 else 0.0


@dataclass(frozen=True)
class ValidationResult:
    table_key: str
    table_name: str
    steps: tuple[StepResult, ...]
    total_rows: int
    chunks_processed: int

    @property
    def all_passed(self) -> bool:
        return all(s.n_failed == 0 for s in self.steps)

    @property
    def total_failures(self) -> int:
        return sum(s.n_failed for s in self.steps)

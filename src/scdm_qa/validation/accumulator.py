from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from scdm_qa.validation.results import StepResult, ValidationResult


@dataclass
class _MutableStepAccum:
    step_index: int
    assertion_type: str
    column: str
    description: str
    n_passed: int = 0
    n_failed: int = 0
    failing_rows: list[pl.DataFrame] = field(default_factory=list)
    failing_rows_count: int = 0
    check_id: str | None = None
    severity: str | None = None


class ValidationAccumulator:
    def __init__(
        self,
        table_key: str,
        table_name: str,
        *,
        max_failing_rows: int = 500,
    ) -> None:
        self._table_key = table_key
        self._table_name = table_name
        self._max_failing_rows = max_failing_rows
        self._steps: dict[int, _MutableStepAccum] = {}
        self._total_rows = 0
        self._chunks_processed = 0

    def add_chunk_results(
        self,
        chunk_row_count: int,
        step_descriptions: list[tuple[int, str, str, str, str | None, str | None]],
        n_passed: dict[int, int],
        n_failed: dict[int, int],
        extracts: dict[int, pl.DataFrame],
    ) -> None:
        self._total_rows += chunk_row_count
        self._chunks_processed += 1

        for step_index, assertion_type, column, description, check_id, severity in step_descriptions:
            if step_index not in self._steps:
                self._steps[step_index] = _MutableStepAccum(
                    step_index=step_index,
                    assertion_type=assertion_type,
                    column=column,
                    description=description,
                    check_id=check_id,
                    severity=severity,
                )

            accum = self._steps[step_index]
            accum.n_passed += n_passed.get(step_index, 0) or 0
            accum.n_failed += n_failed.get(step_index, 0) or 0

            if step_index in extracts and accum.failing_rows_count < self._max_failing_rows:
                extract = extracts[step_index]
                remaining = self._max_failing_rows - accum.failing_rows_count
                if extract.height > remaining:
                    extract = extract.head(remaining)
                accum.failing_rows.append(extract)
                accum.failing_rows_count += extract.height

    def result(self) -> ValidationResult:
        steps: list[StepResult] = []
        for idx in sorted(self._steps.keys()):
            accum = self._steps[idx]
            failing = None
            if accum.failing_rows:
                failing = pl.concat(accum.failing_rows)
            steps.append(
                StepResult(
                    step_index=accum.step_index,
                    assertion_type=accum.assertion_type,
                    column=accum.column,
                    description=accum.description,
                    n_passed=accum.n_passed,
                    n_failed=accum.n_failed,
                    failing_rows=failing,
                    check_id=accum.check_id,
                    severity=accum.severity,
                )
            )
        return ValidationResult(
            table_key=self._table_key,
            table_name=self._table_name,
            steps=tuple(steps),
            total_rows=self._total_rows,
            chunks_processed=self._chunks_processed,
        )

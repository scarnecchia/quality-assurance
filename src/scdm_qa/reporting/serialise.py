from __future__ import annotations

from datetime import UTC, datetime

from scdm_qa.profiling.results import ProfilingResult
from scdm_qa.validation.results import StepResult, ValidationResult


def serialise_step(step: StepResult, max_failing_rows: int) -> dict:
    """Convert a single StepResult to a serialisable dict.

    Args:
        step: The StepResult to serialise
        max_failing_rows: Maximum number of failing rows to include

    Returns:
        Dictionary with check_id, step_index, assertion_type, column, description,
        n_passed, n_failed, pass_rate, severity, and failing_rows.
    """
    failing_rows_list: list[dict] = []
    if step.failing_rows is not None:
        df_truncated = step.failing_rows.head(max_failing_rows)
        failing_rows_list = df_truncated.to_dicts()

    return {
        "check_id": step.check_id,
        "step_index": step.step_index,
        "assertion_type": step.assertion_type,
        "column": step.column,
        "description": step.description,
        "n_passed": step.n_passed,
        "n_failed": step.n_failed,
        "pass_rate": step.f_passed,
        "severity": step.severity,
        "failing_rows": failing_rows_list,
    }


def serialise_validation(result: ValidationResult, max_failing_rows: int) -> dict:
    """Convert a ValidationResult to a serialisable dict.

    Args:
        result: The ValidationResult to serialise
        max_failing_rows: Maximum number of failing rows to include in each step

    Returns:
        Dictionary with table_key, table_name, total_rows, chunks_processed, and steps.
    """
    return {
        "table_key": result.table_key,
        "table_name": result.table_name,
        "total_rows": result.total_rows,
        "chunks_processed": result.chunks_processed,
        "steps": [serialise_step(step, max_failing_rows) for step in result.steps],
    }


def serialise_profiling(result: ProfilingResult) -> dict:
    """Convert a ProfilingResult to a serialisable dict.

    Args:
        result: The ProfilingResult to serialise

    Returns:
        Dictionary with table_key, table_name, total_rows, and columns.
    """
    columns_list = [
        {
            "name": col.name,
            "col_type": col.col_type,
            "total_count": col.total_count,
            "null_count": col.null_count,
            "distinct_count": col.distinct_count,
            "min_value": col.min_value,
            "max_value": col.max_value,
            "completeness": col.completeness,
            "completeness_pct": col.completeness_pct,
        }
        for col in result.columns
    ]

    return {
        "table_key": result.table_key,
        "table_name": result.table_name,
        "total_rows": result.total_rows,
        "columns": columns_list,
    }


def serialise_run(
    results: list[tuple[ValidationResult, ProfilingResult]], *, max_failing_rows: int = 500
) -> dict:
    """Aggregate validation and profiling results into a single JSON-serialisable dict.

    Args:
        results: List of (ValidationResult, ProfilingResult) tuples
        max_failing_rows: Maximum number of failing rows to include per step (default 500)

    Returns:
        Dictionary with schema_version, generated_at, tables, and summary.
    """
    tables_dict = {}
    total_checks = 0
    total_failures = 0
    by_severity_counts = {"Fail": 0, "Warn": 0, "Note": 0, "pass": 0}

    for vr, pr in results:
        tables_dict[vr.table_key] = {
            "validation": serialise_validation(vr, max_failing_rows),
            "profiling": serialise_profiling(pr),
        }

        # Accumulate summary counts
        total_checks += len(vr.steps)
        total_failures += vr.total_failures

        # Count by severity
        for step in vr.steps:
            if step.n_failed > 0:
                severity = step.severity or "pass"
                if severity in by_severity_counts:
                    by_severity_counts[severity] += 1
            else:
                by_severity_counts["pass"] += 1

    return {
        "schema_version": "1.0",
        "generated_at": datetime.now(UTC).isoformat(),
        "tables": tables_dict,
        "summary": {
            "total_checks": total_checks,
            "total_failures": total_failures,
            "by_severity": by_severity_counts,
        },
    }

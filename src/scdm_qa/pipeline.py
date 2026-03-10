from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import structlog

from scdm_qa.config import QAConfig
from scdm_qa.profiling.accumulator import ProfilingAccumulator
from scdm_qa.profiling.results import ProfilingResult
from scdm_qa.readers import create_reader
from scdm_qa.reporting.builder import save_table_report
from scdm_qa.reporting.index import ReportSummary, make_report_summary, save_index
from scdm_qa.schemas import get_schema
from scdm_qa.schemas.checks import get_date_ordering_checks_for_table, get_not_populated_checks_for_table
from scdm_qa.schemas.custom_rules import load_custom_rules
from scdm_qa.validation.global_checks import (
    check_cause_of_death,
    check_date_ordering,
    check_enc_combinations,
    check_enrollment_gaps,
    check_not_populated,
    check_overlapping_spans,
    check_sort_order,
    check_uniqueness,
)
from scdm_qa.validation.results import StepResult, ValidationResult
from scdm_qa.validation.runner import run_validation

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class TableOutcome:
    table_key: str
    success: bool
    validation_result: ValidationResult | None = None
    profiling_result: ProfilingResult | None = None
    error: str | None = None


def run_pipeline(
    config: QAConfig,
    *,
    table_filter: str | None = None,
    profile_only: bool = False,
) -> list[TableOutcome]:
    tables = config.tables
    if table_filter:
        if table_filter not in tables:
            log.error("table not found in config", table=table_filter, available=list(tables.keys()))
            return [TableOutcome(table_key=table_filter, success=False, error=f"table {table_filter!r} not in config")]
        tables = {table_filter: tables[table_filter]}

    outcomes: list[TableOutcome] = []
    report_summaries: list[ReportSummary] = []

    for table_key, file_path in tables.items():
        log.info("processing table", table=table_key, file=str(file_path))
        try:
            outcome = _process_table(
                table_key,
                file_path,
                config,
                profile_only=profile_only,
            )
            outcomes.append(outcome)

            if outcome.validation_result and outcome.profiling_result:
                save_table_report(
                    config.output_dir,
                    table_key,
                    outcome.validation_result,
                    outcome.profiling_result,
                )
                report_summaries.append(
                    make_report_summary(
                        table_key,
                        outcome.validation_result.table_name,
                        outcome.validation_result.total_rows,
                        len(outcome.validation_result.steps),
                        outcome.validation_result.total_failures,
                    )
                )
            elif outcome.profiling_result:
                # Profile-only mode: create a report with just profiling data
                empty_vr = ValidationResult(
                    table_key=table_key,
                    table_name=outcome.profiling_result.table_name,
                    steps=(),
                    total_rows=outcome.profiling_result.total_rows,
                    chunks_processed=0,
                )
                save_table_report(
                    config.output_dir,
                    table_key,
                    empty_vr,
                    outcome.profiling_result,
                )
                report_summaries.append(
                    make_report_summary(
                        table_key,
                        outcome.profiling_result.table_name,
                        outcome.profiling_result.total_rows,
                        0,
                        0,
                    )
                )

        except Exception as exc:
            log.error("table processing failed", table=table_key, error=str(exc))
            outcomes.append(TableOutcome(table_key=table_key, success=False, error=str(exc)))

    if report_summaries:
        save_index(config.output_dir, report_summaries)

    return outcomes


def _process_table(
    table_key: str,
    file_path: Path,
    config: QAConfig,
    *,
    profile_only: bool = False,
) -> TableOutcome:
    if not file_path.exists():
        return TableOutcome(table_key=table_key, success=False, error=f"file not found: {file_path}")

    schema = get_schema(table_key)
    reader = create_reader(file_path, chunk_size=config.chunk_size)
    custom_extend_fn = load_custom_rules(table_key, config.custom_rules_dir)

    profiling_acc = ProfilingAccumulator(schema)

    if profile_only:
        for chunk in reader.chunks():
            profiling_acc.add_chunk(chunk)
        return TableOutcome(
            table_key=table_key,
            success=True,
            profiling_result=profiling_acc.result(),
        )

    # Single-pass: profiling accumulator runs inside validation runner
    validation_result = run_validation(
        reader,
        schema,
        max_failing_rows=config.max_failing_rows,
        profiling_accumulator=profiling_acc,
        custom_extend_fn=custom_extend_fn,
    )

    profiling_result = profiling_acc.result()

    # Global checks (uniqueness + sort order)
    global_steps: list[StepResult] = []

    if schema.unique_row:
        uniqueness_reader = create_reader(file_path, chunk_size=config.chunk_size)
        uniqueness_step = check_uniqueness(
            file_path,
            schema,
            chunks=uniqueness_reader.chunks(),
            max_failing_rows=config.max_failing_rows,
        )
        if uniqueness_step is not None:
            global_steps.append(uniqueness_step)

    if schema.sort_order:
        # NOTE: This requires a second scan. Could be optimised by collecting
        # chunk boundary rows during the validation pass.
        sort_reader = create_reader(file_path, chunk_size=config.chunk_size)
        sort_step = check_sort_order(schema, sort_reader.chunks())
        if sort_step is not None:
            global_steps.append(sort_step)

    if get_not_populated_checks_for_table(schema.table_key):
        # L1 global check: not populated (check 111)
        not_pop_reader = create_reader(file_path, chunk_size=config.chunk_size)
        not_pop_steps = check_not_populated(schema, not_pop_reader.chunks())
        global_steps.extend(not_pop_steps)

    # L2 check: date ordering (check 226)
    if get_date_ordering_checks_for_table(schema.table_key):
        date_order_reader = create_reader(file_path, chunk_size=config.chunk_size)
        date_order_steps = check_date_ordering(schema, date_order_reader.chunks(), max_failing_rows=config.max_failing_rows)
        global_steps.extend(date_order_steps)

    # L2 check: cause of death (checks 236, 237)
    if schema.table_key == "cause_of_death":
        cod_reader = create_reader(file_path, chunk_size=config.chunk_size)
        cod_steps = check_cause_of_death(schema, cod_reader.chunks(), max_failing_rows=config.max_failing_rows)
        global_steps.extend(cod_steps)

    # L2 checks: enrollment overlaps and gaps (checks 215, 216)
    if schema.table_key == "enrollment":
        overlap_reader = create_reader(file_path, chunk_size=config.chunk_size)
        overlap_step = check_overlapping_spans(
            file_path, schema, overlap_reader.chunks(),
            max_failing_rows=config.max_failing_rows,
        )
        if overlap_step is not None:
            global_steps.append(overlap_step)

        gaps_reader = create_reader(file_path, chunk_size=config.chunk_size)
        gaps_step = check_enrollment_gaps(
            schema, gaps_reader.chunks(),
            max_failing_rows=config.max_failing_rows,
        )
        if gaps_step is not None:
            global_steps.append(gaps_step)

    # L2 checks: ENC field combinations (checks 244, 245)
    if schema.table_key == "encounter":
        enc_combo_reader = create_reader(file_path, chunk_size=config.chunk_size)
        enc_combo_steps = check_enc_combinations(
            schema, enc_combo_reader.chunks(),
            max_failing_rows=config.max_failing_rows,
        )
        global_steps.extend(enc_combo_steps)

    if global_steps:
        all_steps = list(validation_result.steps) + global_steps
        validation_result = ValidationResult(
            table_key=validation_result.table_key,
            table_name=validation_result.table_name,
            steps=tuple(all_steps),
            total_rows=validation_result.total_rows,
            chunks_processed=validation_result.chunks_processed,
        )

    return TableOutcome(
        table_key=table_key,
        success=True,
        validation_result=validation_result,
        profiling_result=profiling_result,
    )


def compute_exit_code(
    outcomes: list[TableOutcome],
    *,
    error_threshold: float = 0.05,
) -> int:
    """Compute CLI exit code from pipeline outcomes.

    Severity-aware:
        - Note: informational only, never escalates exit code
        - Warn: failures contribute to exit code 1
        - Fail (or None): failures contribute to exit code 1; threshold exceedance → 2

    Returns:
        0: all checks pass (no failures in non-Note checks)
        1: some failures exist but all within threshold (warnings)
        2: processing errors or at least one Fail/None step exceeds error threshold
    """
    has_errors = any(not o.success for o in outcomes)
    if has_errors:
        return 2

    has_failures = False
    has_threshold_exceedance = False

    for o in outcomes:
        if o.validation_result is None:
            continue
        for step in o.validation_result.steps:
            # Note-severity checks are informational — skip for exit code
            if step.severity == "Note":
                continue
            if step.n_failed > 0:
                has_failures = True
                if step.f_failed > error_threshold:
                    has_threshold_exceedance = True

    if has_threshold_exceedance:
        return 2
    if has_failures:
        return 1
    return 0

from __future__ import annotations

from typing import TYPE_CHECKING

import pointblank as pb
import polars as pl
import structlog

from scdm_qa.readers.base import TableReader
from scdm_qa.schemas.custom_rules import ExtendFn
from scdm_qa.schemas.models import TableSchema
from scdm_qa.schemas.validation import build_validation
from scdm_qa.validation.accumulator import ValidationAccumulator
from scdm_qa.validation.results import ValidationResult

if TYPE_CHECKING:
    from scdm_qa.profiling.accumulator import ProfilingAccumulator

log = structlog.get_logger(__name__)


def run_validation(
    reader: TableReader,
    schema: TableSchema,
    *,
    thresholds: pb.Thresholds | None = None,
    max_failing_rows: int = 500,
    profiling_accumulator: ProfilingAccumulator | None = None,
    custom_extend_fn: ExtendFn | None = None,
) -> ValidationResult:
    accumulator = ValidationAccumulator(
        table_key=schema.table_key,
        table_name=schema.table_name,
        max_failing_rows=max_failing_rows,
    )

    step_descriptions: list[tuple[int, str, str, str]] = []

    for chunk_num, chunk in enumerate(reader.chunks(), start=1):
        if profiling_accumulator is not None:
            profiling_accumulator.add_chunk(chunk)

        log.info(
            "validating chunk",
            table=schema.table_key,
            chunk=chunk_num,
            rows=chunk.height,
        )

        if chunk_num == 1:
            step_descriptions = _build_step_descriptions(schema, set(chunk.columns))

        validation = build_validation(chunk, schema, thresholds=thresholds)
        if custom_extend_fn is not None:
            from scdm_qa.schemas.custom_rules import apply_custom_rules
            validation = apply_custom_rules(validation, chunk, custom_extend_fn)
        result = validation.interrogate(
            collect_extracts=True,
            extract_limit=max_failing_rows,
        )

        n_passed = result.n_passed()
        n_failed = result.n_failed()

        # CRITICAL: Verify step_descriptions length matches pointblank results after first chunk
        # to catch silent drift between _build_step_descriptions and build_validation.
        if chunk_num == 1:
            num_steps_in_descriptions = len(step_descriptions)
            num_steps_in_results = len(n_passed)
            if num_steps_in_descriptions != num_steps_in_results:
                raise ValueError(
                    f"Step count mismatch for table '{schema.table_key}': "
                    f"_build_step_descriptions generated {num_steps_in_descriptions} steps, "
                    f"but build_validation produced {num_steps_in_results} steps in pointblank results. "
                    f"This indicates the two code paths have drifted. Both must iterate columns and rules in the same order."
                )

        extracts: dict[int, pl.DataFrame] = {}
        for step_idx in n_failed:
            if n_failed[step_idx] > 0:
                extract = result.get_data_extracts(i=step_idx, frame=True)
                if extract is not None and hasattr(extract, "height") and extract.height > 0:
                    extracts[step_idx] = extract

        accumulator.add_chunk_results(
            chunk_row_count=chunk.height,
            step_descriptions=step_descriptions,
            n_passed=n_passed,
            n_failed=n_failed,
            extracts=extracts,
        )

    final = accumulator.result()
    if final.chunks_processed == 0:
        log.warning(
            "validation found no chunks to process",
            table=schema.table_key,
        )
    log.info(
        "validation complete",
        table=schema.table_key,
        total_rows=final.total_rows,
        chunks=final.chunks_processed,
        total_failures=final.total_failures,
    )
    return final


def _build_step_descriptions(
    schema: TableSchema,
    present_columns: set[str],
) -> list[tuple[int, str, str, str]]:
    """Build step descriptions matching the order of steps in build_validation().

    Returns list of (step_index, assertion_type, column, description).
    Step indices are 1-based to match pointblank's convention.
    """
    descriptions: list[tuple[int, str, str, str]] = []
    step_idx = 0

    for col in schema.columns:
        if col.name not in present_columns:
            continue
        if not col.missing_allowed:
            step_idx += 1
            descriptions.append((step_idx, "col_vals_not_null", col.name, f"{col.name} not null"))
        if col.allowed_values is not None:
            step_idx += 1
            descriptions.append((step_idx, "col_vals_in_set", col.name, f"{col.name} in allowed values"))
        if col.col_type == "Character" and col.length is not None:
            step_idx += 1
            descriptions.append((step_idx, "col_vals_regex", col.name, f"{col.name} length <= {col.length}"))

    for rule in schema.conditional_rules:
        if rule.target_column not in present_columns:
            continue
        if rule.condition_column not in present_columns:
            continue
        if not rule.condition_values:
            continue
        step_idx += 1
        descriptions.append((
            step_idx,
            "col_vals_not_null (conditional)",
            rule.target_column,
            f"{rule.target_column} not null when {rule.condition_column} in {sorted(rule.condition_values)}",
        ))

    return descriptions

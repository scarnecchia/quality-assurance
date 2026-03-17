from __future__ import annotations

from typing import TYPE_CHECKING

import pointblank as pb
import polars as pl
import structlog

from scdm_qa.readers.base import TableReader
from scdm_qa.schemas.checks import get_per_chunk_checks_for_table
from scdm_qa.schemas.code_checks import get_format_checks_for_table, get_length_checks_for_table
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

    step_descriptions: list[tuple[int, str, str, str, str | None, str | None]] = []

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
            step_descriptions = build_step_descriptions(schema, set(chunk.columns))

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
        # to catch silent drift between build_step_descriptions and build_validation.
        if chunk_num == 1:
            num_steps_in_descriptions = len(step_descriptions)
            num_steps_in_results = len(n_passed)
            if num_steps_in_descriptions != num_steps_in_results:
                raise ValueError(
                    f"Step count mismatch for table '{schema.table_key}': "
                    f"build_step_descriptions generated {num_steps_in_descriptions} steps, "
                    f"but build_validation produced {num_steps_in_results} steps in pointblank results. "
                    f"This indicates the two code paths have drifted. Both must iterate columns and rules in the same order."
                )

        extracts: dict[int, pl.DataFrame] = {}
        for step_idx in n_failed:
            failed_count = n_failed[step_idx]
            if failed_count is not None and failed_count > 0:
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


def build_step_descriptions(
    schema: TableSchema,
    present_columns: set[str],
) -> list[tuple[int, str, str, str, str | None, str | None]]:
    """Build step descriptions matching the order of steps in build_validation().

    Returns list of (step_index, assertion_type, column, description, check_id, severity).
    Step indices are 1-based to match pointblank's convention.
    """
    descriptions: list[tuple[int, str, str, str, str | None, str | None]] = []
    step_idx = 0

    for col in schema.columns:
        if col.name not in present_columns:
            continue
        if not col.missing_allowed:
            step_idx += 1
            descriptions.append((step_idx, "col_vals_not_null", col.name, f"{col.name} variable contains a null value", "120", None))
        if col.allowed_values is not None:
            step_idx += 1
            descriptions.append((step_idx, "col_vals_in_set", col.name, f"{col.name} variable contains an incorrect non-missing value", "121", None))
        if col.col_type == "Character" and col.length is not None:
            step_idx += 1
            descriptions.append((step_idx, "col_vals_regex", col.name, f"{col.name} variable length does not conform to specifications", "113", "Fail"))

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
            f"{rule.target_column} variable contains a null value when {rule.condition_column} in {sorted(rule.condition_values)}",
            "120",
            None,
        ))

    # L1 per-chunk checks (122, 124, 128) — must mirror build_validation() order
    for check_def in get_per_chunk_checks_for_table(schema.table_key):
        if check_def.column not in present_columns:
            continue

        if check_def.check_type == "leading_spaces":
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_regex",
                check_def.column,
                f"{check_def.column} no leading spaces (check {check_def.check_id})",
                check_def.check_id,
                check_def.severity,
            ))
        elif check_def.check_type == "unexpected_zeros":
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_gt",
                check_def.column,
                f"{check_def.column} not zero (check {check_def.check_id})",
                check_def.check_id,
                check_def.severity,
            ))
        elif check_def.check_type == "non_numeric":
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_regex",
                check_def.column,
                f"{check_def.column} numeric only (check {check_def.check_id})",
                check_def.check_id,
                check_def.severity,
            ))

    # Code format checks (223) — must mirror build_validation() order
    for fmt_check in get_format_checks_for_table(schema.table_key):
        if fmt_check.column not in present_columns:
            continue
        if fmt_check.codetype_column not in present_columns:
            continue

        if fmt_check.check_subtype == "no_decimal":
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_regex",
                fmt_check.column,
                f"{fmt_check.column} value is incorrect based on {fmt_check.codetype_column} value",
                fmt_check.check_id,
                fmt_check.severity,
            ))
        elif fmt_check.check_subtype == "regex":
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_regex",
                fmt_check.column,
                f"{fmt_check.column} value is incorrect based on {fmt_check.codetype_column} value",
                fmt_check.check_id,
                fmt_check.severity,
            ))
        elif fmt_check.check_subtype == "era_date":
            if fmt_check.date_column not in present_columns:
                continue
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_null",
                fmt_check.column,
                f"{fmt_check.column} value is incorrect based on {fmt_check.date_column} value",
                fmt_check.check_id,
                fmt_check.severity,
            ))
        elif fmt_check.check_subtype == "conditional_presence":
            if fmt_check.condition_column not in present_columns:
                continue
            step_idx += 1
            descriptions.append((
                step_idx,
                "col_vals_null" if fmt_check.expect_null else "col_vals_not_null",
                fmt_check.column,
                f"{fmt_check.column} value is incorrect based on {fmt_check.condition_column} value",
                fmt_check.check_id,
                fmt_check.severity,
            ))

    # Code length checks (228) — must mirror build_validation() order
    for len_check in get_length_checks_for_table(schema.table_key):
        if len_check.column not in present_columns:
            continue
        if len_check.codetype_column not in present_columns:
            continue

        step_idx += 1
        descriptions.append((
            step_idx,
            "col_vals_regex",
            len_check.column,
            f"{len_check.column} value length is incorrect based on {len_check.codetype_column}",
            len_check.check_id,
            len_check.severity,
        ))

    return descriptions

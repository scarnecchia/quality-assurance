from __future__ import annotations

import pointblank as pb
import polars as pl

from scdm_qa.schemas.checks import get_per_chunk_checks_for_table
from scdm_qa.schemas.code_checks import get_format_checks_for_table, get_length_checks_for_table
from scdm_qa.schemas.models import ColumnDef, ConditionalRule, TableSchema


def build_validation(
    data: pl.DataFrame,
    schema: TableSchema,
    *,
    thresholds: pb.Thresholds | None = None,
) -> pb.Validate:
    if thresholds is None:
        thresholds = pb.Thresholds(warning=0.01, error=0.05)

    validation = pb.Validate(
        data=data,
        tbl_name=schema.table_key,
        label=f"SCDM Validation: {schema.table_name}",
        thresholds=thresholds,
    )

    present_columns = set(data.columns)

    for col in schema.columns:
        if col.name not in present_columns:
            continue

        if not col.missing_allowed:
            validation = validation.col_vals_not_null(columns=col.name)

        if col.allowed_values is not None:
            if col.missing_allowed:
                validation = validation.col_vals_in_set(
                    columns=col.name,
                    set=col.allowed_values,
                    pre=lambda df, col_name=col.name: df.filter(pl.col(col_name).is_not_null()),
                )
            else:
                validation = validation.col_vals_in_set(
                    columns=col.name,
                    set=col.allowed_values,
                )

        if col.col_type == "Character" and col.length is not None:
            pattern = f"^.{{0,{col.length}}}$"
            validation = validation.col_vals_regex(
                columns=col.name,
                pattern=pattern,
                na_pass=True,
            )

    for rule in schema.conditional_rules:
        if rule.target_column not in present_columns:
            continue
        if rule.condition_column not in present_columns:
            continue
        if not rule.condition_values:
            continue

        condition_values_list = sorted(rule.condition_values)
        validation = validation.col_vals_not_null(
            columns=rule.target_column,
            pre=lambda df, cv=condition_values_list, cc=rule.condition_column: df.filter(
                pl.col(cc).is_in(cv)
            ),
        )

    # L1 per-chunk checks (122, 124, 128)
    for check_def in get_per_chunk_checks_for_table(schema.table_key):
        if check_def.column not in present_columns:
            continue

        if check_def.check_type == "leading_spaces":
            # Check 122: Flag values with leading whitespace
            # Pattern matches: non-space first char OR empty string
            validation = validation.col_vals_regex(
                columns=check_def.column,
                pattern=r"^[^ ]|^$",
                na_pass=True,
            )
        elif check_def.check_type == "unexpected_zeros":
            # Check 124: Flag numeric columns containing zero
            validation = validation.col_vals_gt(
                columns=check_def.column,
                value=0,
                na_pass=True,
            )
        elif check_def.check_type == "non_numeric":
            # Check 128: Flag non-numeric characters
            validation = validation.col_vals_regex(
                columns=check_def.column,
                pattern=r"^[0-9]*$",
                na_pass=True,
            )

    # Code format checks (223)
    for fmt_check in get_format_checks_for_table(schema.table_key):
        if fmt_check.column not in present_columns:
            continue
        if fmt_check.codetype_column not in present_columns:
            continue

        # pre= filter: only rows where codetype matches AND codetype is not null
        codetype_pre = lambda df, ct_col=fmt_check.codetype_column, ct_val=fmt_check.codetype_value: (
            df.filter(pl.col(ct_col).is_not_null() & (pl.col(ct_col) == ct_val))
        )

        if fmt_check.check_subtype == "no_decimal":
            # Code must not contain a period
            validation = validation.col_vals_regex(
                columns=fmt_check.column,
                pattern=r"^[^.]*$",
                na_pass=True,
                pre=codetype_pre,
            )

        elif fmt_check.check_subtype == "regex":
            # Code must match the specified pattern
            validation = validation.col_vals_regex(
                columns=fmt_check.column,
                pattern=fmt_check.pattern,
                na_pass=True,
                pre=codetype_pre,
            )

        elif fmt_check.check_subtype == "era_date":
            # Era date check: filter to "bad" rows (wrong codetype for the era),
            # then assert code column is null in that set. Any non-null rows fail.
            #
            # For ICD-9 (codetype "09"): rows with ADate >= 2015-10-01 are violations
            # For ICD-10 (codetype "10"): rows with ADate < 2015-10-01 are violations
            #
            # The date_column and era_boundary are on the FormatCheckDef.
            # The codetype_value determines the direction:
            #   "09" → violations are date >= boundary
            #   "10" → violations are date < boundary
            if fmt_check.date_column not in present_columns:
                continue

            # Parse boundary date into YYYYMMDD format for comparison with numeric dates
            boundary_str = fmt_check.era_boundary.replace("-", "")  # "2015-10-01" -> "20151001"
            boundary_num = int(boundary_str)

            if fmt_check.codetype_value == "09":
                # ICD-9 after transition date is a violation
                era_pre = lambda df, ct_col=fmt_check.codetype_column, ct_val=fmt_check.codetype_value, d_col=fmt_check.date_column, boundary=boundary_num: (
                    df.filter(
                        pl.col(ct_col).is_not_null()
                        & (pl.col(ct_col) == ct_val)
                        & (pl.col(d_col) >= boundary)
                    )
                )
            else:
                # ICD-10 before transition date is a violation
                era_pre = lambda df, ct_col=fmt_check.codetype_column, ct_val=fmt_check.codetype_value, d_col=fmt_check.date_column, boundary=boundary_num: (
                    df.filter(
                        pl.col(ct_col).is_not_null()
                        & (pl.col(ct_col) == ct_val)
                        & (pl.col(d_col) < boundary)
                    )
                )

            # Assert that code column is null in the violation set.
            # Any non-null codes in these rows are violations.
            validation = validation.col_vals_null(
                columns=fmt_check.column,
                pre=era_pre,
            )

        elif fmt_check.check_subtype == "conditional_presence":
            # Conditional presence: filter by condition_column values,
            # then assert target column is null or not-null based on expect_null.
            # Fields condition_column, condition_values, expect_null are on FormatCheckDef.
            if fmt_check.condition_column not in present_columns:
                continue

            cond_pre = lambda df, cc=fmt_check.condition_column, cv=list(fmt_check.condition_values): (
                df.filter(pl.col(cc).is_in(cv))
            )
            if fmt_check.expect_null:
                validation = validation.col_vals_null(
                    columns=fmt_check.column,
                    pre=cond_pre,
                )
            else:
                validation = validation.col_vals_not_null(
                    columns=fmt_check.column,
                    pre=cond_pre,
                )

    # Code length checks (228)
    for len_check in get_length_checks_for_table(schema.table_key):
        if len_check.column not in present_columns:
            continue
        if len_check.codetype_column not in present_columns:
            continue

        codetype_pre = lambda df, ct_col=len_check.codetype_column, ct_val=len_check.codetype_value: (
            df.filter(pl.col(ct_col).is_not_null() & (pl.col(ct_col) == ct_val))
        )

        # Code length must be between min_length and max_length (inclusive)
        # Use regex: ^.{min,max}$
        length_pattern = f"^.{{{len_check.min_length},{len_check.max_length}}}$"
        validation = validation.col_vals_regex(
            columns=len_check.column,
            pattern=length_pattern,
            na_pass=True,
            pre=codetype_pre,
        )

    return validation

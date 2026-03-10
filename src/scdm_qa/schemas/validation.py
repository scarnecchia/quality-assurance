from __future__ import annotations

import pointblank as pb
import polars as pl

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

    return validation

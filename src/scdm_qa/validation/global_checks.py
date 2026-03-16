from __future__ import annotations

from typing import TypedDict

import duckdb
import polars as pl
import structlog

from scdm_qa.schemas.checks import (
    ENC_COMBINATION_RULES,
    ENC_RATE_THRESHOLDS,
    get_date_ordering_checks_for_table,
    get_not_populated_checks_for_table,
)
from scdm_qa.schemas.models import TableSchema
from scdm_qa.validation.results import StepResult

log = structlog.get_logger(__name__)

# Maps internal table_key to SAS short table ID used in flag descriptions.
_TABLE_KEY_TO_SAS_ID: dict[str, str] = {
    "cause_of_death": "COD",
    "death": "DTH",
    "demographic": "DEM",
    "diagnosis": "DIA",
    "dispensing": "DIS",
    "encounter": "ENC",
    "enrollment": "ENR",
    "facility": "FAC",
    "inpatient_pharmacy": "IRX",
    "laboratory": "LAB",
    "prescribing": "PRE",
    "procedure": "PRO",
    "provider": "PVD",
    "vital_signs": "VIT",
    "patient_reported_response": "PRR",
    "patient_reported_survey": "PRS",
    "inpatient_transfusion": "TXN",
}


class SortViolation(TypedDict):
    chunk_boundary: str
    issue: str


def check_uniqueness(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> StepResult | None:
    if not schema.unique_row:
        return None

    key_cols = list(schema.unique_row)
    description = f"Duplicate record(s) present for unique key variable(s): {', '.join(key_cols)}"
    cols_sql = ", ".join(f'"{c}"' for c in key_cols)
    safe_view = view_name.replace('"', '""')

    try:
        total_rows = conn.execute(
            f'SELECT COUNT(*) FROM "{safe_view}"'
        ).fetchone()[0]

        dup_row_total = conn.execute(f"""
            SELECT COALESCE(SUM(_dup_count), 0) FROM (
                SELECT COUNT(*) AS _dup_count
                FROM "{safe_view}"
                GROUP BY {cols_sql}
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        failing_df = conn.execute(f"""
            SELECT {cols_sql}, COUNT(*) AS _dup_count
            FROM "{safe_view}"
            GROUP BY {cols_sql}
            HAVING COUNT(*) > 1
            LIMIT {max_failing_rows}
        """).pl()
    except duckdb.Error as e:
        log.error("uniqueness check failed", error=str(e), view=view_name)
        return StepResult(
            step_index=-1,
            assertion_type="rows_distinct",
            column=", ".join(key_cols),
            description=f"Uniqueness check error: {e}",
            n_passed=0,
            n_failed=0,
            failing_rows=None,
            check_id="211",
            severity="Fail",
        )

    n_failed = dup_row_total
    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    log.info(
        "uniqueness check via duckdb",
        key_cols=key_cols,
        total_rows=total_rows,
        duplicate_rows=dup_row_total,
    )

    return StepResult(
        step_index=-1,
        assertion_type="rows_distinct",
        column=", ".join(key_cols),
        description=description,
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=failing_df if failing_df.height > 0 else None,
        check_id="211",
        severity="Fail",
    )


def check_sort_order(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
) -> StepResult | None:
    if not schema.sort_order:
        return None

    sort_cols = list(schema.sort_order)
    sas_id = _TABLE_KEY_TO_SAS_ID.get(schema.table_key, schema.table_key.upper())
    description = f"{sas_id} table is not sorted by the following variables: {', '.join(sort_cols)}"
    safe_view = view_name.replace('"', '""')

    try:
        total_rows = conn.execute(
            f'SELECT COUNT(*) FROM "{safe_view}"'
        ).fetchone()[0]

        # Build LAG-based comparison for each sort column.
        # A row is a violation if any column is strictly less than the
        # previous row's value AND all higher-priority columns are equal.
        # This mirrors multi-column sort comparison.
        lag_cols = []
        for col in sort_cols:
            safe_col = col.replace('"', '""')
            lag_cols.append(
                f'LAG("{safe_col}") OVER () AS "_prev_{safe_col}"'
            )

        lag_select = ", ".join(lag_cols)

        # Build violation condition: for multi-column sort, a violation
        # occurs when, scanning left to right, the first column that
        # differs has decreased.
        conditions = []
        for i, col in enumerate(sort_cols):
            safe_col = col.replace('"', '""')
            # All prior columns are equal
            equal_prefix = " AND ".join(
                f'"{sort_cols[j].replace(chr(34), chr(34)+chr(34))}" = "_prev_{sort_cols[j].replace(chr(34), chr(34)+chr(34))}"'
                for j in range(i)
            )
            cond = f'"{safe_col}" < "_prev_{safe_col}"'
            if equal_prefix:
                cond = f"({equal_prefix} AND {cond})"
            conditions.append(cond)

        violation_where = " OR ".join(f"({c})" for c in conditions)

        n_failed = conn.execute(f"""
            WITH lagged AS (
                SELECT {lag_select},
                       {", ".join(f'"{c}"' for c in sort_cols)}
                FROM "{safe_view}"
            )
            SELECT COUNT(*) FROM lagged
            WHERE {violation_where}
        """).fetchone()[0] or 0

    except duckdb.Error as e:
        log.error("sort order check failed", error=str(e), view=view_name)
        return StepResult(
            step_index=-1,
            assertion_type="sort_order",
            column=", ".join(sort_cols),
            description=f"Sort order check error: {e}",
            n_passed=0,
            n_failed=0,
            failing_rows=None,
            check_id="102",
            severity="Fail",
        )

    # n_passed = rows that are not violations (total - failed).
    # First row can never be a violation (no predecessor).
    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    return StepResult(
        step_index=-1,
        assertion_type="sort_order",
        column=", ".join(sort_cols),
        description=description,
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=None,
        check_id="102",
        severity="Fail",
    )


def check_not_populated(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
) -> list[StepResult]:
    check_defs = get_not_populated_checks_for_table(schema.table_key)
    if not check_defs:
        return []

    safe_view = view_name.replace('"', '""')

    try:
        total_rows = conn.execute(
            f'SELECT COUNT(*) FROM "{safe_view}"'
        ).fetchone()[0]
    except duckdb.Error as e:
        log.error("not populated check failed", error=str(e), view=view_name)
        return []

    results: list[StepResult] = []
    for check_def in check_defs:
        safe_col = check_def.column.replace('"', '""')
        try:
            non_null_count = conn.execute(
                f'SELECT COUNT("{safe_col}") FROM "{safe_view}"'
            ).fetchone()[0]
        except duckdb.Error:
            non_null_count = 0

        if non_null_count == 0:
            n_failed = total_rows
            n_passed = 0
        else:
            n_failed = 0
            n_passed = total_rows

        results.append(
            StepResult(
                step_index=-1,
                assertion_type="not_populated",
                column=check_def.column,
                description=f"{check_def.column} populated (check {check_def.check_id})",
                n_passed=n_passed,
                n_failed=n_failed,
                failing_rows=None,
                check_id=check_def.check_id,
                severity=check_def.severity,
            )
        )

    return results


def check_date_ordering(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    """Check 226: Detect rows where date_a > date_b using DuckDB SQL.

    Rows where either date is null are skipped (not flagged).
    Returns one StepResult per configured date pair.
    """
    ordering_defs = get_date_ordering_checks_for_table(schema.table_key)
    if not ordering_defs:
        return []

    safe_view = view_name.replace('"', '""')
    results: list[StepResult] = []

    for pair_def in ordering_defs:
        safe_a = pair_def.date_a.replace('"', '""')
        safe_b = pair_def.date_b.replace('"', '""')

        try:
            # Count rows where both dates are non-null
            both_present = conn.execute(f"""
                SELECT COUNT(*) FROM "{safe_view}"
                WHERE "{safe_a}" IS NOT NULL AND "{safe_b}" IS NOT NULL
            """).fetchone()[0]

            # Count violations: date_a > date_b (both non-null)
            n_failed = conn.execute(f"""
                SELECT COUNT(*) FROM "{safe_view}"
                WHERE "{safe_a}" IS NOT NULL
                  AND "{safe_b}" IS NOT NULL
                  AND "{safe_a}" > "{safe_b}"
            """).fetchone()[0] or 0

            n_passed = both_present - n_failed

            # Sample failing rows
            failing_df = conn.execute(f"""
                SELECT * FROM "{safe_view}"
                WHERE "{safe_a}" IS NOT NULL
                  AND "{safe_b}" IS NOT NULL
                  AND "{safe_a}" > "{safe_b}"
                LIMIT {max_failing_rows}
            """).pl()

            failing = failing_df if failing_df.height > 0 else None

        except duckdb.Error as e:
            log.error(
                "date ordering check failed",
                error=str(e),
                view=view_name,
                date_a=pair_def.date_a,
                date_b=pair_def.date_b,
            )
            n_passed = 0
            n_failed = 0
            failing = None

        results.append(
            StepResult(
                step_index=-1,
                assertion_type="date_ordering",
                column=f"{pair_def.date_a}, {pair_def.date_b}",
                description=f"{pair_def.description} (check {pair_def.check_id})",
                n_passed=n_passed,
                n_failed=n_failed,
                failing_rows=failing,
                check_id=pair_def.check_id,
                severity=pair_def.severity,
            )
        )

    return results


def check_cause_of_death(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    """Checks 236 and 237: Validate underlying cause of death records using DuckDB SQL.

    236: Each patient in COD must have at least one CauseType='U' record.
    237: Each patient in COD must have at most one CauseType='U' record.

    Returns two StepResults (236 first, then 237).
    """
    if schema.table_key != "cause_of_death":
        return []

    safe_view = view_name.replace('"', '""')

    try:
        total_patients = conn.execute(f"""
            SELECT COUNT(DISTINCT "PatID") FROM "{safe_view}"
        """).fetchone()[0]

        # Check 236: patients with zero CauseType='U'
        missing_u_count = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT "PatID"
                FROM "{safe_view}"
                GROUP BY "PatID"
                HAVING SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) = 0
            )
        """).fetchone()[0] or 0

        failing_236 = conn.execute(f"""
            SELECT "PatID", 0 AS u_count
            FROM "{safe_view}"
            GROUP BY "PatID"
            HAVING SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) = 0
            LIMIT {max_failing_rows}
        """).pl()

        # Check 237: patients with more than one CauseType='U'
        multiple_u_count = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT "PatID"
                FROM "{safe_view}"
                GROUP BY "PatID"
                HAVING SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) > 1
            )
        """).fetchone()[0] or 0

        failing_237 = conn.execute(f"""
            SELECT "PatID",
                   SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) AS u_count
            FROM "{safe_view}"
            GROUP BY "PatID"
            HAVING SUM(CASE WHEN "CauseType" = 'U' THEN 1 ELSE 0 END) > 1
            LIMIT {max_failing_rows}
        """).pl()

    except duckdb.Error as e:
        log.error("cause of death check failed", error=str(e), view=view_name)
        return [
            StepResult(
                step_index=-1,
                assertion_type="cause_of_death",
                column="CauseType",
                description=f"Cause of death check error: {e}",
                n_passed=0, n_failed=0, failing_rows=None,
                check_id="236", severity="Fail",
            ),
            StepResult(
                step_index=-1,
                assertion_type="cause_of_death",
                column="CauseType",
                description=f"Cause of death check error: {e}",
                n_passed=0, n_failed=0, failing_rows=None,
                check_id="237", severity="Fail",
            ),
        ]

    return [
        StepResult(
            step_index=-1,
            assertion_type="cause_of_death",
            column="CauseType",
            description="Each patient has underlying cause of death (check 236)",
            n_passed=total_patients - missing_u_count,
            n_failed=missing_u_count,
            failing_rows=failing_236 if failing_236.height > 0 else None,
            check_id="236",
            severity="Fail",
        ),
        StepResult(
            step_index=-1,
            assertion_type="cause_of_death",
            column="CauseType",
            description="Each patient has at most one underlying cause of death (check 237)",
            n_passed=total_patients - multiple_u_count,
            n_failed=multiple_u_count,
            failing_rows=failing_237 if failing_237.height > 0 else None,
            check_id="237",
            severity="Fail",
        ),
    ]


def check_overlapping_spans(
    conn: duckdb.DuckDBPyConnection,
    view_name: str,
    schema: TableSchema,
    *,
    max_failing_rows: int = 500,
) -> StepResult | None:
    """Check 215: Detect overlapping enrollment spans within the same patient.

    For each patient, sorts spans by Enr_Start and checks if any span's
    Enr_Start is strictly less than the previous span's Enr_End.
    """
    if schema.table_key != "enrollment":
        return None

    safe_view = view_name.replace('"', '""')

    try:
        total_rows = conn.execute(
            f'SELECT COUNT(*) FROM "{safe_view}"'
        ).fetchone()[0]

        n_failed = conn.execute(f"""
            WITH spans AS (
                SELECT "PatID", "Enr_Start", "Enr_End",
                       LAG("Enr_End") OVER (
                           PARTITION BY "PatID" ORDER BY "Enr_Start"
                       ) AS prev_end
                FROM "{safe_view}"
            )
            SELECT COUNT(*) FROM spans WHERE "Enr_Start" < prev_end
        """).fetchone()[0] or 0

        failing_df = conn.execute(f"""
            WITH spans AS (
                SELECT "PatID", "Enr_Start", "Enr_End",
                       LAG("Enr_End") OVER (
                           PARTITION BY "PatID" ORDER BY "Enr_Start"
                       ) AS prev_end
                FROM "{safe_view}"
            )
            SELECT "PatID", "Enr_Start", "Enr_End", prev_end
            FROM spans
            WHERE "Enr_Start" < prev_end
            LIMIT {max_failing_rows}
        """).pl()
    except duckdb.Error as e:
        log.error("overlapping spans check failed", error=str(e), view=view_name)
        return StepResult(
            step_index=-1,
            assertion_type="overlapping_spans",
            column="PatID, Enr_Start, Enr_End",
            description=f"Overlapping spans check error: {e}",
            n_passed=0,
            n_failed=0,
            failing_rows=None,
            check_id="215",
            severity="Fail",
        )

    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    return StepResult(
        step_index=-1,
        assertion_type="overlapping_spans",
        column="PatID, Enr_Start, Enr_End",
        description="No overlapping enrollment spans (check 215)",
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=failing_df if failing_df.height > 0 else None,
        check_id="215",
        severity="Fail",
    )


def check_enrollment_gaps(
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame],
    *,
    max_failing_rows: int = 500,
) -> StepResult | None:
    """Check 216: Detect non-bridged enrollment gaps.

    For each patient, sorts spans by Enr_Start and checks for gaps where
    previous Enr_End + 1 day < current Enr_Start. Adjacent spans
    (Enr_End + 1 day == next Enr_Start) pass.

    Handles both integer (SAS date) and pl.Date dtypes.
    """
    if schema.table_key != "enrollment":
        return None

    all_spans: list[pl.DataFrame] = []
    total_rows = 0

    for chunk in chunks:
        cols = ["PatID", "Enr_Start", "Enr_End"]
        if all(c in chunk.columns for c in cols):
            all_spans.append(chunk.select(cols))
            total_rows += chunk.height

    if not all_spans:
        return None

    combined = pl.concat(all_spans).sort("PatID", "Enr_Start")

    with_prev = combined.with_columns(
        pl.col("Enr_End").shift(1).over("PatID").alias("prev_end")
    )

    # Gap: prev_end + 1 day < Enr_Start (more than 1 day between spans)
    # Use dtype-aware increment: duration(days=1) for Date, +1 for integers
    enr_dtype = combined["Enr_Start"].dtype
    if enr_dtype == pl.Date or enr_dtype == pl.Datetime:
        one_day = pl.duration(days=1)
    else:
        one_day = 1

    gaps = with_prev.filter(
        pl.col("prev_end").is_not_null() & ((pl.col("prev_end") + one_day) < pl.col("Enr_Start"))
    )

    n_failed = gaps.height
    n_passed = total_rows - n_failed if total_rows > n_failed else 0

    return StepResult(
        step_index=-1,
        assertion_type="enrollment_gaps",
        column="PatID, Enr_Start, Enr_End",
        description="No non-bridged enrollment gaps (check 216)",
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=gaps.head(max_failing_rows) if gaps.height > 0 else None,
        check_id="216",
        severity="Warn",
    )


def check_enc_combinations(
    schema: TableSchema,
    chunks: Iterator[pl.DataFrame],
    *,
    max_failing_rows: int = 500,
) -> list[StepResult]:
    """Checks 244 and 245: Validate ENC field combinations.

    244: Flag rows not matching valid combination rules.
    245: Flag EncType groups exceeding rate threshold for invalid combos.

    Returns one StepResult for check 244, plus one per EncType threshold test for 245.
    """
    if schema.table_key != "encounter":
        return []

    required_cols = {"EncType", "DDate", "Discharge_Disposition", "Discharge_Status"}

    all_rows: list[pl.DataFrame] = []
    for chunk in chunks:
        if required_cols.issubset(set(chunk.columns)):
            all_rows.append(chunk.select(list(required_cols)))

    if not all_rows:
        return []

    combined = pl.concat(all_rows)
    total_rows = combined.height

    # Derive ddate_state column
    combined = combined.with_columns(
        pl.when(pl.col("DDate").is_null())
        .then(pl.lit("Null"))
        .otherwise(pl.lit("Present"))
        .alias("_ddate_state")
    )

    # Check each row against combination rules
    # Build a boolean column: True if row is INVALID
    invalid_mask = pl.lit(False)
    for enc_type, (ddate_req, disp_req, status_req) in ENC_COMBINATION_RULES.items():
        type_match = pl.col("EncType") == enc_type
        violation = pl.lit(False)
        if ddate_req:
            violation = violation | (pl.col("_ddate_state") == "Null")
        if disp_req:
            violation = violation | pl.col("Discharge_Disposition").is_null()
        if status_req:
            violation = violation | pl.col("Discharge_Status").is_null()
        invalid_mask = invalid_mask | (type_match & violation)

    # Also flag unknown EncType values
    known_types = list(ENC_COMBINATION_RULES.keys())
    invalid_mask = invalid_mask | ~pl.col("EncType").is_in(known_types)

    combined = combined.with_columns(invalid_mask.alias("_invalid"))
    invalid_rows = combined.filter(pl.col("_invalid"))

    # Check 244: per-row invalid combos
    n_failed_244 = invalid_rows.height
    n_passed_244 = total_rows - n_failed_244

    failing_244 = None
    if n_failed_244 > 0:
        failing_244 = invalid_rows.drop("_ddate_state", "_invalid").head(max_failing_rows)

    results: list[StepResult] = [
        StepResult(
            step_index=-1,
            assertion_type="enc_combinations",
            column="EncType, DDate, Discharge_Disposition, Discharge_Status",
            description="Valid ENC field combination (check 244)",
            n_passed=n_passed_244,
            n_failed=n_failed_244,
            failing_rows=failing_244,
            check_id="244",
            severity="Fail",
        )
    ]

    # Check 245: rate threshold per EncType
    for enc_type, threshold in ENC_RATE_THRESHOLDS.items():
        type_rows = combined.filter(pl.col("EncType") == enc_type)
        type_total = type_rows.height
        if type_total == 0:
            continue

        type_invalid = type_rows.filter(pl.col("_invalid")).height
        rate = type_invalid / type_total

        if rate > threshold:
            n_failed = type_invalid
            n_passed = type_total - type_invalid
        else:
            n_failed = 0
            n_passed = type_total

        results.append(
            StepResult(
                step_index=-1,
                assertion_type="enc_combination_rate",
                column=f"EncType={enc_type}",
                description=f"{enc_type} invalid combo rate {'>' if rate > threshold else '<='} {threshold:.0%} (check 245)",
                n_passed=n_passed,
                n_failed=n_failed,
                failing_rows=None,
                check_id="245",
                severity="Fail",
            )
        )

    return results

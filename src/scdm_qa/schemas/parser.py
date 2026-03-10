from __future__ import annotations

import json
import re
from pathlib import Path

import structlog

from scdm_qa.schemas.models import ColumnDef, ConditionalRule, TableSchema

log = structlog.get_logger(__name__)

_SPEC_PATH = Path(__file__).parent / "tables_documentation.json"

_TABLE_NAME_TO_KEY: dict[str, str] = {
    "Enrollment Table": "enrollment",
    "Demographic Table": "demographic",
    "Dispensing Table": "dispensing",
    "Encounter Table": "encounter",
    "Diagnosis Table": "diagnosis",
    "Procedure Table": "procedure",
    "Prescribing Table": "prescribing",
    "Facility Table": "facility",
    "Provider Table": "provider",
    "Laboratory Result Table": "laboratory",
    "Vital Signs Table": "vital_signs",
    "Death Table": "death",
    "Cause of Death Table": "cause_of_death",
    "Inpatient Pharmacy Table": "inpatient_pharmacy",
    "Inpatient Transfusion Table": "inpatient_transfusion",
    "Mother-Infant Linkage Table": "mother_infant_linkage",
    "Patient-Reported Measures Survey Table": "patient_reported_survey",
    "Patient-Reported Measures Survey Response Table": "patient_reported_response",
    "Feature Engineering Table": "feature_engineering",
}

_CONDITIONAL_PATTERN = re.compile(
    r"conditional on (\w+) value\.?\s+If \1\s*=\s*(.+?)(?:,|$)",
    re.IGNORECASE,
)


def parse_spec(spec_path: Path | None = None) -> list[TableSchema]:
    path = spec_path or _SPEC_PATH
    with open(path, "rb") as f:
        raw = json.load(f)

    tables: list[TableSchema] = []
    for table_raw in raw["scdm_tables"]:
        table_name = table_raw["table_name"]
        table_key = _TABLE_NAME_TO_KEY.get(table_name)
        if table_key is None:
            raise ValueError(f"unknown table name in spec: {table_name!r}")

        columns: list[ColumnDef] = []
        conditional_rules: list[ConditionalRule] = []

        for var in table_raw["variables"]:
            missing_allowed_raw = var["missing_allowed"]
            length_raw = var.get("length")
            values_raw = var.get("values")

            missing_allowed = _parse_missing_allowed(missing_allowed_raw)
            length = length_raw if isinstance(length_raw, int) else None

            allowed_values: frozenset[str] | None = None
            if isinstance(values_raw, list):
                allowed_values = frozenset(v["code"] for v in values_raw)

            columns.append(
                ColumnDef(
                    name=var["name"],
                    col_type=var["type"],
                    missing_allowed=missing_allowed,
                    length=length,
                    allowed_values=allowed_values,
                    definition=var.get("definition", ""),
                    example=var.get("example", ""),
                )
            )

            if isinstance(missing_allowed_raw, str):
                rule = _parse_conditional_rule(var["name"], missing_allowed_raw)
                if rule is not None:
                    conditional_rules.append(rule)

        unique_row_raw = table_raw.get("unique_row")
        unique_row = tuple(unique_row_raw) if unique_row_raw else ()

        tables.append(
            TableSchema(
                table_name=table_name,
                table_key=table_key,
                description=table_raw.get("description", ""),
                sort_order=tuple(table_raw.get("sort_order", [])),
                unique_row=unique_row,
                columns=tuple(columns),
                conditional_rules=tuple(conditional_rules),
            )
        )

    return tables


def _parse_missing_allowed(value: bool | str) -> bool:
    if isinstance(value, bool):
        return value
    lower = value.lower()
    if "conditional" in lower or "special missing" in lower:
        return True
    log.warning(
        "unrecognised missing_allowed string, treating as nullable",
        value=value,
    )
    return True


def _parse_conditional_rule(
    target_column: str, missing_text: str
) -> ConditionalRule | None:
    match = _CONDITIONAL_PATTERN.search(missing_text)
    if match is None:
        return None

    condition_column = match.group(1)
    values_str = match.group(2)

    condition_values: frozenset[str]
    if values_str:
        codes = [v.strip() for v in re.split(r"\s+or\s+|,\s*", values_str)]
        condition_values = frozenset(codes)
    else:
        condition_values = frozenset()

    return ConditionalRule(
        target_column=target_column,
        condition_column=condition_column,
        condition_values=condition_values,
        description=missing_text,
    )

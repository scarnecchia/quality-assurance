"""Parser for cross_table_checks.json (cross-table validation rules)."""

from __future__ import annotations

import json
from pathlib import Path

from scdm_qa.config import ConfigError
from scdm_qa.schemas.models import CrossTableCheckDef

_SPEC_PATH = Path(__file__).parent / "cross_table_checks.json"

# Lazy-loaded module-level cache
_CROSS_TABLE_CHECKS: tuple[CrossTableCheckDef, ...] | None = None


def load_cross_table_checks() -> tuple[CrossTableCheckDef, ...]:
    """Load cross-table check definitions from cross_table_checks.json.

    Returns:
        Tuple of CrossTableCheckDef instances.

    Raises:
        ConfigError: If required fields are missing from any check entry based on its check_type.
    """
    with open(_SPEC_PATH) as f:
        spec = json.load(f)

    checks: list[CrossTableCheckDef] = []

    # Parse cross-table checks
    for i, check_dict in enumerate(spec.get("checks", [])):
        check_type = check_dict.get("check_type")

        # Validate required fields based on check_type
        if check_type == "referential_integrity":
            required_fields = {
                "check_id",
                "check_type",
                "severity",
                "description",
                "source_table",
                "reference_table",
                "source_column",
                "reference_column",
            }
        elif check_type == "length_consistency":
            required_fields = {
                "check_id",
                "check_type",
                "severity",
                "description",
                "source_table",
                "source_column",
                "table_group",
            }
        elif check_type == "cross_date_compare":
            required_fields = {
                "check_id",
                "check_type",
                "severity",
                "description",
                "source_table",
                "reference_table",
                "source_column",
                "reference_column",
                "target_column",
            }
        elif check_type == "length_excess":
            required_fields = {
                "check_id",
                "check_type",
                "severity",
                "description",
                "source_table",
                "source_column",
            }
        elif check_type == "column_mismatch":
            required_fields = {
                "check_id",
                "check_type",
                "severity",
                "description",
                "source_table",
                "column_a",
                "column_b",
            }
        else:
            raise ConfigError(f"checks[{i}] has unknown check_type: {check_type}")

        missing = required_fields - set(check_dict.keys())
        if missing:
            raise ConfigError(f"checks[{i}] missing required fields: {missing}")

        null_fields = {f for f in required_fields if check_dict.get(f) is None}
        if null_fields:
            raise ConfigError(f"checks[{i}] has null values for required fields: {null_fields}")

        # Convert table_group from list to tuple if present
        table_group = check_dict.get("table_group")
        if table_group is not None and isinstance(table_group, list):
            table_group = tuple(table_group) if table_group else None

        checks.append(
            CrossTableCheckDef(
                check_id=check_dict["check_id"],
                check_type=check_dict["check_type"],
                severity=check_dict["severity"],
                description=check_dict["description"],
                source_table=check_dict["source_table"],
                reference_table=check_dict.get("reference_table"),
                source_column=check_dict.get("source_column"),
                reference_column=check_dict.get("reference_column"),
                target_column=check_dict.get("target_column"),
                column_a=check_dict.get("column_a"),
                column_b=check_dict.get("column_b"),
                table_group=table_group,
            )
        )

    return tuple(checks)


def _ensure_loaded() -> None:
    """Lazily load checks if not already loaded."""
    global _CROSS_TABLE_CHECKS
    if _CROSS_TABLE_CHECKS is None:
        _CROSS_TABLE_CHECKS = load_cross_table_checks()


def get_cross_table_checks() -> tuple[CrossTableCheckDef, ...]:
    """Return all cross-table check definitions."""
    _ensure_loaded()
    assert _CROSS_TABLE_CHECKS is not None
    return _CROSS_TABLE_CHECKS


def get_checks_for_table(table_key: str) -> tuple[CrossTableCheckDef, ...]:
    """Return cross-table check definitions where table_key is source or reference table.

    Args:
        table_key: The table key to filter by.

    Returns:
        Tuple of CrossTableCheckDef instances that involve this table.
    """
    _ensure_loaded()
    assert _CROSS_TABLE_CHECKS is not None
    return tuple(
        c
        for c in _CROSS_TABLE_CHECKS
        if c.source_table == table_key or c.reference_table == table_key
    )

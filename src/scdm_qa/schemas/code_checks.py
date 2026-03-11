"""Parser for code_checks.json (format 223 and length 228 validation rules)."""

from __future__ import annotations

import json
from pathlib import Path

from scdm_qa.config import ConfigError
from scdm_qa.schemas.models import FormatCheckDef, LengthCheckDef

_SPEC_PATH = Path(__file__).parent / "code_checks.json"

# Lazy-loaded module-level caches
_FORMAT_CHECKS: tuple[FormatCheckDef, ...] | None = None
_LENGTH_CHECKS: tuple[LengthCheckDef, ...] | None = None


def load_code_checks() -> tuple[tuple[FormatCheckDef, ...], tuple[LengthCheckDef, ...]]:
    """Load code check definitions from code_checks.json.

    Returns:
        Tuple of (format_checks, length_checks) where each is a tuple of the respective dataclass instances.

    Raises:
        ConfigError: If required fields are missing from any check entry.
    """
    with open(_SPEC_PATH) as f:
        spec = json.load(f)

    if "format_checks" not in spec:
        raise ConfigError("code_checks.json missing 'format_checks' key")
    if "length_checks" not in spec:
        raise ConfigError("code_checks.json missing 'length_checks' key")

    format_checks: list[FormatCheckDef] = []
    length_checks: list[LengthCheckDef] = []

    # Parse format checks (check_id 223)
    for i, check_dict in enumerate(spec["format_checks"]):
        required_fields = {
            "check_id",
            "table_key",
            "column",
            "codetype_column",
            "codetype_value",
            "check_subtype",
            "severity",
            "pattern",
            "description",
        }
        missing = required_fields - set(check_dict.keys())
        if missing:
            raise ConfigError(
                f"format_checks[{i}] missing required fields: {missing}"
            )

        # Handle condition_values: convert to tuple if present
        condition_values = check_dict.get("condition_values")
        if condition_values is not None and isinstance(condition_values, list):
            condition_values = tuple(condition_values) if condition_values else None

        format_checks.append(
            FormatCheckDef(
                check_id=check_dict["check_id"],
                table_key=check_dict["table_key"],
                column=check_dict["column"],
                codetype_column=check_dict["codetype_column"],
                codetype_value=check_dict["codetype_value"],
                check_subtype=check_dict["check_subtype"],
                severity=check_dict["severity"],
                pattern=check_dict["pattern"],
                description=check_dict["description"],
                date_column=check_dict.get("date_column"),
                era_boundary=check_dict.get("era_boundary"),
                condition_column=check_dict.get("condition_column"),
                condition_values=condition_values,
                expect_null=check_dict.get("expect_null", False),
            )
        )

    # Parse length checks (check_id 228)
    for i, check_dict in enumerate(spec["length_checks"]):
        required_fields = {
            "check_id",
            "table_key",
            "column",
            "codetype_column",
            "codetype_value",
            "min_length",
            "max_length",
            "severity",
            "description",
        }
        missing = required_fields - set(check_dict.keys())
        if missing:
            raise ConfigError(f"length_checks[{i}] missing required fields: {missing}")

        length_checks.append(
            LengthCheckDef(
                check_id=check_dict["check_id"],
                table_key=check_dict["table_key"],
                column=check_dict["column"],
                codetype_column=check_dict["codetype_column"],
                codetype_value=check_dict["codetype_value"],
                min_length=check_dict["min_length"],
                max_length=check_dict["max_length"],
                severity=check_dict["severity"],
                description=check_dict["description"],
            )
        )

    return tuple(format_checks), tuple(length_checks)


def _ensure_loaded() -> None:
    """Lazily load checks if not already loaded."""
    global _FORMAT_CHECKS, _LENGTH_CHECKS
    if _FORMAT_CHECKS is None or _LENGTH_CHECKS is None:
        _FORMAT_CHECKS, _LENGTH_CHECKS = load_code_checks()


def get_format_checks_for_table(table_key: str) -> tuple[FormatCheckDef, ...]:
    """Return format check definitions for a given table key."""
    _ensure_loaded()
    assert _FORMAT_CHECKS is not None
    return tuple(c for c in _FORMAT_CHECKS if c.table_key == table_key)


def get_length_checks_for_table(table_key: str) -> tuple[LengthCheckDef, ...]:
    """Return length check definitions for a given table key."""
    _ensure_loaded()
    assert _LENGTH_CHECKS is not None
    return tuple(c for c in _LENGTH_CHECKS if c.table_key == table_key)

"""Tests for code_checks module (format 223 and length 228 rules)."""

from dataclasses import FrozenInstanceError

import pytest

from scdm_qa.config import ConfigError
from scdm_qa.schemas.code_checks import (
    get_format_checks_for_table,
    get_length_checks_for_table,
    load_code_checks,
)
from scdm_qa.schemas.models import FormatCheckDef, LengthCheckDef


class TestFormatCheckDef:
    """Verify FormatCheckDef is a frozen dataclass with required fields."""

    def test_format_check_def_creation(self):
        """Construct a FormatCheckDef instance."""
        check = FormatCheckDef(
            check_id="223",
            table_key="diagnosis",
            column="DX",
            codetype_column="DX_CodeType",
            codetype_value="09",
            check_subtype="no_decimal",
            severity="Fail",
            pattern=None,
            description="ICD-9 codes must not contain decimals",
        )
        assert check.check_id == "223"
        assert check.table_key == "diagnosis"
        assert check.column == "DX"
        assert check.codetype_column == "DX_CodeType"
        assert check.codetype_value == "09"
        assert check.check_subtype == "no_decimal"
        assert check.severity == "Fail"
        assert check.pattern is None
        assert check.description == "ICD-9 codes must not contain decimals"

    def test_format_check_def_with_pattern(self):
        """Construct a FormatCheckDef with regex pattern."""
        check = FormatCheckDef(
            check_id="223",
            table_key="procedure",
            column="PX",
            codetype_column="PX_CodeType",
            codetype_value="C4",
            check_subtype="regex",
            severity="Fail",
            pattern=r"^\d{4}[AaMmUu]$|^\d{5}$",
            description="CPT-4 codes must match format",
        )
        assert check.pattern == r"^\d{4}[AaMmUu]$|^\d{5}$"

    def test_format_check_def_era_date_fields(self):
        """Construct a FormatCheckDef with era_date fields."""
        check = FormatCheckDef(
            check_id="223",
            table_key="diagnosis",
            column="DX",
            codetype_column="DX_CodeType",
            codetype_value="09",
            check_subtype="era_date",
            severity="Fail",
            pattern=None,
            description="ICD-9 codes after transition date",
            date_column="ADate",
            era_boundary="2015-10-01",
        )
        assert check.date_column == "ADate"
        assert check.era_boundary == "2015-10-01"

    def test_format_check_def_conditional_presence_fields(self):
        """Construct a FormatCheckDef with conditional_presence fields."""
        check = FormatCheckDef(
            check_id="223",
            table_key="diagnosis",
            column="PDX",
            codetype_column="DX_CodeType",
            codetype_value="09",
            check_subtype="conditional_presence",
            severity="Fail",
            pattern=None,
            description="PDX required for inpatient encounters",
            condition_column="EncType",
            condition_values=("IP", "IS"),
            expect_null=False,
        )
        assert check.condition_column == "EncType"
        assert check.condition_values == ("IP", "IS")
        assert check.expect_null is False

    def test_format_check_def_is_frozen(self):
        """Verify FormatCheckDef is frozen (cannot reassign fields)."""
        check = FormatCheckDef(
            check_id="223",
            table_key="diagnosis",
            column="DX",
            codetype_column="DX_CodeType",
            codetype_value="09",
            check_subtype="no_decimal",
            severity="Fail",
            pattern=None,
            description="ICD-9 codes must not contain decimals",
        )
        with pytest.raises(FrozenInstanceError):
            check.check_id = "999"


class TestLengthCheckDef:
    """Verify LengthCheckDef is a frozen dataclass with required fields."""

    def test_length_check_def_creation(self):
        """Construct a LengthCheckDef instance."""
        check = LengthCheckDef(
            check_id="228",
            table_key="diagnosis",
            column="DX",
            codetype_column="DX_CodeType",
            codetype_value="09",
            min_length=3,
            max_length=5,
            severity="Warn",
            description="ICD-9 codes must be 3-5 chars",
        )
        assert check.check_id == "228"
        assert check.table_key == "diagnosis"
        assert check.column == "DX"
        assert check.codetype_column == "DX_CodeType"
        assert check.codetype_value == "09"
        assert check.min_length == 3
        assert check.max_length == 5
        assert check.severity == "Warn"
        assert check.description == "ICD-9 codes must be 3-5 chars"

    def test_length_check_def_is_frozen(self):
        """Verify LengthCheckDef is frozen."""
        check = LengthCheckDef(
            check_id="228",
            table_key="diagnosis",
            column="DX",
            codetype_column="DX_CodeType",
            codetype_value="09",
            min_length=3,
            max_length=5,
            severity="Warn",
            description="ICD-9 codes must be 3-5 chars",
        )
        with pytest.raises(FrozenInstanceError):
            check.min_length = 4


class TestCodeChecksParser:
    """Test load_code_checks and table filtering functions."""

    def test_load_code_checks_returns_tuples(self):
        """load_code_checks returns tuple of format and length checks."""
        format_checks, length_checks = load_code_checks()
        assert isinstance(format_checks, tuple)
        assert isinstance(length_checks, tuple)
        assert len(format_checks) > 0
        assert len(length_checks) > 0

    def test_all_format_checks_are_correct_type(self):
        """All items in format_checks tuple are FormatCheckDef."""
        format_checks, _ = load_code_checks()
        for check in format_checks:
            assert isinstance(check, FormatCheckDef)
            assert check.check_id == "223"

    def test_all_length_checks_are_correct_type(self):
        """All items in length_checks tuple are LengthCheckDef."""
        _, length_checks = load_code_checks()
        for check in length_checks:
            assert isinstance(check, LengthCheckDef)
            assert check.check_id == "228"

    def test_get_format_checks_for_diagnosis_table(self):
        """get_format_checks_for_table returns only diagnosis rules."""
        checks = get_format_checks_for_table("diagnosis")
        assert isinstance(checks, tuple)
        for check in checks:
            assert check.table_key == "diagnosis"

    def test_get_format_checks_for_nonexistent_table(self):
        """get_format_checks_for_table returns empty tuple for nonexistent table."""
        checks = get_format_checks_for_table("nonexistent")
        assert checks == ()

    def test_get_length_checks_for_procedure_table(self):
        """get_length_checks_for_table returns only procedure rules."""
        checks = get_length_checks_for_table("procedure")
        assert isinstance(checks, tuple)
        for check in checks:
            assert check.table_key == "procedure"

    def test_get_length_checks_for_nonexistent_table(self):
        """get_length_checks_for_table returns empty tuple for nonexistent table."""
        checks = get_length_checks_for_table("nonexistent")
        assert checks == ()

    def test_malformed_json_raises_config_error(self, tmp_path, monkeypatch):
        """Loading from malformed JSON raises ConfigError."""
        # Create a temporary malformed JSON file
        bad_json = tmp_path / "bad_code_checks.json"
        bad_json.write_text('{"format_checks": [{"check_id": "223"}]}')  # missing required fields

        # Monkeypatch the code_checks module to use our bad file
        import scdm_qa.schemas.code_checks as cc_module

        original_spec_path = cc_module._SPEC_PATH

        # Temporarily replace the spec path
        monkeypatch.setattr(cc_module, "_SPEC_PATH", bad_json)
        monkeypatch.setattr(cc_module, "_FORMAT_CHECKS", None)
        monkeypatch.setattr(cc_module, "_LENGTH_CHECKS", None)

        try:
            with pytest.raises(ConfigError):
                cc_module.load_code_checks()
        finally:
            monkeypatch.setattr(cc_module, "_SPEC_PATH", original_spec_path)

    def test_format_check_fields_populated_correctly(self):
        """Format check fields are populated correctly from JSON."""
        format_checks, _ = load_code_checks()
        # Find a no_decimal check
        no_decimal = next(
            (c for c in format_checks if c.check_subtype == "no_decimal"),
            None,
        )
        assert no_decimal is not None
        assert no_decimal.pattern is None
        assert no_decimal.date_column is None
        assert no_decimal.era_boundary is None

    def test_length_check_fields_populated_correctly(self):
        """Length check fields are populated correctly from JSON."""
        _, length_checks = load_code_checks()
        assert len(length_checks) > 0
        for check in length_checks[:1]:
            assert check.min_length > 0
            assert check.max_length >= check.min_length
            assert check.severity == "Warn"

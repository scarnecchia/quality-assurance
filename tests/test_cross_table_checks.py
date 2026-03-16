"""Tests for cross_table_checks module (cross-table validation rules)."""

from dataclasses import FrozenInstanceError

import pytest

from scdm_qa.config import ConfigError
from scdm_qa.schemas.cross_table_checks import (
    get_checks_for_table,
    get_cross_table_checks,
    load_cross_table_checks,
)
from scdm_qa.schemas.models import CrossTableCheckDef


class TestCrossTableCheckDef:
    """Verify CrossTableCheckDef is a frozen dataclass with required fields."""

    def test_referential_integrity_check_creation(self):
        """Construct a referential_integrity CrossTableCheckDef."""
        check = CrossTableCheckDef(
            check_id="201",
            check_type="referential_integrity",
            severity="Warn",
            description="PatID in diagnosis but not in enrollment",
            source_table="diagnosis",
            reference_table="enrollment",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column=None,
            column_a=None,
            column_b=None,
            table_group=None,
        )
        assert check.check_id == "201"
        assert check.check_type == "referential_integrity"
        assert check.source_table == "diagnosis"
        assert check.reference_table == "enrollment"
        assert check.join_column == "PatID"
        assert check.join_reference_column == "PatID"

    def test_length_consistency_check_creation(self):
        """Construct a length_consistency CrossTableCheckDef."""
        check = CrossTableCheckDef(
            check_id="203",
            check_type="length_consistency",
            severity="Warn",
            description="PatID length consistency across tables",
            source_table="enrollment",
            reference_table=None,
            join_column="PatID",
            join_reference_column=None,
            compare_column=None,
            column_a=None,
            column_b=None,
            table_group=("enrollment", "demographic", "diagnosis"),
        )
        assert check.check_id == "203"
        assert check.check_type == "length_consistency"
        assert check.table_group == ("enrollment", "demographic", "diagnosis")
        assert check.join_column == "PatID"

    def test_cross_date_compare_check_creation(self):
        """Construct a cross_date_compare CrossTableCheckDef."""
        check = CrossTableCheckDef(
            check_id="205",
            check_type="cross_date_compare",
            severity="Fail",
            description="Enr_Start before Birth_Date",
            source_table="enrollment",
            reference_table="demographic",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column="Enr_Start",
            compare_reference_column="Birth_Date",
            column_a=None,
            column_b=None,
            table_group=None,
        )
        assert check.check_id == "205"
        assert check.check_type == "cross_date_compare"
        assert check.compare_column == "Enr_Start"
        assert check.reference_table == "demographic"

    def test_length_excess_check_creation(self):
        """Construct a length_excess CrossTableCheckDef."""
        check = CrossTableCheckDef(
            check_id="209",
            check_type="length_excess",
            severity="Warn",
            description="Actual PatID length much smaller than declared",
            source_table="diagnosis",
            reference_table=None,
            join_column="PatID",
            join_reference_column=None,
            compare_column=None,
            column_a=None,
            column_b=None,
            table_group=None,
        )
        assert check.check_id == "209"
        assert check.check_type == "length_excess"
        assert check.join_column == "PatID"

    def test_column_mismatch_check_creation(self):
        """Construct a column_mismatch CrossTableCheckDef."""
        check = CrossTableCheckDef(
            check_id="224",
            check_type="column_mismatch",
            severity="Warn",
            description="Hispanic must not differ from ImputedHispanic",
            source_table="demographic",
            reference_table=None,
            join_column=None,
            join_reference_column=None,
            compare_column=None,
            column_a="Hispanic",
            column_b="ImputedHispanic",
            table_group=None,
        )
        assert check.check_id == "224"
        assert check.check_type == "column_mismatch"
        assert check.column_a == "Hispanic"
        assert check.column_b == "ImputedHispanic"
        assert check.reference_table is None

    def test_cross_table_check_def_is_frozen(self):
        """Verify CrossTableCheckDef is frozen (cannot reassign fields)."""
        check = CrossTableCheckDef(
            check_id="201",
            check_type="referential_integrity",
            severity="Warn",
            description="PatID in diagnosis but not in enrollment",
            source_table="diagnosis",
            reference_table="enrollment",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column=None,
            column_a=None,
            column_b=None,
            table_group=None,
        )
        with pytest.raises(FrozenInstanceError):
            check.check_id = "999"

    def test_all_fields_accessible(self):
        """All fields are accessible and set correctly."""
        check = CrossTableCheckDef(
            check_id="201",
            check_type="referential_integrity",
            severity="Warn",
            description="Test description",
            source_table="diagnosis",
            reference_table="enrollment",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column=None,
            column_a=None,
            column_b=None,
            table_group=None,
        )
        assert check.check_id == "201"
        assert check.check_type == "referential_integrity"
        assert check.severity == "Warn"
        assert check.description == "Test description"
        assert check.source_table == "diagnosis"
        assert check.reference_table == "enrollment"
        assert check.join_column == "PatID"
        assert check.join_reference_column == "PatID"
        assert check.compare_column is None
        assert check.column_a is None
        assert check.column_b is None
        assert check.table_group is None


class TestCrossTableChecksParser:
    """Test load_cross_table_checks and table filtering functions."""

    def test_load_cross_table_checks_returns_tuple(self):
        """load_cross_table_checks returns tuple of CrossTableCheckDef instances."""
        checks = load_cross_table_checks()
        assert isinstance(checks, tuple)
        assert len(checks) > 0

    def test_all_checks_are_correct_type(self):
        """All items in checks tuple are CrossTableCheckDef."""
        checks = load_cross_table_checks()
        for check in checks:
            assert isinstance(check, CrossTableCheckDef)

    def test_get_cross_table_checks_returns_all(self):
        """get_cross_table_checks returns all checks."""
        checks = get_cross_table_checks()
        assert isinstance(checks, tuple)
        assert len(checks) > 0
        loaded = load_cross_table_checks()
        assert checks == loaded

    def test_referential_integrity_checks_present(self):
        """Check 201 referential_integrity checks are present."""
        checks = get_cross_table_checks()
        ref_int_checks = [c for c in checks if c.check_type == "referential_integrity"]
        assert len(ref_int_checks) > 0
        assert all(c.check_id == "201" for c in ref_int_checks)

    def test_length_consistency_checks_present(self):
        """Check 203 length_consistency checks are present."""
        checks = get_cross_table_checks()
        len_con_checks = [c for c in checks if c.check_type == "length_consistency"]
        assert len(len_con_checks) > 0
        assert all(c.check_id == "203" for c in len_con_checks)

    def test_cross_date_compare_checks_present(self):
        """Cross_date_compare checks (205, 206, 227) are present."""
        checks = get_cross_table_checks()
        date_checks = [c for c in checks if c.check_type == "cross_date_compare"]
        assert len(date_checks) > 0
        check_ids = {c.check_id for c in date_checks}
        assert {"205", "206", "227"} <= check_ids

    def test_length_excess_checks_present(self):
        """Check 209 length_excess checks are present."""
        checks = get_cross_table_checks()
        excess_checks = [c for c in checks if c.check_type == "length_excess"]
        assert len(excess_checks) > 0
        assert all(c.check_id == "209" for c in excess_checks)

    def test_column_mismatch_check_present(self):
        """Check 224 column_mismatch check is present."""
        checks = get_cross_table_checks()
        mismatch_checks = [c for c in checks if c.check_type == "column_mismatch"]
        assert len(mismatch_checks) > 0
        assert all(c.check_id == "224" for c in mismatch_checks)

    def test_get_checks_for_enrollment_table(self):
        """get_checks_for_table returns checks involving enrollment table."""
        checks = get_checks_for_table("enrollment")
        assert isinstance(checks, tuple)
        assert len(checks) > 0
        # enrollment is involved in referential_integrity as reference_table
        # and in length_consistency as source_table
        assert any(c.reference_table == "enrollment" for c in checks)

    def test_get_checks_for_diagnosis_table(self):
        """get_checks_for_table returns checks involving diagnosis table."""
        checks = get_checks_for_table("diagnosis")
        assert isinstance(checks, tuple)
        assert len(checks) > 0
        # diagnosis is source_table in some referential_integrity checks
        assert any(c.source_table == "diagnosis" for c in checks)

    def test_get_checks_for_nonexistent_table(self):
        """get_checks_for_table returns empty tuple for nonexistent table."""
        checks = get_checks_for_table("nonexistent_table")
        assert checks == ()

    def test_demographic_referenced_as_reference_table(self):
        """demographic table is used as reference_table in cross_date_compare checks."""
        checks = get_checks_for_table("demographic")
        # Should include cross_date_compare checks where demographic is the reference
        cross_date_checks = [c for c in checks if c.check_type == "cross_date_compare"]
        assert len(cross_date_checks) > 0
        assert any(c.reference_table == "demographic" for c in cross_date_checks)

    def test_length_consistency_has_table_group(self):
        """length_consistency checks have table_group field populated."""
        checks = get_cross_table_checks()
        len_con_checks = [c for c in checks if c.check_type == "length_consistency"]
        assert len(len_con_checks) > 0
        for check in len_con_checks:
            assert check.table_group is not None
            assert isinstance(check.table_group, tuple)
            assert len(check.table_group) > 0

    def test_referential_integrity_has_required_fields(self):
        """referential_integrity checks have required fields."""
        checks = get_cross_table_checks()
        ref_int_checks = [c for c in checks if c.check_type == "referential_integrity"]
        for check in ref_int_checks:
            assert check.join_column is not None
            assert check.join_reference_column is not None
            assert check.reference_table is not None

    def test_column_mismatch_has_column_a_and_b(self):
        """column_mismatch checks have column_a and column_b."""
        checks = get_cross_table_checks()
        mismatch_checks = [c for c in checks if c.check_type == "column_mismatch"]
        for check in mismatch_checks:
            assert check.column_a is not None
            assert check.column_b is not None

    def test_malformed_json_raises_config_error(self, tmp_path, monkeypatch):
        """Loading from malformed JSON (missing required field) raises ConfigError."""
        bad_json = tmp_path / "bad_cross_table_checks.json"
        bad_json.write_text('{"checks": [{"check_id": "201"}]}')  # missing required fields

        import scdm_qa.schemas.cross_table_checks as ctc_module

        monkeypatch.setattr(ctc_module, "_SPEC_PATH", bad_json)
        monkeypatch.setattr(ctc_module, "_CROSS_TABLE_CHECKS", None)

        with pytest.raises(ConfigError):
            ctc_module.load_cross_table_checks()

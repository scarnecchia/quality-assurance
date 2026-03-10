"""Tests for cross_table_checks module (cross-table validation rules)."""

from dataclasses import FrozenInstanceError

import pytest

from scdm_qa.config import ConfigError
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
            source_column="PatID",
            reference_column="PatID",
            target_column=None,
            column_a=None,
            column_b=None,
            table_group=None,
        )
        assert check.check_id == "201"
        assert check.check_type == "referential_integrity"
        assert check.source_table == "diagnosis"
        assert check.reference_table == "enrollment"
        assert check.source_column == "PatID"
        assert check.reference_column == "PatID"

    def test_length_consistency_check_creation(self):
        """Construct a length_consistency CrossTableCheckDef."""
        check = CrossTableCheckDef(
            check_id="203",
            check_type="length_consistency",
            severity="Warn",
            description="PatID length consistency across tables",
            source_table="enrollment",
            reference_table=None,
            source_column="PatID",
            reference_column=None,
            target_column=None,
            column_a=None,
            column_b=None,
            table_group=("enrollment", "demographic", "diagnosis"),
        )
        assert check.check_id == "203"
        assert check.check_type == "length_consistency"
        assert check.table_group == ("enrollment", "demographic", "diagnosis")
        assert check.source_column == "PatID"

    def test_cross_date_compare_check_creation(self):
        """Construct a cross_date_compare CrossTableCheckDef."""
        check = CrossTableCheckDef(
            check_id="205",
            check_type="cross_date_compare",
            severity="Fail",
            description="Enr_Start before Birth_Date",
            source_table="enrollment",
            reference_table="demographic",
            source_column="PatID",
            reference_column="PatID",
            target_column="Enr_Start",
            column_a=None,
            column_b=None,
            table_group=None,
        )
        assert check.check_id == "205"
        assert check.check_type == "cross_date_compare"
        assert check.target_column == "Enr_Start"
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
            source_column="PatID",
            reference_column=None,
            target_column=None,
            column_a=None,
            column_b=None,
            table_group=None,
        )
        assert check.check_id == "209"
        assert check.check_type == "length_excess"
        assert check.source_column == "PatID"

    def test_column_mismatch_check_creation(self):
        """Construct a column_mismatch CrossTableCheckDef."""
        check = CrossTableCheckDef(
            check_id="224",
            check_type="column_mismatch",
            severity="Warn",
            description="Hispanic must not differ from ImputedHispanic",
            source_table="demographic",
            reference_table=None,
            source_column=None,
            reference_column=None,
            target_column=None,
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
            source_column="PatID",
            reference_column="PatID",
            target_column=None,
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
            source_column="PatID",
            reference_column="PatID",
            target_column=None,
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
        assert check.source_column == "PatID"
        assert check.reference_column == "PatID"
        assert check.target_column is None
        assert check.column_a is None
        assert check.column_b is None
        assert check.table_group is None

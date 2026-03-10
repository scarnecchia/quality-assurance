from __future__ import annotations

import polars as pl
import pytest

from scdm_qa.schemas import build_validation, get_schema, list_table_keys


class TestSchemaRegistry:
    def test_lists_19_table_keys(self) -> None:
        keys = list_table_keys()
        assert len(keys) == 19

    def test_get_schema_returns_table(self) -> None:
        schema = get_schema("demographic")
        assert schema.table_key == "demographic"

    def test_get_schema_raises_on_unknown_key(self) -> None:
        with pytest.raises(KeyError, match="unknown table key"):
            get_schema("nonexistent")


class TestBuildValidationNullability:
    def test_non_nullable_column_with_null_fails(self) -> None:
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1", None, "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
        })
        v = build_validation(df, schema).interrogate()
        fail_fractions = v.f_failed()
        assert any(f > 0 for f in fail_fractions.values()), "expected at least one failing step"


class TestBuildValidationEnumMembership:
    def test_invalid_enum_value_fails(self) -> None:
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "EncounterID": ["E1"],
            "ADate": [1000],
            "EncType": ["XX"],  # invalid
        })
        v = build_validation(df, schema).interrogate()
        fail_fractions = v.f_failed()
        assert any(f > 0 for f in fail_fractions.values())


class TestBuildValidationStringLength:
    def test_character_column_exceeding_length_fails(self) -> None:
        schema = get_schema("encounter")
        enctype_col = schema.get_column("EncType")
        assert enctype_col is not None
        assert enctype_col.length is not None

        long_value = "X" * (enctype_col.length + 10)
        df = pl.DataFrame({
            "PatID": ["P1"],
            "EncounterID": ["E1"],
            "ADate": [1000],
            "EncType": [long_value],
        })
        v = build_validation(df, schema).interrogate()
        fail_fractions = v.f_failed()
        assert any(f > 0 for f in fail_fractions.values())


class TestBuildValidationConditionalRules:
    def test_ddate_null_when_enctype_ip_fails(self) -> None:
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "EncounterID": ["E1"],
            "ADate": [1000],
            "EncType": ["IP"],
            "DDate": [None],
        })
        v = build_validation(df, schema).interrogate()
        fail_fractions = v.f_failed()
        assert any(f > 0 for f in fail_fractions.values())

    def test_ddate_null_when_enctype_av_passes(self) -> None:
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "EncounterID": ["E1"],
            "ADate": [1000],
            "EncType": ["AV"],
            "DDate": [None],
        })
        v = build_validation(df, schema).interrogate()
        fail_fractions = v.f_failed()
        # The conditional rule should not fail for AV since it's not in the trigger set
        # When the conditional precondition filters to an empty DataFrame, the step
        # has a None fail_fraction (zero rows matched condition = zero failures)
        # Verify that the conditional rule step (the last step) is None or 0
        conditional_rule_fraction = list(fail_fractions.values())[-1]
        assert conditional_rule_fraction is None or conditional_rule_fraction == 0.0

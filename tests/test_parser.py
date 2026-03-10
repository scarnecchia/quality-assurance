from __future__ import annotations

from scdm_qa.schemas.parser import parse_spec


class TestParseSpec:
    def test_parses_all_19_tables(self) -> None:
        tables = parse_spec()
        assert len(tables) == 19

    def test_all_table_keys_are_unique(self) -> None:
        tables = parse_spec()
        keys = [t.table_key for t in tables]
        assert len(keys) == len(set(keys))

    def test_expected_table_keys_present(self) -> None:
        tables = parse_spec()
        keys = {t.table_key for t in tables}
        expected = {
            "enrollment", "demographic", "dispensing", "encounter",
            "diagnosis", "procedure", "prescribing", "facility",
            "provider", "laboratory", "vital_signs", "death",
            "cause_of_death", "inpatient_pharmacy", "inpatient_transfusion",
            "mother_infant_linkage", "patient_reported_survey",
            "patient_reported_response", "feature_engineering",
        }
        assert keys == expected


class TestColumnParsing:
    def test_demographic_has_patid_column(self) -> None:
        tables = parse_spec()
        demo = next(t for t in tables if t.table_key == "demographic")
        col = demo.get_column("PatID")
        assert col is not None
        assert col.missing_allowed is False

    def test_encounter_enctype_has_allowed_values(self) -> None:
        tables = parse_spec()
        enc = next(t for t in tables if t.table_key == "encounter")
        col = enc.get_column("EncType")
        assert col is not None
        assert col.allowed_values is not None
        assert "IP" in col.allowed_values
        assert "AV" in col.allowed_values

    def test_character_column_has_type(self) -> None:
        tables = parse_spec()
        enc = next(t for t in tables if t.table_key == "encounter")
        enctype = enc.get_column("EncType")
        assert enctype is not None
        assert enctype.col_type == "Character"


class TestConditionalRules:
    def test_encounter_has_conditional_rules(self) -> None:
        tables = parse_spec()
        enc = next(t for t in tables if t.table_key == "encounter")
        assert len(enc.conditional_rules) > 0

    def test_ddate_conditional_on_enctype(self) -> None:
        tables = parse_spec()
        enc = next(t for t in tables if t.table_key == "encounter")
        ddate_rules = [r for r in enc.conditional_rules if r.target_column == "DDate"]
        assert len(ddate_rules) == 1
        rule = ddate_rules[0]
        assert rule.condition_column == "EncType"
        assert "IP" in rule.condition_values or "IS" in rule.condition_values


class TestUniqueRowAndSortOrder:
    def test_demographic_unique_row_is_patid(self) -> None:
        tables = parse_spec()
        demo = next(t for t in tables if t.table_key == "demographic")
        assert "PatID" in demo.unique_row

    def test_vital_signs_has_empty_unique_row(self) -> None:
        tables = parse_spec()
        vs = next(t for t in tables if t.table_key == "vital_signs")
        assert len(vs.unique_row) == 0

    def test_all_tables_have_sort_order(self) -> None:
        tables = parse_spec()
        for table in tables:
            assert len(table.sort_order) > 0, f"{table.table_key} has no sort_order"

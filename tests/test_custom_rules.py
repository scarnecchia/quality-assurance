from __future__ import annotations

from pathlib import Path

import polars as pl

from scdm_qa.schemas import build_validation, get_schema
from scdm_qa.schemas.custom_rules import apply_custom_rules, load_custom_rules


class TestLoadCustomRules:
    def test_returns_none_when_no_dir(self) -> None:
        result = load_custom_rules("demographic", None)
        assert result is None

    def test_returns_none_when_file_missing(self, tmp_path: Path) -> None:
        result = load_custom_rules("demographic", tmp_path)
        assert result is None

    def test_loads_extension_file(self, tmp_path: Path) -> None:
        rules_file = tmp_path / "demographic_rules.py"
        rules_file.write_text(
            "def extend_validation(validation, data):\n"
            "    return validation.col_vals_not_null(columns='PatID')\n"
        )
        result = load_custom_rules("demographic", tmp_path)
        assert result is not None
        assert callable(result)


class TestApplyCustomRules:
    def test_extends_validation_chain(self, tmp_path: Path) -> None:
        rules_file = tmp_path / "demographic_rules.py"
        rules_file.write_text(
            "def extend_validation(validation, data):\n"
            "    return validation.col_vals_not_null(columns='PatID')\n"
        )
        extend_fn = load_custom_rules("demographic", tmp_path)

        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        })
        validation = build_validation(df, schema)
        extended = apply_custom_rules(validation, df, extend_fn)
        result = extended.interrogate()
        # Should have more steps than without custom rules
        n_steps_original = len(build_validation(df, schema).interrogate().n_passed())
        n_steps_extended = len(result.n_passed())
        assert n_steps_extended > n_steps_original

    def test_noop_when_no_extension(self) -> None:
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        validation = build_validation(df, schema)
        result = apply_custom_rules(validation, df, None)
        assert result is validation  # same object, unchanged

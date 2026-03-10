from __future__ import annotations

from pathlib import Path

import polars as pl

from scdm_qa.readers import create_reader
from scdm_qa.schemas import get_schema
from scdm_qa.validation.runner import run_validation


class TestRunnerDetectsNullViolation:
    def test_null_patid_in_demographic(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "PatID": ["P1", None, "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        result = run_validation(reader, schema)

        assert not result.all_passed
        assert result.total_failures > 0


class TestRunnerDetectsInvalidEnum:
    def test_invalid_enctype(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "EncounterID": ["E1", "E2"],
            "ADate": [1000, 2000],
            "EncType": ["IP", "XX"],  # XX is invalid
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("encounter")
        result = run_validation(reader, schema)

        assert not result.all_passed


class TestRunnerMultipleChunks:
    def test_accumulates_across_chunks(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "PatID": [f"P{i}" for i in range(50)] + [None] * 5,
            "Birth_Date": list(range(55)),
            "Sex": ["F"] * 55,
            "Hispanic": ["Y"] * 55,
            "Race": ["1"] * 55,
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=20)
        schema = get_schema("demographic")
        result = run_validation(reader, schema)

        assert result.chunks_processed > 1
        assert result.total_rows == 55
        assert result.total_failures > 0


class TestRunnerDetectsOverLengthCharacters:
    def test_overlength_enctype_in_encounter(self, tmp_path: Path) -> None:
        """AC2.3: Character columns exceeding spec-defined string lengths produce validation warnings.

        EncType has length=2 in spec. Create value "XXXXXXXXXX" (10 chars) which should fail.
        """
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "EncounterID": ["E1", "E2"],
            "ADate": [1000, 2000],
            "EncType": ["IP", "XXXXXXXXXX"],  # Second value exceeds length=2
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("encounter")
        result = run_validation(reader, schema)

        assert not result.all_passed
        assert result.total_failures > 0
        # Verify the failure is in a step involving EncType
        enctype_steps = [s for s in result.steps if s.column == "EncType"]
        assert any(s.n_failed > 0 for s in enctype_steps)


class TestRunnerDetectsConditionalRuleViolation:
    def test_ddate_null_when_enctype_ip(self, tmp_path: Path) -> None:
        """AC2.5: Conditional rules fire correctly.

        When EncType=IP, DDate is required (conditional rule).
        Create a row with EncType='IP' but DDate=null which should fail.
        """
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "EncounterID": ["E1", "E2"],
            "ADate": [1000, 2000],
            "EncType": ["AV", "IP"],
            "DDate": [2000, None],  # Second row: EncType=IP but DDate is null (violation)
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("encounter")
        result = run_validation(reader, schema)

        assert not result.all_passed
        assert result.total_failures > 0
        # Verify the failure is in a conditional rule step for DDate
        conditional_steps = [s for s in result.steps if "conditional" in s.assertion_type.lower()]
        assert any(s.column == "DDate" and s.n_failed > 0 for s in conditional_steps)

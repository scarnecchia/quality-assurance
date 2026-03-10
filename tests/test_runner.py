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

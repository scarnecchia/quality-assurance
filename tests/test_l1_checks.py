from __future__ import annotations

from pathlib import Path

import polars as pl

from scdm_qa.readers import create_reader
from scdm_qa.schemas import get_schema
from scdm_qa.validation.runner import run_validation


class TestCheck122LeadingSpaces:
    """Test L1 Check 122: Flag values with leading whitespace."""

    def test_check_122_detects_leading_space(self, tmp_path: Path) -> None:
        """AC1.2: Check 122 flags character values with leading whitespace."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "TableID": ["C1", "C2", "C3"],
            "COD_DATE": [1000, 2000, 3000],
            "COD": ["E10", " E11", "E12"],  # Second value has leading space
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("cause_of_death")
        result = run_validation(reader, schema)

        # Find the check 122 step for COD column
        check_122_steps = [s for s in result.steps if s.check_id == "122" and s.column == "COD"]
        assert len(check_122_steps) > 0, "Expected at least one check 122 step for COD"
        assert check_122_steps[0].n_failed > 0, "Expected check 122 to detect leading space"

    def test_check_122_with_nulls_passes_na_pass(self, tmp_path: Path) -> None:
        """AC1.6: Null values in L1 check columns are not flagged (na_pass=True)."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "TableID": ["C1", "C2", "C3"],
            "COD_DATE": [1000, 2000, 3000],
            "COD": ["E10", None, "E12"],  # Second value is null
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("cause_of_death")
        result = run_validation(reader, schema)

        # Nulls should not contribute to failures (na_pass=True)
        check_122_steps = [s for s in result.steps if s.check_id == "122" and s.column == "COD"]
        assert len(check_122_steps) > 0
        # Since we have valid data and only null (which is skipped), all should pass
        assert check_122_steps[0].n_failed == 0

    def test_check_122_clean_data_passes(self, tmp_path: Path) -> None:
        """Test check 122 with clean data (no leading spaces)."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "TableID": ["C1", "C2", "C3"],
            "COD_DATE": [1000, 2000, 3000],
            "COD": ["E10", "E11", "E12"],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("cause_of_death")
        result = run_validation(reader, schema)

        check_122_steps = [s for s in result.steps if s.check_id == "122"]
        # All steps should pass for clean data
        assert all(s.n_failed == 0 for s in check_122_steps)


class TestCheck124UnexpectedZeros:
    """Test L1 Check 124: Flag numeric columns containing zero values."""

    def test_check_124_detects_zero(self, tmp_path: Path) -> None:
        """AC1.3: Check 124 flags numeric columns containing zero values."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "RxID": ["R1", "R2", "R3"],
            "RxDate": [1000, 2000, 3000],
            "RxSup": [30, 0, 60],  # Second value is zero (suspicious)
            "RxAmt": [100.0, 200.0, 300.0],
        })
        path = tmp_path / "dispensing.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("dispensing")
        result = run_validation(reader, schema)

        # Find the check 124 step for RxSup column
        check_124_steps = [s for s in result.steps if s.check_id == "124" and s.column == "RxSup"]
        assert len(check_124_steps) > 0, "Expected at least one check 124 step for RxSup"
        assert check_124_steps[0].n_failed > 0, "Expected check 124 to detect zero value"

    def test_check_124_with_nulls_passes_na_pass(self, tmp_path: Path) -> None:
        """AC1.6: Null values in L1 check columns are not flagged (na_pass=True)."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "RxID": ["R1", "R2", "R3"],
            "RxDate": [1000, 2000, 3000],
            "RxSup": [30, None, 60],  # Second value is null
            "RxAmt": [100.0, 200.0, 300.0],
        })
        path = tmp_path / "dispensing.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("dispensing")
        result = run_validation(reader, schema)

        # Nulls should not contribute to failures (na_pass=True)
        check_124_steps = [s for s in result.steps if s.check_id == "124" and s.column == "RxSup"]
        assert len(check_124_steps) > 0
        assert check_124_steps[0].n_failed == 0

    def test_check_124_clean_data_passes(self, tmp_path: Path) -> None:
        """Test check 124 with clean data (all positive values)."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "RxID": ["R1", "R2", "R3"],
            "RxDate": [1000, 2000, 3000],
            "RxSup": [30, 60, 90],
            "RxAmt": [100.0, 200.0, 300.0],
        })
        path = tmp_path / "dispensing.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("dispensing")
        result = run_validation(reader, schema)

        check_124_steps = [s for s in result.steps if s.check_id == "124"]
        # All steps should pass for clean data
        assert all(s.n_failed == 0 for s in check_124_steps)


class TestCheck128NonNumeric:
    """Test L1 Check 128: Flag non-numeric characters in PostalCode."""

    def test_check_128_detects_non_numeric(self, tmp_path: Path) -> None:
        """AC1.4: Check 128 flags PostalCode values containing non-numeric characters."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
            "PostalCode": ["12345", "K1A0B1", "67890"],  # Second value has non-numeric chars
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        result = run_validation(reader, schema)

        # Find the check 128 step for PostalCode column
        check_128_steps = [s for s in result.steps if s.check_id == "128" and s.column == "PostalCode"]
        assert len(check_128_steps) > 0, "Expected at least one check 128 step for PostalCode"
        assert check_128_steps[0].n_failed > 0, "Expected check 128 to detect non-numeric characters"

    def test_check_128_with_nulls_passes_na_pass(self, tmp_path: Path) -> None:
        """AC1.6: Null values in L1 check columns are not flagged (na_pass=True)."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
            "PostalCode": ["12345", None, "67890"],  # Second value is null
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        result = run_validation(reader, schema)

        # Nulls should not contribute to failures (na_pass=True)
        check_128_steps = [s for s in result.steps if s.check_id == "128" and s.column == "PostalCode"]
        assert len(check_128_steps) > 0
        assert check_128_steps[0].n_failed == 0

    def test_check_128_clean_data_passes(self, tmp_path: Path) -> None:
        """Test check 128 with clean data (all numeric)."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
            "PostalCode": ["12345", "67890", "13579"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        result = run_validation(reader, schema)

        check_128_steps = [s for s in result.steps if s.check_id == "128"]
        # All steps should pass for clean data
        assert all(s.n_failed == 0 for s in check_128_steps)


class TestL1CheckSeverity:
    """Test that L1 checks carry the correct severity levels (AC4.1)."""

    def test_check_122_has_correct_severity(self, tmp_path: Path) -> None:
        """AC4.1: Checks marked Warn in SAS reference produce correct severity."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "TableID": ["C1", "C2"],
            "COD_DATE": [1000, 2000],
            "COD": ["E10", " E11"],  # One with leading space
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("cause_of_death")
        result = run_validation(reader, schema)

        check_122_steps = [s for s in result.steps if s.check_id == "122"]
        assert len(check_122_steps) > 0
        assert check_122_steps[0].severity == "Warn"

    def test_check_124_has_correct_severity(self, tmp_path: Path) -> None:
        """AC4.1: Check 124 is marked Warn."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "RxID": ["R1", "R2"],
            "RxDate": [1000, 2000],
            "RxSup": [30, 0],  # One is zero
            "RxAmt": [100.0, 200.0],
        })
        path = tmp_path / "dispensing.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("dispensing")
        result = run_validation(reader, schema)

        check_124_steps = [s for s in result.steps if s.check_id == "124"]
        assert len(check_124_steps) > 0
        assert check_124_steps[0].severity == "Warn"

    def test_check_128_has_correct_severity(self, tmp_path: Path) -> None:
        """AC4.1: Check 128 is marked Warn."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
            "PostalCode": ["12345", "K1A0B1"],  # One has non-numeric
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        result = run_validation(reader, schema)

        check_128_steps = [s for s in result.steps if s.check_id == "128"]
        assert len(check_128_steps) > 0
        assert check_128_steps[0].severity == "Warn"

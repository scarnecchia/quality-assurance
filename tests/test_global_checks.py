from __future__ import annotations

from pathlib import Path
from unittest import mock

import polars as pl
import pytest

from scdm_qa.schemas import get_schema
from scdm_qa.validation.global_checks import check_sort_order, check_uniqueness


class TestUniquenessInMemory:
    def test_detects_duplicate_keys(self) -> None:
        schema = get_schema("demographic")
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
            pl.DataFrame({"PatID": ["P2", "P3"], "Birth_Date": [3, 4], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])

        result = check_uniqueness(
            Path("dummy.sas7bdat"),  # non-parquet forces in-memory path
            schema,
            chunks=chunks,
        )
        assert result is not None
        # P2 appears twice → 2 duplicate rows
        assert result.n_failed == 2

    def test_no_duplicates_passes(self) -> None:
        schema = get_schema("demographic")
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
            pl.DataFrame({"PatID": ["P3", "P4"], "Birth_Date": [3, 4], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])
        result = check_uniqueness(Path("dummy.sas7bdat"), schema, chunks=chunks)
        assert result is not None
        assert result.n_failed == 0

    def test_returns_none_for_table_without_unique_row(self) -> None:
        schema = get_schema("vital_signs")
        result = check_uniqueness(Path("dummy.parquet"), schema)
        assert result is None


class TestUniquenessDuckDB:
    def test_detects_duplicates_via_duckdb(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        df = pl.DataFrame({"PatID": ["P1", "P1", "P3"]})
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        schema = get_schema("demographic")
        result = check_uniqueness(path, schema)
        assert result is not None
        assert result.n_failed > 0

    def test_fallback_to_in_memory_when_duckdb_unavailable(self, tmp_path: Path) -> None:
        """Test graceful fallback when DuckDB is unavailable (AC5.3)."""
        df = pl.DataFrame({
            "PatID": ["P1", "P1", "P3"],
            "Birth_Date": [1, 2, 3],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "1"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        schema = get_schema("demographic")

        # Patch _uniqueness_duckdb to simulate unavailability (returns None)
        with mock.patch("scdm_qa.validation.global_checks._uniqueness_duckdb", return_value=None):
            chunks = iter([
                pl.DataFrame({
                    "PatID": ["P1", "P1"],
                    "Birth_Date": [1, 2],
                    "Sex": ["F", "M"],
                    "Hispanic": ["Y", "N"],
                    "Race": ["1", "2"],
                }),
                pl.DataFrame({
                    "PatID": ["P3"],
                    "Birth_Date": [3],
                    "Sex": ["F"],
                    "Hispanic": ["Y"],
                    "Race": ["1"],
                }),
            ])

            result = check_uniqueness(path, schema, chunks=chunks)
            assert result is not None
            # P1 appears twice → 2 duplicate rows detected via in-memory fallback
            assert result.n_failed == 2


class TestSortOrder:
    def test_detects_sort_break_at_boundary(self) -> None:
        schema = get_schema("demographic")
        # Chunk 1 ends with P3, chunk 2 starts with P1 — sort order break
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P3"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [3, 4], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])
        result = check_sort_order(schema, chunks)
        assert result is not None
        assert result.n_failed > 0

    def test_correctly_sorted_passes(self) -> None:
        schema = get_schema("demographic")
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
            pl.DataFrame({"PatID": ["P3", "P4"], "Birth_Date": [3, 4], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])
        result = check_sort_order(schema, chunks)
        assert result is not None
        assert result.n_failed == 0

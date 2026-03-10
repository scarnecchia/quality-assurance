from __future__ import annotations

from pathlib import Path
from unittest import mock

import polars as pl
import pytest

from scdm_qa.schemas import get_schema
from scdm_qa.schemas.checks import get_not_populated_checks_for_table
from scdm_qa.validation.global_checks import check_not_populated, check_sort_order, check_uniqueness


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


class TestGlobalCheckCheckIdNone:
    def test_uniqueness_check_has_check_id_none(self) -> None:
        schema = get_schema("demographic")
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])
        result = check_uniqueness(Path("dummy.sas7bdat"), schema, chunks=chunks)
        assert result is not None
        assert result.check_id is None
        assert result.severity is None

    def test_sort_order_check_has_check_id_none(self) -> None:
        schema = get_schema("demographic")
        chunks = iter([
            pl.DataFrame({"PatID": ["P1", "P2"], "Birth_Date": [1, 2], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
            pl.DataFrame({"PatID": ["P3", "P4"], "Birth_Date": [3, 4], "Sex": ["F", "M"], "Hispanic": ["Y", "N"], "Race": ["1", "2"]}),
        ])
        result = check_sort_order(schema, chunks)
        assert result is not None
        assert result.check_id is None
        assert result.severity is None


class TestNotPopulated:
    def test_detects_entirely_null_column(self) -> None:
        """Test AC1.1: Check 111 flags columns with zero non-null records as not populated."""
        schema = get_schema("encounter")
        # DDate is a check-111 target column. All nulls → should fail.
        chunks = iter([
            pl.DataFrame({
                "EncounterID": ["E1", "E2"],
                "PatID": ["P1", "P2"],
                "EncounterDate": [1000, 2000],
                "EncounterType": ["IP", "OP"],
                "DDate": [None, None],
                "Discharge_Disposition": [None, None],
                "Discharge_Status": [None, None],
                "Admitting_Source": [None, None],
            }),
            pl.DataFrame({
                "EncounterID": ["E3"],
                "PatID": ["P3"],
                "EncounterDate": [3000],
                "EncounterType": ["ED"],
                "DDate": [None],
                "Discharge_Disposition": [None],
                "Discharge_Status": [None],
                "Admitting_Source": [None],
            }),
        ])
        results = check_not_populated(schema, chunks)

        # Should have results for DDate, Discharge_Disposition, Discharge_Status, Admitting_Source
        assert len(results) == 4

        ddate_result = next(r for r in results if r.column == "DDate")
        assert ddate_result.n_failed == 3  # 3 total rows, all failed
        assert ddate_result.n_passed == 0
        assert ddate_result.check_id == "111"

    def test_populated_column_passes(self) -> None:
        """Test AC1.1: Check 111 passes when target column has at least one non-null value."""
        schema = get_schema("encounter")
        chunks = iter([
            pl.DataFrame({
                "EncounterID": ["E1", "E2"],
                "PatID": ["P1", "P2"],
                "EncounterDate": [1000, 2000],
                "EncounterType": ["IP", "OP"],
                "DDate": [1001, None],  # At least one non-null
                "Discharge_Disposition": ["1", None],
                "Discharge_Status": ["A", None],
                "Admitting_Source": ["01", None],
            }),
        ])
        results = check_not_populated(schema, chunks)

        # DDate has one non-null → should pass
        ddate_result = next(r for r in results if r.column == "DDate")
        assert ddate_result.n_failed == 0
        assert ddate_result.n_passed == 2

    def test_only_registry_columns_checked(self) -> None:
        """Test AC1.5: Only check-111 target columns are checked, not all columns."""
        schema = get_schema("demographic")
        # Race is NOT a check-111 target, but is entirely null
        # ImputedHispanic and ImputedRace ARE check-111 targets
        chunks = iter([
            pl.DataFrame({
                "PatID": ["P1", "P2"],
                "Birth_Date": [1000, 2000],
                "Sex": ["F", "M"],
                "Hispanic": ["Y", "N"],
                "Race": [None, None],  # All null but NOT a check-111 target
                "ImputedHispanic": ["Y", "N"],  # Check-111 target, populated
                "ImputedRace": [None, None],  # Check-111 target, not populated
            }),
        ])
        results = check_not_populated(schema, chunks)

        # Should have 2 results (ImputedHispanic and ImputedRace), not 3
        assert len(results) == 2

        # Race should NOT appear in results
        race_results = [r for r in results if r.column == "Race"]
        assert len(race_results) == 0

        # ImputedRace should fail
        imputed_race = next(r for r in results if r.column == "ImputedRace")
        assert imputed_race.n_failed == 2

    def test_no_check_defs_returns_empty_list(self) -> None:
        """Test that tables with no check-111 definitions return empty list."""
        schema = get_schema("vital_signs")  # No check-111 defs
        chunks = iter([
            pl.DataFrame({
                "EncounterID": ["E1"],
                "VitalsDate": [1000],
                "HT": [None],
            }),
        ])
        results = check_not_populated(schema, chunks)
        assert results == []

    def test_check_id_is_111(self) -> None:
        """Test AC4.1: Check-111 results have check_id='111'."""
        schema = get_schema("encounter")
        chunks = iter([
            pl.DataFrame({
                "EncounterID": ["E1"],
                "PatID": ["P1"],
                "EncounterDate": [1000],
                "EncounterType": ["IP"],
                "DDate": [None],
                "Discharge_Disposition": [None],
                "Discharge_Status": [None],
                "Admitting_Source": [None],
            }),
        ])
        results = check_not_populated(schema, chunks)

        for result in results:
            assert result.check_id == "111"

    def test_severity_from_registry(self) -> None:
        """Test AC4.2: Severity field matches the check registry definitions."""
        schema = get_schema("demographic")
        chunks = iter([
            pl.DataFrame({
                "PatID": ["P1"],
                "Birth_Date": [1000],
                "Sex": ["F"],
                "Hispanic": ["Y"],
                "Race": ["1"],
                "ImputedHispanic": [None],
                "ImputedRace": [None],
            }),
        ])
        results = check_not_populated(schema, chunks)

        # Get check defs to verify severity values
        check_defs = get_not_populated_checks_for_table("demographic")

        for result in results:
            check_def = next(c for c in check_defs if c.column == result.column)
            assert result.severity == check_def.severity

    def test_multiple_chunks_accumulation(self) -> None:
        """Test that non-null counts accumulate correctly across multiple chunks."""
        schema = get_schema("encounter")
        chunks = iter([
            pl.DataFrame({
                "EncounterID": ["E1", "E2"],
                "PatID": ["P1", "P2"],
                "EncounterDate": [1000, 2000],
                "EncounterType": ["IP", "OP"],
                "DDate": [None, 1001],  # One null, one value
                "Discharge_Disposition": [None, None],
                "Discharge_Status": [None, None],
                "Admitting_Source": [None, None],
            }),
            pl.DataFrame({
                "EncounterID": ["E3"],
                "PatID": ["P3"],
                "EncounterDate": [3000],
                "EncounterType": ["ED"],
                "DDate": [None],
                "Discharge_Disposition": [None],
                "Discharge_Status": [None],
                "Admitting_Source": [None],
            }),
        ])
        results = check_not_populated(schema, chunks)

        # 3 total rows, DDate has at least 1 non-null → entire column is populated
        # n_passed = total_rows = 3, n_failed = 0
        ddate_result = next(r for r in results if r.column == "DDate")
        assert ddate_result.n_passed == 3
        assert ddate_result.n_failed == 0

        # But Discharge_Disposition has all nulls → entire column is not populated
        disposition_result = next(r for r in results if r.column == "Discharge_Disposition")
        assert disposition_result.n_passed == 0
        assert disposition_result.n_failed == 3

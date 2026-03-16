from __future__ import annotations

from pathlib import Path
import polars as pl
import pytest

from scdm_qa.schemas import get_schema
from scdm_qa.schemas.checks import get_date_ordering_checks_for_table, get_not_populated_checks_for_table
from scdm_qa.validation.duckdb_utils import create_connection
from scdm_qa.validation.global_checks import (
    check_cause_of_death,
    check_date_ordering,
    check_enc_combinations,
    check_enrollment_gaps,
    check_not_populated,
    check_overlapping_spans,
    check_sort_order,
    check_uniqueness,
)


class TestUniqueness:
    def test_detects_duplicate_keys(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P2"],
            "Birth_Date": [1, 2, 3],
            "Sex": ["F", "M", "M"],
            "Hispanic": ["Y", "N", "N"],
            "Race": ["1", "2", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_uniqueness(conn, "demographic", schema)
            assert result is not None
            # P2 appears twice → 2 duplicate rows
            assert result.n_failed == 2
        finally:
            conn.close()

    def test_no_duplicates_passes(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1, 2, 3],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "1"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_uniqueness(conn, "demographic", schema)
            assert result is not None
            assert result.n_failed == 0
            assert result.n_passed == 3
        finally:
            conn.close()

    def test_returns_none_for_table_without_unique_row(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("vital_signs")
        df = pl.DataFrame({"PatID": ["P1", "P2"], "VitalType": ["BP", "HR"]})
        path = tmp_path / "vital_signs.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "vital_signs" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_uniqueness(conn, "vital_signs", schema)
            assert result is None
        finally:
            conn.close()

    def test_check_id_211_and_severity_fail(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_uniqueness(conn, "demographic", schema)
            assert result is not None
            assert result.check_id == "211"
            assert result.severity == "Fail"
        finally:
            conn.close()

    def test_multiple_duplicates(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1", "P1", "P1", "P2", "P2"],
            "Birth_Date": [1, 1, 1, 2, 2],
            "Sex": ["F", "F", "F", "M", "M"],
            "Hispanic": ["Y", "Y", "Y", "N", "N"],
            "Race": ["1", "1", "1", "2", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_uniqueness(conn, "demographic", schema)
            assert result is not None
            # P1 has 3 duplicate rows, P2 has 2 duplicate rows
            assert result.n_failed == 5
            assert result.n_passed == 0
        finally:
            conn.close()


class TestSortOrder:
    def test_intra_chunk_sort_violation_detected(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        # Single chunk with rows [P3, P1, P2] — violates sort order
        df = pl.DataFrame({
            "PatID": ["P3", "P1", "P2"],
            "Birth_Date": [2, 1, 3],
            "Sex": ["M", "F", "M"],
            "Hispanic": ["N", "Y", "N"],
            "Race": ["2", "1", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_sort_order(conn, "demographic", schema)
            assert result is not None
            # P1 row violates sort (P3 > P1)
            assert result.n_failed > 0
            assert result.check_id == "102"
        finally:
            conn.close()

    def test_correctly_sorted_passes(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        # Correctly sorted rows [P1, P2, P3]
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1, 2, 3],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "1"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_sort_order(conn, "demographic", schema)
            assert result is not None
            assert result.n_failed == 0
            assert result.n_passed == 3
        finally:
            conn.close()

    def test_equal_adjacent_rows_pass(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        # Equal adjacent rows: [P1, P1, P2] — should pass (equal is not a violation)
        df = pl.DataFrame({
            "PatID": ["P1", "P1", "P2"],
            "Birth_Date": [1, 1, 2],
            "Sex": ["F", "F", "M"],
            "Hispanic": ["Y", "Y", "N"],
            "Race": ["1", "1", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_sort_order(conn, "demographic", schema)
            assert result is not None
            assert result.n_failed == 0
            assert result.check_id == "102"
            assert result.severity == "Fail"
        finally:
            conn.close()


class TestGlobalCheckCheckIds:
    def test_uniqueness_check_has_check_id_211(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1, 2],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_uniqueness(conn, "demographic", schema)
            assert result is not None
            assert result.check_id == "211"
            assert result.severity == "Fail"
        finally:
            conn.close()

    def test_sort_order_check_has_check_id_102(self, tmp_path: Path) -> None:
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3", "P4"],
            "Birth_Date": [1, 2, 3, 4],
            "Sex": ["F", "M", "F", "M"],
            "Hispanic": ["Y", "N", "Y", "N"],
            "Race": ["1", "2", "1", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_sort_order(conn, "demographic", schema)
            assert result is not None
            assert result.check_id == "102"
            assert result.severity == "Fail"
        finally:
            conn.close()


class TestNotPopulated:
    def test_detects_entirely_null_column(self, tmp_path: Path) -> None:
        """Test AC1.1: Check 111 flags columns with zero non-null records as not populated."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        # DDate is a check-111 target column. All nulls → should fail.
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2", "E3"],
            "PatID": ["P1", "P2", "P3"],
            "EncounterDate": [1000, 2000, 3000],
            "EncounterType": ["IP", "OP", "ED"],
            "DDate": [None, None, None],
            "Discharge_Disposition": [None, None, None],
            "Discharge_Status": [None, None, None],
            "Admitting_Source": [None, None, None],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_not_populated(conn, "encounter", schema)

            # Should have results for DDate, Discharge_Disposition, Discharge_Status, Admitting_Source
            assert len(results) == 4

            ddate_result = next(r for r in results if r.column == "DDate")
            assert ddate_result.n_failed == 3  # 3 total rows, all failed
            assert ddate_result.n_passed == 0
            assert ddate_result.check_id == "111"
        finally:
            conn.close()

    def test_populated_column_passes(self, tmp_path: Path) -> None:
        """Test AC1.1: Check 111 passes when target column has at least one non-null value."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2"],
            "PatID": ["P1", "P2"],
            "EncounterDate": [1000, 2000],
            "EncounterType": ["IP", "OP"],
            "DDate": [1001, None],  # At least one non-null
            "Discharge_Disposition": ["1", None],
            "Discharge_Status": ["A", None],
            "Admitting_Source": ["01", None],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_not_populated(conn, "encounter", schema)

            # DDate has one non-null → should pass
            ddate_result = next(r for r in results if r.column == "DDate")
            assert ddate_result.n_failed == 0
            assert ddate_result.n_passed == 2
        finally:
            conn.close()

    def test_only_registry_columns_checked(self, tmp_path: Path) -> None:
        """Test AC1.5: Only check-111 target columns are checked, not all columns."""
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        # Race is NOT a check-111 target, but is entirely null
        # ImputedHispanic and ImputedRace ARE check-111 targets
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": [None, None],  # All null but NOT a check-111 target
            "ImputedHispanic": ["Y", "N"],  # Check-111 target, populated
            "ImputedRace": [None, None],  # Check-111 target, not populated
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_not_populated(conn, "demographic", schema)

            # Should have 2 results (ImputedHispanic and ImputedRace), not 3
            assert len(results) == 2

            # Race should NOT appear in results
            race_results = [r for r in results if r.column == "Race"]
            assert len(race_results) == 0

            # ImputedRace should fail
            imputed_race = next(r for r in results if r.column == "ImputedRace")
            assert imputed_race.n_failed == 2
        finally:
            conn.close()

    def test_no_check_defs_returns_empty_list(self, tmp_path: Path) -> None:
        """Test that tables with no check-111 definitions return empty list."""
        pytest.importorskip("duckdb")
        schema = get_schema("vital_signs")  # No check-111 defs
        df = pl.DataFrame({
            "EncounterID": ["E1"],
            "VitalsDate": [1000],
            "HT": [None],
        })
        path = tmp_path / "vital_signs.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "vital_signs" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_not_populated(conn, "vital_signs", schema)
            assert results == []
        finally:
            conn.close()

    def test_check_id_is_111(self, tmp_path: Path) -> None:
        """Test AC4.1: Check-111 results have check_id='111'."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1"],
            "PatID": ["P1"],
            "EncounterDate": [1000],
            "EncounterType": ["IP"],
            "DDate": [None],
            "Discharge_Disposition": [None],
            "Discharge_Status": [None],
            "Admitting_Source": [None],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_not_populated(conn, "encounter", schema)

            for result in results:
                assert result.check_id == "111"
        finally:
            conn.close()

    def test_severity_from_registry(self, tmp_path: Path) -> None:
        """Test AC4.2: Severity field matches the check registry definitions."""
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
            "ImputedHispanic": [None],
            "ImputedRace": [None],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_not_populated(conn, "demographic", schema)

            # Get check defs to verify severity values
            check_defs = get_not_populated_checks_for_table("demographic")

            # Explicitly assert expected severity values from registry
            imputed_hispanic_def = next(c for c in check_defs if c.column == "ImputedHispanic")
            imputed_race_def = next(c for c in check_defs if c.column == "ImputedRace")
            assert imputed_hispanic_def.severity == "Note"
            assert imputed_race_def.severity == "Note"

            # Verify results match the registry definitions
            for result in results:
                check_def = next(c for c in check_defs if c.column == result.column)
                assert result.severity == check_def.severity
        finally:
            conn.close()

    def test_single_sql_query_per_column(self, tmp_path: Path) -> None:
        """Test that DuckDB SQL aggregation happens at once, not per-chunk."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2", "E3"],
            "PatID": ["P1", "P2", "P3"],
            "EncounterDate": [1000, 2000, 3000],
            "EncounterType": ["IP", "OP", "ED"],
            "DDate": [None, 1001, None],  # One non-null
            "Discharge_Disposition": [None, None, None],
            "Discharge_Status": [None, None, None],
            "Admitting_Source": [None, None, None],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_not_populated(conn, "encounter", schema)

            # 3 total rows, DDate has at least 1 non-null → entire column is populated
            ddate_result = next(r for r in results if r.column == "DDate")
            assert ddate_result.n_passed == 3
            assert ddate_result.n_failed == 0

            # But Discharge_Disposition has all nulls → entire column is not populated
            disposition_result = next(r for r in results if r.column == "Discharge_Disposition")
            assert disposition_result.n_passed == 0
            assert disposition_result.n_failed == 3
        finally:
            conn.close()


class TestDateOrdering:
    """Test suite for check_date_ordering (L2 check 226)."""

    def test_detects_violations_adate_greater_than_ddate(self, tmp_path: Path) -> None:
        """Test AC2.1: Check 226 flags rows where ADate > DDate."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2", "E3"],
            "PatID": ["P1", "P2", "P3"],
            "EncounterDate": [1000, 2000, 3000],
            "EncounterType": ["IP", "OP", "ED"],
            "ADate": [1100, 1050, 1200],  # E1: 1100 > 1050, E3: 1200 > 1050
            "DDate": [1050, 2000, 1050],
            "Discharge_Disposition": ["1", "1", "1"],
            "Discharge_Status": ["A", "A", "A"],
            "Admitting_Source": ["01", "01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema)

            assert len(results) == 1
            assert results[0].check_id == "226"
            assert results[0].column == "ADate, DDate"
            assert results[0].assertion_type == "date_ordering"
            assert results[0].n_failed == 2  # Two rows violate ordering
            assert results[0].n_passed == 1  # One row is valid
        finally:
            conn.close()

    def test_skips_rows_with_null_dates(self, tmp_path: Path) -> None:
        """Test AC2.6: Check 226 does not flag rows where either date is null."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2", "E3", "E4"],
            "PatID": ["P1", "P2", "P3", "P4"],
            "EncounterDate": [1000, 2000, 3000, 4000],
            "EncounterType": ["IP", "OP", "ED", "IP"],
            "ADate": [1100, None, 1200, 1050],  # E2: null ADate, E3 would violate but let's test
            "DDate": [1050, 2000, 1050, 1050],  # E4: ADate <= DDate (valid)
            "Discharge_Disposition": ["1", "1", "1", "1"],
            "Discharge_Status": ["A", "A", "A", "A"],
            "Admitting_Source": ["01", "01", "01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema)

            # E1 violates, E2 skipped (null ADate), E3 violates, E4 passes
            # But we're testing that nulls are skipped
            assert len(results) == 1
            # Only rows with BOTH dates non-null are counted
            # E1, E3, E4 have both dates non-null
            # E2 has null ADate so not counted
            assert results[0].n_passed + results[0].n_failed == 3  # Only 3 rows with both dates
        finally:
            conn.close()

    def test_clean_data_passes(self, tmp_path: Path) -> None:
        """Test that data with all ADate <= DDate passes."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2", "E3"],
            "PatID": ["P1", "P2", "P3"],
            "EncounterDate": [1000, 2000, 3000],
            "EncounterType": ["IP", "OP", "ED"],
            "ADate": [1000, 1500, 2500],
            "DDate": [1050, 2000, 3000],
            "Discharge_Disposition": ["1", "1", "1"],
            "Discharge_Status": ["A", "A", "A"],
            "Admitting_Source": ["01", "01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema)

            assert len(results) == 1
            assert results[0].n_failed == 0
            assert results[0].n_passed == 3
        finally:
            conn.close()

    def test_multiple_chunks_accumulation(self, tmp_path: Path) -> None:
        """Test that violations are detected across all rows in a single view."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2", "E3", "E4"],
            "PatID": ["P1", "P2", "P3", "P4"],
            "EncounterDate": [1000, 2000, 3000, 4000],
            "EncounterType": ["IP", "OP", "ED", "IP"],
            "ADate": [1100, 1050, 1200, 1050],  # E1 and E3 violate
            "DDate": [1050, 2000, 1050, 2000],
            "Discharge_Disposition": ["1", "1", "1", "1"],
            "Discharge_Status": ["A", "A", "A", "A"],
            "Admitting_Source": ["01", "01", "01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema)

            assert len(results) == 1
            assert results[0].n_failed == 2  # E1 and E3 violate
            assert results[0].n_passed == 2  # E2 and E4 pass
        finally:
            conn.close()

    def test_enrollment_date_ordering(self, tmp_path: Path) -> None:
        """Test date ordering check for enrollment table (Enr_Start <= Enr_End)."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "PlanID": ["PL1", "PL2", "PL3"],
            "Enr_Start": [1000, 1500, 2000],
            "Enr_End": [1500, 1400, 2500],  # P2: 1500 > 1400 (violates)
            "PlanType": ["HMO", "HMO", "PPO"],
            "PayerType": ["Commercial", "Commercial", "Medicare"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "enrollment", schema)

            assert len(results) == 1
            assert results[0].check_id == "226"
            assert results[0].column == "Enr_Start, Enr_End"
            assert results[0].n_failed == 1
            assert results[0].n_passed == 2
        finally:
            conn.close()

    def test_no_date_ordering_defs_returns_empty_list(self, tmp_path: Path) -> None:
        """Test that tables with no date ordering checks return empty list."""
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")  # No date ordering defs
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "demographic", schema)
            assert results == []
        finally:
            conn.close()

    def test_check_id_is_226(self, tmp_path: Path) -> None:
        """Test AC4.1: All returned StepResults have check_id='226'."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1"],
            "PatID": ["P1"],
            "EncounterDate": [1000],
            "EncounterType": ["IP"],
            "ADate": [1100],
            "DDate": [1050],
            "Discharge_Disposition": ["1"],
            "Discharge_Status": ["A"],
            "Admitting_Source": ["01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema)

            assert len(results) > 0
            for result in results:
                assert result.check_id == "226"
        finally:
            conn.close()

    def test_severity_from_registry(self, tmp_path: Path) -> None:
        """Test AC4.1: Severity matches registry definitions."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1"],
            "PatID": ["P1"],
            "EncounterDate": [1000],
            "EncounterType": ["IP"],
            "ADate": [1000],
            "DDate": [1000],
            "Discharge_Disposition": ["1"],
            "Discharge_Status": ["A"],
            "Admitting_Source": ["01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema)

            # Get registry definitions to verify severity
            check_defs = get_date_ordering_checks_for_table("encounter")
            assert len(check_defs) > 0

            # Verify each result has matching severity from registry
            for result in results:
                check_def = next(c for c in check_defs if c.date_a == "ADate" and c.date_b == "DDate")
                assert result.severity == check_def.severity
                assert result.severity == "Fail"  # From registry
        finally:
            conn.close()

    def test_date_pair_count_for_encounter(self, tmp_path: Path) -> None:
        """Test that encounter table has one date ordering pair."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1"],
            "PatID": ["P1"],
            "EncounterDate": [1000],
            "EncounterType": ["IP"],
            "ADate": [1000],
            "DDate": [1000],
            "Discharge_Disposition": ["1"],
            "Discharge_Status": ["A"],
            "Admitting_Source": ["01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema)

            # Encounter only has one date ordering pair (ADate > DDate)
            assert len(results) == 1
            assert results[0].column == "ADate, DDate"
        finally:
            conn.close()

    def test_date_pair_count_for_enrollment(self, tmp_path: Path) -> None:
        """Test that enrollment table has one date ordering pair."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "PlanID": ["PL1"],
            "Enr_Start": [1000],
            "Enr_End": [1000],
            "PlanType": ["HMO"],
            "PayerType": ["Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "enrollment", schema)

            # Enrollment only has one date ordering pair (Enr_Start > Enr_End)
            assert len(results) == 1
            assert results[0].column == "Enr_Start, Enr_End"
        finally:
            conn.close()

    def test_failing_rows_sampled(self, tmp_path: Path) -> None:
        """Test that failing rows are captured and sampled correctly."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2", "E3"],
            "PatID": ["P1", "P2", "P3"],
            "EncounterDate": [1000, 2000, 3000],
            "EncounterType": ["IP", "OP", "ED"],
            "ADate": [1100, 1100, 1100],  # All violate
            "DDate": [1050, 1050, 1050],
            "Discharge_Disposition": ["1", "1", "1"],
            "Discharge_Status": ["A", "A", "A"],
            "Admitting_Source": ["01", "01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema)

            assert len(results) == 1
            assert results[0].n_failed == 3
            assert results[0].failing_rows is not None
            assert results[0].failing_rows.height == 3
        finally:
            conn.close()

    def test_failing_rows_respects_max_failing_rows(self, tmp_path: Path) -> None:
        """Test that failing row samples are bounded by max_failing_rows."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        # Create many violations
        df = pl.DataFrame({
            "EncounterID": [f"E{i}" for i in range(10)],
            "PatID": [f"P{i}" for i in range(10)],
            "EncounterDate": [1000 + i * 100 for i in range(10)],
            "EncounterType": ["IP"] * 10,
            "ADate": [1100] * 10,  # All violate
            "DDate": [1050] * 10,
            "Discharge_Disposition": ["1"] * 10,
            "Discharge_Status": ["A"] * 10,
            "Admitting_Source": ["01"] * 10,
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema, max_failing_rows=5)

            assert len(results) == 1
            assert results[0].n_failed == 10  # All 10 fail
            assert results[0].failing_rows is not None
            assert results[0].failing_rows.height == 5  # But sampled to 5
        finally:
            conn.close()

    def test_null_adate_and_null_ddate_both_skipped(self, tmp_path: Path) -> None:
        """Test that rows with null ADate, null DDate, or both are skipped."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2", "E3", "E4", "E5"],
            "PatID": ["P1", "P2", "P3", "P4", "P5"],
            "EncounterDate": [1000, 2000, 3000, 4000, 5000],
            "EncounterType": ["IP", "OP", "ED", "IP", "OP"],
            "ADate": [1100, None, 1200, None, 1050],  # E2 and E4 have null ADate
            "DDate": [1050, 2000, None, None, 1050],  # E3 and E4 have null DDate
            "Discharge_Disposition": ["1", "1", "1", "1", "1"],
            "Discharge_Status": ["A", "A", "A", "A", "A"],
            "Admitting_Source": ["01", "01", "01", "01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_date_ordering(conn, "encounter", schema)

            assert len(results) == 1
            # Only E1 and E5 have both dates non-null
            # E1: 1100 > 1050 (violates), E5: 1050 <= 1050 (passes)
            assert results[0].n_passed + results[0].n_failed == 2
            assert results[0].n_failed == 1
            assert results[0].n_passed == 1
        finally:
            conn.close()


class TestCauseOfDeath:
    """Test suite for check_cause_of_death (L2 checks 236, 237)."""

    def test_check_236_detects_missing_underlying_cause(self, tmp_path: Path) -> None:
        """Test AC2.3: Check 236 flags patients with no CauseType='U' record."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        # Patient P1 has CauseType='C' and 'I' but no 'U'
        df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "CauseType": ["C", "I"],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)

            assert len(results) == 2
            result_236 = results[0]
            assert result_236.check_id == "236"
            assert result_236.n_failed >= 1
            assert result_236.assertion_type == "cause_of_death"
        finally:
            conn.close()

    def test_check_237_detects_multiple_underlying_causes(self, tmp_path: Path) -> None:
        """Test AC2.7: Check 237 flags patients with >1 CauseType='U' records."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        # Patient P3 has two CauseType='U' records
        df = pl.DataFrame({
            "PatID": ["P3", "P3"],
            "CauseType": ["U", "U"],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)

            assert len(results) == 2
            result_237 = results[1]
            assert result_237.check_id == "237"
            assert result_237.n_failed >= 1
            assert result_237.assertion_type == "cause_of_death"
        finally:
            conn.close()

    def test_exactly_one_u_passes_both_checks(self, tmp_path: Path) -> None:
        """Test AC2.7: Patient with exactly one 'U' and other types passes both checks."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        # Patient P2 has exactly one CauseType='U' plus other types
        df = pl.DataFrame({
            "PatID": ["P2", "P2", "P2"],
            "CauseType": ["U", "C", "I"],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)

            assert len(results) == 2
            result_236 = results[0]
            result_237 = results[1]
            # P2 passes check 236 (has one 'U')
            assert result_236.n_failed == 0
            # P2 passes check 237 (has exactly one 'U', not more)
            assert result_237.n_failed == 0
        finally:
            conn.close()

    def test_check_ids_are_236_and_237(self, tmp_path: Path) -> None:
        """Test AC4.1: Results have correct check_id values."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "CauseType": ["C"],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)

            assert len(results) == 2
            assert results[0].check_id == "236"
            assert results[1].check_id == "237"
        finally:
            conn.close()

    def test_severity_is_fail(self, tmp_path: Path) -> None:
        """Test AC4.1: Both results have severity='Fail'."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "CauseType": ["C"],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)

            assert len(results) == 2
            assert results[0].severity == "Fail"
            assert results[1].severity == "Fail"
        finally:
            conn.close()

    def test_returns_empty_list_for_non_cod_table(self, tmp_path: Path) -> None:
        """Test that non-cause_of_death tables return empty list."""
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "demographic", schema)
            assert results == []
        finally:
            conn.close()

    def test_multiple_chunks_accumulation(self, tmp_path: Path) -> None:
        """Test that records are aggregated correctly across all rows in a single view."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        # All records in one table
        df = pl.DataFrame({
            "PatID": ["P1", "P1", "P2", "P2"],
            "CauseType": ["U", "C", "C", "I"],  # P1 has one 'U', P2 has no 'U'
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)

            assert len(results) == 2
            result_236 = results[0]
            result_237 = results[1]
            # Total patients: 2 (P1 and P2)
            # Check 236: P2 fails (no 'U'), P1 passes
            assert result_236.n_failed == 1
            assert result_236.n_passed == 1
            # Check 237: both pass (P1 has 1 'U', P2 has 0 'U')
            assert result_237.n_failed == 0
            assert result_237.n_passed == 2
        finally:
            conn.close()

    def test_multiple_patients_with_various_scenarios(self, tmp_path: Path) -> None:
        """Test mixed scenarios: missing U, multiple U, correct U."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        df = pl.DataFrame({
            "PatID": ["P1", "P1", "P2", "P2", "P2", "P3", "P3"],
            "CauseType": ["C", "I", "U", "C", "I", "U", "U"],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)

            assert len(results) == 2
            result_236 = results[0]
            result_237 = results[1]
            # P1: no 'U' (fails 236)
            # P2: one 'U' (passes both)
            # P3: two 'U' (fails 237)
            assert result_236.n_failed == 1  # Only P1
            assert result_236.n_passed == 2  # P2 and P3
            assert result_237.n_failed == 1  # Only P3
            assert result_237.n_passed == 2  # P1 and P2
        finally:
            conn.close()

    def test_failing_rows_are_capped_at_max_failing_rows(self, tmp_path: Path) -> None:
        """Test that failing row samples are bounded by max_failing_rows."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        # Create 10 patients with no 'U'
        df = pl.DataFrame({
            "PatID": [f"P{i}" for i in range(10)],
            "CauseType": ["C"] * 10,
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema, max_failing_rows=5)

            assert len(results) == 2
            result_236 = results[0]
            # All 10 patients fail check 236
            assert result_236.n_failed == 10
            # But failing_rows is sampled to max_failing_rows
            assert result_236.failing_rows is not None
            assert result_236.failing_rows.height == 5
        finally:
            conn.close()

    def test_empty_dataframe_returns_zero_counts(self, tmp_path: Path) -> None:
        """Test that empty dataframe returns results with zero passed/failed."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        df = pl.DataFrame({
            "PatID": [],
            "CauseType": [],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)
            # Empty table: 0 patients total, so 0 passed and 0 failed for both checks
            assert len(results) == 2
            assert results[0].n_passed == 0
            assert results[0].n_failed == 0
            assert results[1].n_passed == 0
            assert results[1].n_failed == 0
        finally:
            conn.close()

    def test_cause_of_death_returns_two_results(self, tmp_path: Path) -> None:
        """Test that cause_of_death checks return two results."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "CauseType": ["C"],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)
            assert len(results) == 2
        finally:
            conn.close()

    def test_column_names_are_correct(self, tmp_path: Path) -> None:
        """Test that results reference the CauseType column."""
        pytest.importorskip("duckdb")
        schema = get_schema("cause_of_death")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "CauseType": ["C"],
        })
        path = tmp_path / "cause_of_death.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "cause_of_death" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_cause_of_death(conn, "cause_of_death", schema)

            assert len(results) == 2
            assert results[0].column == "CauseType"
            assert results[1].column == "CauseType"
        finally:
            conn.close()


class TestOverlappingSpans:
    """Test suite for check_overlapping_spans (L2 check 215)."""

    def test_detects_overlapping_spans(self, tmp_path: Path) -> None:
        """Test AC2.2: Check 215 flags overlapping enrollment spans within same patient."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # Patient P1 has two overlapping spans: [100, 200] and [150, 300]
        df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "PlanID": ["PL1", "PL2"],
            "Enr_Start": [100, 150],
            "Enr_End": [200, 300],
            "PlanType": ["HMO", "HMO"],
            "PayerType": ["Commercial", "Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_overlapping_spans(conn, "enrollment", schema)
            assert result is not None
            assert result.check_id == "215"
            assert result.n_failed > 0
            assert result.assertion_type == "overlapping_spans"
        finally:
            conn.close()

    def test_non_overlapping_spans_pass(self, tmp_path: Path) -> None:
        """Test that non-overlapping spans pass."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # Patient P1 has non-overlapping spans: [100, 200] and [201, 300]
        df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "PlanID": ["PL1", "PL2"],
            "Enr_Start": [100, 201],
            "Enr_End": [200, 300],
            "PlanType": ["HMO", "HMO"],
            "PayerType": ["Commercial", "Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_overlapping_spans(conn, "enrollment", schema)
            assert result is not None
            assert result.n_failed == 0
            assert result.n_passed == 2
        finally:
            conn.close()

    def test_returns_none_for_non_enrollment_table(self, tmp_path: Path) -> None:
        """Test that non-enrollment tables return None."""
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_overlapping_spans(conn, "demographic", schema)
            assert result is None
        finally:
            conn.close()

    def test_check_id_215_and_severity_fail(self, tmp_path: Path) -> None:
        """Test AC4.1: Check 215 has correct check_id and severity."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "PlanID": ["PL1"],
            "Enr_Start": [100],
            "Enr_End": [200],
            "PlanType": ["HMO"],
            "PayerType": ["Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_overlapping_spans(conn, "enrollment", schema)
            assert result is not None
            assert result.check_id == "215"
            assert result.severity == "Fail"
        finally:
            conn.close()

    def test_multiple_patients_with_overlaps(self, tmp_path: Path) -> None:
        """Test overlap detection across multiple patients."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # P1 has overlapping spans, P2 has non-overlapping
        df = pl.DataFrame({
            "PatID": ["P1", "P1", "P2", "P2"],
            "PlanID": ["PL1", "PL2", "PL3", "PL4"],
            "Enr_Start": [100, 150, 500, 600],
            "Enr_End": [200, 300, 550, 700],
            "PlanType": ["HMO", "HMO", "PPO", "PPO"],
            "PayerType": ["Commercial", "Commercial", "Medicare", "Medicare"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_overlapping_spans(conn, "enrollment", schema)
            assert result is not None
            # P1 has 1 overlapping row (the second span), P2 has 0
            assert result.n_failed == 1
            assert result.n_passed == 3
        finally:
            conn.close()

    def test_overlaps_across_multiple_rows(self, tmp_path: Path) -> None:
        """Test overlap detection with multiple overlapping rows."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # P1 has multiple overlapping rows
        df = pl.DataFrame({
            "PatID": ["P1", "P1", "P1"],
            "PlanID": ["PL1", "PL2", "PL3"],
            "Enr_Start": [100, 150, 200],
            "Enr_End": [200, 300, 350],
            "PlanType": ["HMO", "HMO", "HMO"],
            "PayerType": ["Commercial", "Commercial", "Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_overlapping_spans(conn, "enrollment", schema)
            assert result is not None
            # PL2 and PL3 are overlapping with previous spans
            assert result.n_failed > 0
        finally:
            conn.close()

    def test_date_type_handling(self, tmp_path: Path) -> None:
        """Test that both Date and integer dtypes are handled correctly."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # Use pl.Date dtype instead of integers
        df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "PlanID": ["PL1", "PL2"],
            "Enr_Start": pl.Series(
                "Enr_Start",
                ["2020-01-01", "2020-01-15"],
                dtype=pl.Date,
            ),
            "Enr_End": pl.Series(
                "Enr_End",
                ["2020-01-31", "2020-02-28"],
                dtype=pl.Date,
            ),
            "PlanType": ["HMO", "HMO"],
            "PayerType": ["Commercial", "Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_overlapping_spans(conn, "enrollment", schema)
            assert result is not None
            # Overlapping dates should be detected
            assert result.n_failed > 0
        finally:
            conn.close()


class TestEnrollmentGaps:
    """Test suite for check_enrollment_gaps (L2 check 216) using DuckDB views."""

    def test_detects_gaps(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.2: Check 216 flags non-bridged enrollment gaps."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # Patient P1 has a gap: [100, 200] and [300, 400] (gap of 99 days)
        df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "PlanID": ["PL1", "PL2"],
            "Enr_Start": [100, 300],
            "Enr_End": [200, 400],
            "PlanType": ["HMO", "HMO"],
            "PayerType": ["Commercial", "Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_enrollment_gaps(conn, "enrollment", schema)
            assert result is not None
            assert result.check_id == "216"
            assert result.n_failed > 0
            assert result.assertion_type == "enrollment_gaps"
        finally:
            conn.close()

    def test_adjacent_spans_pass(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.3: Adjacent spans (Enr_End + 1 day == next Enr_Start) pass."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # Patient P1 has adjacent spans: [100, 200] and [201, 300]
        df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "PlanID": ["PL1", "PL2"],
            "Enr_Start": [100, 201],
            "Enr_End": [200, 300],
            "PlanType": ["HMO", "HMO"],
            "PayerType": ["Commercial", "Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_enrollment_gaps(conn, "enrollment", schema)
            assert result is not None
            assert result.n_failed == 0
        finally:
            conn.close()

    def test_contiguous_spans_pass(self, tmp_path: Path) -> None:
        """Test that contiguous spans (no gap) pass."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # Patient P1 has contiguous spans: [100, 200] and [200, 300]
        df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "PlanID": ["PL1", "PL2"],
            "Enr_Start": [100, 200],
            "Enr_End": [200, 300],
            "PlanType": ["HMO", "HMO"],
            "PayerType": ["Commercial", "Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_enrollment_gaps(conn, "enrollment", schema)
            assert result is not None
            assert result.n_failed == 0
        finally:
            conn.close()

    def test_returns_none_for_non_enrollment_table(self, tmp_path: Path) -> None:
        """Test that non-enrollment tables return None."""
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_enrollment_gaps(conn, "demographic", schema)
            assert result is None
        finally:
            conn.close()

    def test_check_id_216_and_severity_warn(self, tmp_path: Path) -> None:
        """Test GH-7.AC7.1, GH-7.AC7.2: Check 216 has correct check_id and severity."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "PlanID": ["PL1"],
            "Enr_Start": [100],
            "Enr_End": [200],
            "PlanType": ["HMO"],
            "PayerType": ["Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_enrollment_gaps(conn, "enrollment", schema)
            assert result is not None
            assert result.check_id == "216"
            assert result.severity == "Warn"
        finally:
            conn.close()

    def test_multiple_patients_with_gaps(self, tmp_path: Path) -> None:
        """Test gap detection across multiple patients."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # P1 has a gap, P2 has no gap
        df = pl.DataFrame({
            "PatID": ["P1", "P1", "P2", "P2"],
            "PlanID": ["PL1", "PL2", "PL3", "PL4"],
            "Enr_Start": [100, 300, 500, 501],
            "Enr_End": [200, 400, 550, 650],
            "PlanType": ["HMO", "HMO", "PPO", "PPO"],
            "PayerType": ["Commercial", "Commercial", "Medicare", "Medicare"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_enrollment_gaps(conn, "enrollment", schema)
            assert result is not None
            # P1 has 1 gap row (the second span), P2 has 0
            assert result.n_failed == 1
            assert result.n_passed == 3
        finally:
            conn.close()

    def test_integer_dates(self, tmp_path: Path) -> None:
        """Test that integer dates are handled correctly."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        # Integer dates with gaps
        df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "PlanID": ["PL1", "PL2"],
            "Enr_Start": [100, 300],
            "Enr_End": [200, 400],
            "PlanType": ["HMO", "HMO"],
            "PayerType": ["Commercial", "Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_enrollment_gaps(conn, "enrollment", schema)
            assert result is not None
            assert result.n_failed > 0
        finally:
            conn.close()

    def test_failing_rows_sampled(self, tmp_path: Path) -> None:
        """Test that failing rows are captured correctly."""
        pytest.importorskip("duckdb")
        schema = get_schema("enrollment")
        df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "PlanID": ["PL1", "PL2"],
            "Enr_Start": [100, 300],
            "Enr_End": [200, 400],
            "PlanType": ["HMO", "HMO"],
            "PayerType": ["Commercial", "Commercial"],
        })
        path = tmp_path / "enrollment.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "enrollment" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            result = check_enrollment_gaps(conn, "enrollment", schema)
            assert result is not None
            assert result.n_failed > 0
            assert result.failing_rows is not None
            assert result.failing_rows.height > 0
        finally:
            conn.close()


class TestEncCombinations:
    """Test suite for check_enc_combinations (L2 checks 244, 245) using DuckDB views."""

    def test_invalid_combo_ip_missing_ddate(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.4: Check 244 flags IP rows where DDate is null (required)."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2"],
            "PatID": ["P1", "P2"],
            "EncounterDate": [1000, 2000],
            "EncType": ["IP", "IP"],
            "DDate": [None, 1500],  # E1 missing DDate (invalid for IP)
            "Discharge_Disposition": ["1", "2"],
            "Discharge_Status": ["A", "A"],
            "Admitting_Source": ["01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            # Should have check 244 result
            check_244 = next(r for r in results if r.check_id == "244")
            assert check_244.n_failed == 1  # E1 is invalid
            assert check_244.n_passed == 1
        finally:
            conn.close()

    def test_valid_combo_ip_all_fields(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.4: Check 244 passes when IP has all required fields."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1"],
            "PatID": ["P1"],
            "EncounterDate": [1000],
            "EncType": ["IP"],
            "DDate": [1500],  # DDate present
            "Discharge_Disposition": ["1"],  # Present
            "Discharge_Status": ["A"],  # Present
            "Admitting_Source": ["01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_244 = next(r for r in results if r.check_id == "244")
            assert check_244.n_failed == 0
            assert check_244.n_passed == 1
        finally:
            conn.close()

    def test_invalid_combo_ip_missing_discharge_disposition(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.4: Check 244 flags IP rows where Discharge_Disposition is null (required)."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2"],
            "PatID": ["P1", "P2"],
            "EncounterDate": [1000, 2000],
            "EncType": ["IP", "IP"],
            "DDate": [1500, 1600],  # Both have DDate present
            "Discharge_Disposition": [None, "2"],  # E1 missing Discharge_Disposition (invalid for IP)
            "Discharge_Status": ["A", "A"],
            "Admitting_Source": ["01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_244 = next(r for r in results if r.check_id == "244")
            assert check_244.n_failed == 1  # E1 is invalid
            assert check_244.n_passed == 1
        finally:
            conn.close()

    def test_invalid_combo_ip_missing_discharge_status(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.4: Check 244 flags IP rows where Discharge_Status is null (required)."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2"],
            "PatID": ["P1", "P2"],
            "EncounterDate": [1000, 2000],
            "EncType": ["IP", "IP"],
            "DDate": [1500, 1600],  # Both have DDate present
            "Discharge_Disposition": ["1", "2"],
            "Discharge_Status": [None, "A"],  # E1 missing Discharge_Status (invalid for IP)
            "Admitting_Source": ["01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_244 = next(r for r in results if r.check_id == "244")
            assert check_244.n_failed == 1  # E1 is invalid
            assert check_244.n_passed == 1
        finally:
            conn.close()

    def test_valid_combo_av_with_nulls(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.4: Check 244 passes when AV has null DDate, Disposition, Status."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1"],
            "PatID": ["P1"],
            "EncounterDate": [1000],
            "EncType": ["AV"],
            "DDate": [None],  # Null is OK for AV
            "Discharge_Disposition": [None],  # Null is OK for AV
            "Discharge_Status": [None],  # Null is OK for AV
            "Admitting_Source": ["01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_244 = next(r for r in results if r.check_id == "244")
            assert check_244.n_failed == 0
            assert check_244.n_passed == 1
        finally:
            conn.close()

    def test_threshold_exceeded_check_245(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.5: Check 245 flags EncType groups exceeding threshold."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        # Create 100 IP rows, 10 invalid (10% > 5% threshold)
        ip_rows = []
        for i in range(100):
            if i < 10:
                # Invalid: missing DDate
                ip_rows.append({
                    "EncounterID": f"E{i}",
                    "PatID": f"P{i}",
                    "EncounterDate": 1000 + i,
                    "EncType": "IP",
                    "DDate": None,  # Invalid for IP
                    "Discharge_Disposition": "1",
                    "Discharge_Status": "A",
                    "Admitting_Source": "01",
                })
            else:
                # Valid
                ip_rows.append({
                    "EncounterID": f"E{i}",
                    "PatID": f"P{i}",
                    "EncounterDate": 1000 + i,
                    "EncType": "IP",
                    "DDate": 1500 + i,
                    "Discharge_Disposition": "1",
                    "Discharge_Status": "A",
                    "Admitting_Source": "01",
                })

        df = pl.DataFrame(ip_rows)
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_245_ip = next((r for r in results if r.check_id == "245" and "IP" in r.column), None)
            assert check_245_ip is not None
            assert check_245_ip.n_failed == 10  # 10% exceeds 5% threshold
            assert check_245_ip.n_passed == 90  # Remaining valid rows
        finally:
            conn.close()

    def test_threshold_not_exceeded_check_245(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.5: Check 245 passes when EncType group is under threshold."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        # Create 100 IP rows, 3 invalid (3% < 5% threshold)
        ip_rows = []
        for i in range(100):
            if i < 3:
                # Invalid: missing DDate
                ip_rows.append({
                    "EncounterID": f"E{i}",
                    "PatID": f"P{i}",
                    "EncounterDate": 1000 + i,
                    "EncType": "IP",
                    "DDate": None,  # Invalid for IP
                    "Discharge_Disposition": "1",
                    "Discharge_Status": "A",
                    "Admitting_Source": "01",
                })
            else:
                # Valid
                ip_rows.append({
                    "EncounterID": f"E{i}",
                    "PatID": f"P{i}",
                    "EncounterDate": 1000 + i,
                    "EncType": "IP",
                    "DDate": 1500 + i,
                    "Discharge_Disposition": "1",
                    "Discharge_Status": "A",
                    "Admitting_Source": "01",
                })

        df = pl.DataFrame(ip_rows)
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_245_ip = next((r for r in results if r.check_id == "245" and "IP" in r.column), None)
            assert check_245_ip is not None
            assert check_245_ip.n_failed == 0  # 3% under threshold
            assert check_245_ip.n_passed == 100
        finally:
            conn.close()

    def test_unknown_enctype_flagged(self, tmp_path: Path) -> None:
        """Test GH-7.AC5.6: Unknown EncType values are flagged as invalid."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2"],
            "PatID": ["P1", "P2"],
            "EncounterDate": [1000, 2000],
            "EncType": ["IP", "XX"],  # XX is unknown
            "DDate": [1500, 1600],
            "Discharge_Disposition": ["1", "2"],
            "Discharge_Status": ["A", "A"],
            "Admitting_Source": ["01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_244 = next(r for r in results if r.check_id == "244")
            assert check_244.n_failed == 1  # E2 has unknown EncType
            assert check_244.n_passed == 1
        finally:
            conn.close()

    def test_non_encounter_table_returns_empty(self, tmp_path: Path) -> None:
        """Test that non-encounter tables return empty list."""
        pytest.importorskip("duckdb")
        schema = get_schema("demographic")
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "demographic" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "demographic", schema)
            assert results == []
        finally:
            conn.close()

    def test_check_244_has_correct_id(self, tmp_path: Path) -> None:
        """Test GH-7.AC7.1, GH-7.AC7.2: Check 244 result has check_id='244' and severity='Fail'."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1"],
            "PatID": ["P1"],
            "EncounterDate": [1000],
            "EncType": ["IP"],
            "DDate": [1500],
            "Discharge_Disposition": ["1"],
            "Discharge_Status": ["A"],
            "Admitting_Source": ["01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_244 = next(r for r in results if r.check_id == "244")
            assert check_244.check_id == "244"
            assert check_244.severity == "Fail"
        finally:
            conn.close()

    def test_check_245_has_correct_id(self, tmp_path: Path) -> None:
        """Test GH-7.AC7.1, GH-7.AC7.2: Check 245 result has check_id='245' and severity='Fail'."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1"],
            "PatID": ["P1"],
            "EncounterDate": [1000],
            "EncType": ["IP"],
            "DDate": [1500],
            "Discharge_Disposition": ["1"],
            "Discharge_Status": ["A"],
            "Admitting_Source": ["01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_245_results = [r for r in results if r.check_id == "245"]
            assert len(check_245_results) > 0
            for result in check_245_results:
                assert result.check_id == "245"
                assert result.severity == "Fail"
        finally:
            conn.close()

    def test_multiple_enctypes_multiple_checks_245(self, tmp_path: Path) -> None:
        """Test that check 245 has one result per EncType with non-zero count."""
        pytest.importorskip("duckdb")
        schema = get_schema("encounter")
        df = pl.DataFrame({
            "EncounterID": ["E1", "E2", "E3"],
            "PatID": ["P1", "P2", "P3"],
            "EncounterDate": [1000, 2000, 3000],
            "EncType": ["IP", "ED", "AV"],
            "DDate": [1500, None, None],
            "Discharge_Disposition": ["1", None, None],
            "Discharge_Status": ["A", None, None],
            "Admitting_Source": ["01", "01", "01"],
        })
        path = tmp_path / "encounter.parquet"
        df.write_parquet(path)

        conn = create_connection()
        try:
            safe_path = str(path).replace("'", "''")
            conn.execute(f'CREATE VIEW "encounter" AS SELECT * FROM read_parquet(\'{safe_path}\')')
            results = check_enc_combinations(conn, "encounter", schema)

            check_245_results = [r for r in results if r.check_id == "245"]
            # Should have results for IP, ED, AV (all have non-zero count)
            assert len(check_245_results) == 3
            enctypes = {r.column.split("=")[1] for r in check_245_results}
            assert enctypes == {"IP", "ED", "AV"}
        finally:
            conn.close()

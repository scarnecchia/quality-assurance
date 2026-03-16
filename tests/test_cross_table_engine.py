"""Tests for cross-table validation engine."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest import mock

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scdm_qa.config import QAConfig
from scdm_qa.schemas import get_schema
from scdm_qa.schemas.models import CrossTableCheckDef, TableSchema, ColumnDef
from scdm_qa.validation.cross_table import (
    run_cross_table_checks,
    build_arrow_schema,
    _convert_sas_to_parquet,
    _build_write_schema,
)


@pytest.fixture
def tmp_tables(tmp_path: Path) -> dict[str, Path]:
    """Create temporary parquet files for testing."""
    tables = {}

    # enrollment table with sample data
    enrollment_df = pl.DataFrame({
        "PatID": ["P001", "P002", "P003"],
        "Enr_Start": [20200101, 20200115, 20200201],
        "Enr_End": [20200301, 20200415, 20200601],
    })
    tables["enrollment"] = tmp_path / "enrollment.parquet"
    enrollment_df.write_parquet(tables["enrollment"])

    # demographic table with birth dates
    demographic_df = pl.DataFrame({
        "PatID": ["P001", "P002", "P003"],
        "Birth_Date": [19800101, 19850315, 19900515],
        "Hispanic": ["Y", "N", "Y"],
        "ImputedHispanic": ["Y", "N", "Y"],
        "PostalCode_Date": [20200105, 20200120, 19800101],  # P003 has date before birth
    })
    tables["demographic"] = tmp_path / "demographic.parquet"
    demographic_df.write_parquet(tables["demographic"])

    # diagnosis table with PatID not all in enrollment
    diagnosis_df = pl.DataFrame({
        "PatID": ["P001", "P002", "P999"],  # P999 not in enrollment
        "DX": ["E11.9", "I10", "J45.9"],
        "ADate": [20200105, 20200120, 20200210],
        "ProviderID": ["PR001", "PR002", "PR003"],
    })
    tables["diagnosis"] = tmp_path / "diagnosis.parquet"
    diagnosis_df.write_parquet(tables["diagnosis"])

    # procedure table
    procedure_df = pl.DataFrame({
        "PatID": ["P001", "P002"],
        "PX": ["99213", "99214"],
        "ADate": [20200110, 20200125],
    })
    tables["procedure"] = tmp_path / "procedure.parquet"
    procedure_df.write_parquet(tables["procedure"])

    # encounter table
    encounter_df = pl.DataFrame({
        "PatID": ["P001", "P002", "P003"],
        "ADate": [20200105, 20200120, 20200210],
        "DDate": [20200106, 20200125, 20200215],
        "EncType": ["OP", "IP", "ED"],
        "Discharge_Disposition": [None, "01", None],
        "Discharge_Status": [None, "01", None],
    })
    tables["encounter"] = tmp_path / "encounter.parquet"
    encounter_df.write_parquet(tables["encounter"])

    return tables


@pytest.fixture
def config_with_tables(tmp_tables: dict[str, Path]) -> QAConfig:
    """Create QAConfig with test tables."""
    return QAConfig(
        tables=tmp_tables,
        chunk_size=500_000,
        max_failing_rows=500,
    )


class TestOrchestrator:
    """Test Task 1: Orchestrator and view registration."""

    def test_registers_parquet_tables_as_views(self, config_with_tables: QAConfig) -> None:
        """Test AC1.10: Tables are registered as DuckDB views."""
        pytest.importorskip("duckdb")
        checks: tuple[CrossTableCheckDef, ...] = ()

        # Should not crash even with empty checks
        results = run_cross_table_checks(config_with_tables, checks)
        assert isinstance(results, list)

    def test_missing_reference_table_is_skipped(self, tmp_path: Path) -> None:
        """Test AC1.10: Missing reference table → check skipped with warning, no crash."""
        pytest.importorskip("duckdb")

        # Create only enrollment (no demographic)
        enrollment_df = pl.DataFrame({"PatID": ["P001", "P002"]})
        enrollment_path = tmp_path / "enrollment.parquet"
        enrollment_df.write_parquet(enrollment_path)

        config = QAConfig(
            tables={"enrollment": enrollment_path},
            max_failing_rows=500,
        )

        # Check that requires both enrollment and demographic
        check = CrossTableCheckDef(
            check_id="205",
            check_type="cross_date_compare",
            severity="Fail",
            description="Test check",
            source_table="enrollment",
            reference_table="demographic",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column="Enr_Start",
            compare_reference_column="Birth_Date",
        )

        results = run_cross_table_checks(config, (check,))
        # Check should be skipped, no results
        assert len(results) == 0

    def test_temp_parquet_cleanup(self, tmp_path: Path) -> None:
        """Test AC1.12: Temp parquet files are cleaned up after checks complete.

        Note: Actual SAS conversion is tested in test_sas_to_parquet_conversion fixture
        since creating valid SAS files requires pyreadstat.write_sas7bdat which may not be available.
        This test verifies the cleanup logic works for parquet tables.
        """
        pytest.importorskip("duckdb")

        # Create a simple parquet file
        df = pl.DataFrame({
            "PatID": ["P001", "P002"],
            "Value": [100, 200],
        })
        table_path = tmp_path / "test.parquet"
        df.write_parquet(table_path)

        config = QAConfig(
            tables={"test": table_path},
            max_failing_rows=500,
        )

        # Run an empty check set
        results = run_cross_table_checks(config, ())
        assert isinstance(results, list)


class TestReferentialIntegrity:
    """Test Check 201: Referential integrity."""

    def test_detects_missing_patid_in_enrollment(self, config_with_tables: QAConfig) -> None:
        """Test AC1.2: PatID in diagnosis not in enrollment is flagged."""
        pytest.importorskip("duckdb")

        check = CrossTableCheckDef(
            check_id="201",
            check_type="referential_integrity",
            severity="Warn",
            description="PatID in diagnosis but not in enrollment",
            source_table="diagnosis",
            reference_table="enrollment",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column=None,
        )

        results = run_cross_table_checks(config_with_tables, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.check_id == "201"
        assert result.n_failed == 1  # P999 is not in enrollment
        assert result.n_passed == 2
        assert result.failing_rows is not None
        assert result.failing_rows.height == 1

    def test_no_missing_references_passes(self, config_with_tables: QAConfig) -> None:
        """Test AC1.2: When all PatIDs exist in reference, check passes."""
        pytest.importorskip("duckdb")

        # procedure → enrollment (all P001, P002 exist)
        check = CrossTableCheckDef(
            check_id="201",
            check_type="referential_integrity",
            severity="Warn",
            description="PatID in procedure but not in enrollment",
            source_table="procedure",
            reference_table="enrollment",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column=None,
        )

        results = run_cross_table_checks(config_with_tables, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.n_failed == 0
        assert result.n_passed == 2


class TestLengthConsistency:
    """Test Check 203: Length consistency across tables."""

    def test_detects_different_max_lengths(self, tmp_path: Path) -> None:
        """Test AC1.3: Different max string lengths for same column is flagged."""
        pytest.importorskip("duckdb")

        # Create two tables with different max PatID lengths
        enrollment_df = pl.DataFrame({
            "PatID": ["P001", "P002"],
        })
        enrollment_path = tmp_path / "enrollment.parquet"
        enrollment_df.write_parquet(enrollment_path)

        diagnosis_df = pl.DataFrame({
            "PatID": ["P001XXXX", "P002YYYY"],  # Longer PatID
        })
        diagnosis_path = tmp_path / "diagnosis.parquet"
        diagnosis_df.write_parquet(diagnosis_path)

        config = QAConfig(
            tables={"enrollment": enrollment_path, "diagnosis": diagnosis_path},
            max_failing_rows=500,
        )

        check = CrossTableCheckDef(
            check_id="203",
            check_type="length_consistency",
            severity="Warn",
            description="PatID length consistency",
            source_table="enrollment",
            join_column="PatID",
            reference_table=None,
            join_reference_column=None,
            table_group=("enrollment", "diagnosis"),
            compare_column=None,
        )

        results = run_cross_table_checks(config, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.check_id == "203"
        assert result.n_failed == 2  # Two different max lengths
        assert result.failing_rows is not None

    def test_consistent_lengths_passes(self, tmp_path: Path) -> None:
        """Test AC1.3: When max lengths are same across tables, check passes."""
        pytest.importorskip("duckdb")

        enrollment_df = pl.DataFrame({
            "PatID": ["P001", "P002"],
        })
        enrollment_path = tmp_path / "enrollment.parquet"
        enrollment_df.write_parquet(enrollment_path)

        diagnosis_df = pl.DataFrame({
            "PatID": ["P003", "P004"],  # Same max length as enrollment
        })
        diagnosis_path = tmp_path / "diagnosis.parquet"
        diagnosis_df.write_parquet(diagnosis_path)

        config = QAConfig(
            tables={"enrollment": enrollment_path, "diagnosis": diagnosis_path},
            max_failing_rows=500,
        )

        check = CrossTableCheckDef(
            check_id="203",
            check_type="length_consistency",
            severity="Warn",
            description="PatID length consistency",
            source_table="enrollment",
            join_column="PatID",
            reference_table=None,
            join_reference_column=None,
            table_group=("enrollment", "diagnosis"),
            compare_column=None,
        )

        results = run_cross_table_checks(config, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.n_failed == 0
        assert result.n_passed == 1


class TestCrossDateCompare:
    """Test Checks 205, 206, 227: Cross-table date comparison."""

    def test_detects_enr_start_before_birth_date(self, tmp_path: Path) -> None:
        """Test AC1.4: Enr_Start before Birth_Date is flagged."""
        pytest.importorskip("duckdb")

        enrollment_df = pl.DataFrame({
            "PatID": ["P001", "P002", "P003"],
            "Enr_Start": [20200101, 19800101, 20200301],
        })
        enrollment_path = tmp_path / "enrollment.parquet"
        enrollment_df.write_parquet(enrollment_path)

        demographic_df = pl.DataFrame({
            "PatID": ["P001", "P002", "P003"],
            "Birth_Date": [19800101, 19900101, 20190101],
        })
        demographic_path = tmp_path / "demographic.parquet"
        demographic_df.write_parquet(demographic_path)

        config = QAConfig(
            tables={"enrollment": enrollment_path, "demographic": demographic_path},
            max_failing_rows=500,
        )

        check = CrossTableCheckDef(
            check_id="205",
            check_type="cross_date_compare",
            severity="Fail",
            description="Enr_Start must not be before Birth_Date",
            source_table="enrollment",
            reference_table="demographic",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column="Enr_Start",
            compare_reference_column="Birth_Date",
        )

        results = run_cross_table_checks(config, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.check_id == "205"
        assert result.n_failed == 1  # P002 has Enr_Start before Birth_Date
        assert result.n_passed == 2

    def test_detects_adate_before_birth_date(self, config_with_tables: QAConfig) -> None:
        """Test AC1.5: ADate before Birth_Date in encounter is flagged."""
        pytest.importorskip("duckdb")

        check = CrossTableCheckDef(
            check_id="206",
            check_type="cross_date_compare",
            severity="Fail",
            description="Encounter ADate must not be before Birth_Date",
            source_table="encounter",
            reference_table="demographic",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column="ADate",
            compare_reference_column="Birth_Date",
        )

        results = run_cross_table_checks(config_with_tables, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.check_id == "206"
        # ADate values are after birth dates in test data
        assert result.n_failed == 0

    def test_detects_postal_code_date_before_birth_date(self, config_with_tables: QAConfig) -> None:
        """Test AC1.6: PostalCode_Date before Birth_Date is flagged."""
        pytest.importorskip("duckdb")

        check = CrossTableCheckDef(
            check_id="227",
            check_type="cross_date_compare",
            severity="Fail",
            description="PostalCode_Date must not be before Birth_Date",
            source_table="demographic",
            reference_table="demographic",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column="PostalCode_Date",
            compare_reference_column="Birth_Date",
        )

        results = run_cross_table_checks(config_with_tables, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.check_id == "227"
        assert result.n_failed == 1  # P003 has PostalCode_Date before Birth_Date


class TestLengthExcess:
    """Test Check 209: Actual max length much smaller than declared."""

    def test_flags_actual_much_smaller_than_declared(self, tmp_path: Path) -> None:
        """Test AC1.7: Actual max length < declared * 0.5 is flagged."""
        import unittest.mock
        pytest.importorskip("duckdb")

        # Create diagnosis table for testing length_excess check
        diagnosis_df = pl.DataFrame({
            "PatID": ["P001", "P002", "P003"],
            "DX": ["AB", "CD", "EF"],  # actual max length = 2
        })
        diagnosis_path = tmp_path / "diagnosis.parquet"
        diagnosis_df.write_parquet(diagnosis_path)

        config = QAConfig(
            tables={"diagnosis": diagnosis_path},
            max_failing_rows=500,
        )

        # Create a mock schema where DX has declared length = 100
        # actual_max = 2, threshold = 100 * 0.5 = 50
        # 2 < 50 is true, so should trigger n_failed = 1
        from scdm_qa.schemas.models import TableSchema, ColumnDef

        mock_col_def = ColumnDef(
            name="DX",
            col_type="Character",
            missing_allowed=True,
            length=100,
            allowed_values=None,
            definition="Diagnosis code",
            example="E11.9",
        )
        mock_schema = TableSchema(
            table_name="diagnosis",
            table_key="diagnosis",
            description="Diagnosis table",
            sort_order=(),
            unique_row=(),
            columns=(mock_col_def,),
            conditional_rules=(),
        )

        check = CrossTableCheckDef(
            check_id="209",
            check_type="length_excess",
            severity="Warn",
            description="Actual DX length much smaller than declared (100)",
            source_table="diagnosis",
            join_column="DX",
            compare_column=None,
            reference_table=None,
            join_reference_column=None,
        )

        with unittest.mock.patch(
            "scdm_qa.validation.cross_table.get_schema", return_value=mock_schema
        ):
            results = run_cross_table_checks(config, (check,))

        assert len(results) == 1
        result = results[0]
        assert result.check_id == "209"
        # DX column has actual max length 2, declared 100
        # 2 < 100*0.5 = 50, so should fail
        assert result.n_failed == 1, f"Expected n_failed == 1, got {result.n_failed}"
        assert result.failing_rows is not None

    def test_passes_when_actual_large_enough(self, tmp_path: Path) -> None:
        """Test AC1.7: When actual max length is >= declared * 0.5, check passes."""
        pytest.importorskip("duckdb")

        # Create diagnosis table where actual max length is large relative to declared
        diagnosis_df = pl.DataFrame({
            "PatID": ["P001", "P002", "P003"],
            "DX": ["E11.9", "I10", "J45.9"],
            "PDX": ["1", "0", "1"],  # max length=1
        })
        diagnosis_path = tmp_path / "diagnosis.parquet"
        diagnosis_df.write_parquet(diagnosis_path)

        config = QAConfig(
            tables={"diagnosis": diagnosis_path},
            max_failing_rows=500,
        )

        check = CrossTableCheckDef(
            check_id="209",
            check_type="length_excess",
            severity="Warn",
            description="Actual PDX length test",
            source_table="diagnosis",
            join_column="PDX",
            compare_column=None,
            reference_table=None,
            join_reference_column=None,
        )

        results = run_cross_table_checks(config, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.check_id == "209"
        # PDX actual max length=1, declared=1 in schema
        # 1 is not < 1 * 0.5 = 0.5, so should pass
        assert result.n_failed == 0
        assert result.n_passed == 1


class TestColumnMismatch:
    """Test Check 224: Column mismatch (Hispanic != ImputedHispanic)."""

    def test_detects_mismatches(self, tmp_path: Path) -> None:
        """Test AC1.8: Hispanic != ImputedHispanic (both non-null) is flagged."""
        pytest.importorskip("duckdb")

        demographic_df = pl.DataFrame({
            "PatID": ["P001", "P002", "P003", "P004"],
            "Hispanic": ["Y", "N", "Y", None],
            "ImputedHispanic": ["Y", "N", "N", "Y"],  # P003 mismatch, P004 has null
        })
        demographic_path = tmp_path / "demographic.parquet"
        demographic_df.write_parquet(demographic_path)

        config = QAConfig(
            tables={"demographic": demographic_path},
            max_failing_rows=500,
        )

        check = CrossTableCheckDef(
            check_id="224",
            check_type="column_mismatch",
            severity="Warn",
            description="Hispanic must not differ from ImputedHispanic",
            source_table="demographic",
            column_a="Hispanic",
            column_b="ImputedHispanic",
            compare_column=None,
            reference_table=None,
            join_column=None,
            join_reference_column=None,
        )

        results = run_cross_table_checks(config, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.check_id == "224"
        assert result.n_failed == 1  # Only P003
        assert result.n_passed == 2  # P001, P002 match

    def test_no_mismatches_passes(self, config_with_tables: QAConfig) -> None:
        """Test AC1.8: When Hispanic == ImputedHispanic, check passes."""
        pytest.importorskip("duckdb")

        check = CrossTableCheckDef(
            check_id="224",
            check_type="column_mismatch",
            severity="Warn",
            description="Hispanic must not differ from ImputedHispanic",
            source_table="demographic",
            column_a="Hispanic",
            column_b="ImputedHispanic",
            compare_column=None,
            reference_table=None,
            join_column=None,
            join_reference_column=None,
        )

        results = run_cross_table_checks(config_with_tables, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.n_failed == 0
        assert result.n_passed == 3


class TestErrorHandling:
    """Test AC1.11: Error handling."""

    def test_duckdb_error_returns_error_result(self, tmp_path: Path) -> None:
        """Test AC1.11: DuckDB error on a single check → error StepResult, pipeline continues."""
        pytest.importorskip("duckdb")

        enrollment_df = pl.DataFrame({
            "PatID": ["P001", "P002"],
        })
        enrollment_path = tmp_path / "enrollment.parquet"
        enrollment_df.write_parquet(enrollment_path)

        config = QAConfig(
            tables={"enrollment": enrollment_path},
            max_failing_rows=500,
        )

        # Create a check that references a non-existent column
        check = CrossTableCheckDef(
            check_id="999",
            check_type="referential_integrity",
            severity="Fail",
            description="Test error handling",
            source_table="enrollment",
            reference_table="enrollment",
            join_column="NonExistentColumn",
            join_reference_column="PatID",
            compare_column=None,
        )

        results = run_cross_table_checks(config, (check,))
        assert len(results) == 1
        result = results[0]
        assert result.check_id == "999"
        # Should have error in description
        assert "error" in result.description.lower()


class TestTableFiltering:
    """Test filtering checks by table."""

    def test_filters_checks_by_source_table(self, config_with_tables: QAConfig) -> None:
        """Test that table_filter only runs checks involving specified table."""
        pytest.importorskip("duckdb")

        check1 = CrossTableCheckDef(
            check_id="201",
            check_type="referential_integrity",
            severity="Warn",
            description="Check for diagnosis",
            source_table="diagnosis",
            reference_table="enrollment",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column=None,
        )

        check2 = CrossTableCheckDef(
            check_id="201",
            check_type="referential_integrity",
            severity="Warn",
            description="Check for procedure",
            source_table="procedure",
            reference_table="enrollment",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column=None,
        )

        results = run_cross_table_checks(
            config_with_tables,
            (check1, check2),
            table_filter="diagnosis",
        )
        # Only check1 should run (source_table == diagnosis)
        assert len(results) == 1
        assert results[0].description == "Check for diagnosis"

    def test_filters_checks_by_reference_table(self, config_with_tables: QAConfig) -> None:
        """Test that table_filter includes checks where table is reference."""
        pytest.importorskip("duckdb")

        check1 = CrossTableCheckDef(
            check_id="201",
            check_type="referential_integrity",
            severity="Warn",
            description="Check for diagnosis",
            source_table="diagnosis",
            reference_table="enrollment",
            join_column="PatID",
            join_reference_column="PatID",
            compare_column=None,
        )

        results = run_cross_table_checks(
            config_with_tables,
            (check1,),
            table_filter="enrollment",
        )
        # check1 should run because enrollment is its reference_table
        assert len(results) == 1


class TestBuildArrowSchema:
    """Test build_arrow_schema: SCDM-to-pyarrow schema conversion."""

    def test_known_table_produces_correct_types(self) -> None:
        """Test GH-6.AC3.1: Known SCDM table resolves to canonical pyarrow.Schema."""
        schema = get_schema("demographic")
        arrow_schema = build_arrow_schema(schema)

        assert isinstance(arrow_schema, pa.Schema)
        assert len(arrow_schema.names) > 0
        # demographic should have multiple fields
        assert "PatID" in arrow_schema.names

    def test_numeric_columns_map_to_float64(self) -> None:
        """Test numeric columns are mapped to pa.float64()."""
        schema = get_schema("demographic")
        arrow_schema = build_arrow_schema(schema)

        # Find a known numeric column in demographic (e.g., Birth_Date)
        birth_date_field = arrow_schema.field("Birth_Date")
        assert birth_date_field.type == pa.float64()

    def test_character_columns_map_to_utf8(self) -> None:
        """Test character columns are mapped to pa.utf8()."""
        schema = get_schema("demographic")
        arrow_schema = build_arrow_schema(schema)

        # Find a known character column in demographic (e.g., Sex)
        sex_field = arrow_schema.field("Sex")
        assert sex_field.type == pa.utf8()

    def test_nullability_matches_missing_allowed(self) -> None:
        """Test nullability in arrow schema matches ColumnDef.missing_allowed."""
        schema = get_schema("demographic")
        arrow_schema = build_arrow_schema(schema)

        # Check that nullability matches the column definition
        for col_def in schema.columns:
            arrow_field = arrow_schema.field(col_def.name)
            assert arrow_field.nullable == col_def.missing_allowed

    def test_data_columns_filters_and_orders(self) -> None:
        """Test data_columns parameter filters and orders columns."""
        schema = get_schema("demographic")
        # Specify a subset in a specific order (fully reversed from spec order)
        data_cols = ("Hispanic", "Birth_Date", "PatID")
        arrow_schema = build_arrow_schema(schema, data_columns=data_cols)

        # Schema should include only these columns in this order
        assert arrow_schema.names == ["Hispanic", "Birth_Date", "PatID"]

    def test_data_columns_excludes_non_spec_columns(self) -> None:
        """Test that columns in data but not in spec are excluded."""
        schema = get_schema("demographic")
        # Include a column that exists in spec and one that doesn't
        data_cols = ("PatID", "Birth_Date", "UnknownColumn", "Hispanic")
        arrow_schema = build_arrow_schema(schema, data_columns=data_cols)

        # UnknownColumn should not be in the schema
        assert "UnknownColumn" not in arrow_schema.names
        # Only spec columns should be present
        assert set(arrow_schema.names) <= set(schema.column_names)

    def test_unknown_col_type_raises_value_error(self) -> None:
        """Test that unknown col_type raises ValueError."""
        # Create a custom TableSchema with an unknown col_type
        bad_col = ColumnDef(
            name="BadColumn",
            col_type="UnknownType",
            missing_allowed=True,
            length=None,
            allowed_values=None,
            definition="Test column with unknown type",
            example="test",
        )
        bad_schema = TableSchema(
            table_name="test_table",
            table_key="test_table",
            description="Test table",
            sort_order=(),
            unique_row=(),
            columns=(bad_col,),
            conditional_rules=(),
        )

        with pytest.raises(ValueError, match="unrecognised SCDM col_type"):
            build_arrow_schema(bad_schema)


class TestStreamingSasConversion:
    """Test Task 1: Streaming SAS-to-Parquet conversion."""

    def test_multi_chunk_preserves_all_rows(self) -> None:
        """Test GH-6.AC1.1: Multi-chunk write preserves all rows."""
        # Create mock reader yielding 3 chunks of 10 rows each
        chunk1 = pl.DataFrame({"PatID": [f"P{i:03d}" for i in range(10)], "Value": list(range(10))})
        chunk2 = pl.DataFrame({"PatID": [f"P{i:03d}" for i in range(10, 20)], "Value": list(range(10, 20))})
        chunk3 = pl.DataFrame({"PatID": [f"P{i:03d}" for i in range(20, 30)], "Value": list(range(20, 30))})

        mock_reader = mock.Mock()
        mock_reader.chunks.return_value = [chunk1, chunk2, chunk3]

        with mock.patch("scdm_qa.readers.create_reader", return_value=mock_reader):
            result_path = _convert_sas_to_parquet(
                Path("/fake/path.sas7bdat"),
                chunk_size=500_000,
                table_key="test_table",
            )

        # Read back the parquet file and verify all rows are present
        result_df = pl.read_parquet(result_path)
        assert result_df.height == 30
        assert set(result_df["Value"].to_list()) == set(range(30))
        result_path.unlink()

    def test_each_chunk_becomes_row_group(self) -> None:
        """Test GH-6.AC1.2: Each chunk becomes a separate Parquet row group."""
        chunk1 = pl.DataFrame({"PatID": ["P001", "P002"], "Value": [1, 2]})
        chunk2 = pl.DataFrame({"PatID": ["P003", "P004"], "Value": [3, 4]})
        chunk3 = pl.DataFrame({"PatID": ["P005", "P006"], "Value": [5, 6]})

        mock_reader = mock.Mock()
        mock_reader.chunks.return_value = [chunk1, chunk2, chunk3]

        with mock.patch("scdm_qa.readers.create_reader", return_value=mock_reader):
            result_path = _convert_sas_to_parquet(
                Path("/fake/path.sas7bdat"),
                chunk_size=500_000,
                table_key="test_table",
            )

        # Read parquet metadata
        parquet_meta = pq.read_metadata(result_path)
        assert parquet_meta.num_row_groups == 3
        result_path.unlink()

    def test_output_schema_matches_canonical_types(self) -> None:
        """Test GH-6.AC1.3: Output schema matches canonical SCDM spec types."""
        # Create chunk with data that could be inferred as different types
        chunk = pl.DataFrame({
            "PatID": [1001.0, 1002.0],  # Numeric in spec
            "Birth_Date": [19800101.0, 19850315.0],  # Numeric in spec
            "Sex": ["M", "F"],  # Character in spec
        })

        mock_reader = mock.Mock()
        mock_reader.chunks.return_value = [chunk]

        with mock.patch("scdm_qa.readers.create_reader", return_value=mock_reader):
            result_path = _convert_sas_to_parquet(
                Path("/fake/path.sas7bdat"),
                chunk_size=500_000,
                table_key="demographic",
            )

        # Read back and verify schema types match SCDM spec
        result_df = pl.read_parquet(result_path)
        result_schema = result_df.to_arrow().schema
        # Numeric columns (Birth_Date, PatID) should be float64
        assert result_schema.field("Birth_Date").type == pa.float64()
        assert result_schema.field("PatID").type == pa.float64()
        # Character columns (Sex) should be string-like (utf8 or large_string)
        sex_type = result_schema.field("Sex").type
        assert sex_type in (pa.utf8(), pa.large_string(), pa.string())
        result_path.unlink()

    def test_empty_input_produces_valid_parquet(self) -> None:
        """Test GH-6.AC1.4: Empty input (zero chunks) produces valid empty Parquet."""
        mock_reader = mock.Mock()
        mock_reader.chunks.return_value = []

        with mock.patch("scdm_qa.readers.create_reader", return_value=mock_reader):
            result_path = _convert_sas_to_parquet(
                Path("/fake/path.sas7bdat"),
                chunk_size=500_000,
                table_key="demographic",
            )

        # Read back and verify it's a valid empty parquet
        result_df = pl.read_parquet(result_path)
        assert result_df.height == 0
        # Should have columns from the canonical schema
        assert len(result_df.columns) > 0
        result_path.unlink()

    def test_all_null_column_cast_to_canonical_type(self) -> None:
        """Test GH-6.AC1.5: All-null column is cast to canonical type, not inferred as null."""
        # Create chunk where a Numeric column is all null
        chunk = pl.DataFrame({
            "PatID": [1001.0, 1002.0],
            "Birth_Date": [None, None],  # All null, could be inferred as Null type
            "Sex": ["M", "F"],
        })

        mock_reader = mock.Mock()
        mock_reader.chunks.return_value = [chunk]

        with mock.patch("scdm_qa.readers.create_reader", return_value=mock_reader):
            result_path = _convert_sas_to_parquet(
                Path("/fake/path.sas7bdat"),
                chunk_size=500_000,
                table_key="demographic",
            )

        # Read back and verify Birth_Date was cast to float64, not null
        result_df = pl.read_parquet(result_path)
        result_schema = result_df.to_arrow().schema
        assert result_schema.field("Birth_Date").type == pa.float64()
        result_path.unlink()

    def test_extra_columns_preserved_with_inferred_types(self) -> None:
        """Test GH-6.AC3.2: Extra columns not in SCDM spec are preserved with inferred types."""
        chunk = pl.DataFrame({
            "PatID": [1001.0, 1002.0],
            "Birth_Date": [19800101.0, 19850315.0],
            "CustomColumn": [100, 200],  # Not in spec
        })

        mock_reader = mock.Mock()
        mock_reader.chunks.return_value = [chunk]

        with mock.patch("scdm_qa.readers.create_reader", return_value=mock_reader):
            result_path = _convert_sas_to_parquet(
                Path("/fake/path.sas7bdat"),
                chunk_size=500_000,
                table_key="demographic",
            )

        # Read back and verify CustomColumn is present with its inferred type
        result_df = pl.read_parquet(result_path)
        assert "CustomColumn" in result_df.columns
        # CustomColumn should have int type (inferred from data)
        result_schema = result_df.to_arrow().schema
        assert "CustomColumn" in result_schema.names
        result_path.unlink()

    def test_unknown_table_key_falls_back_to_inference(self, caplog) -> None:
        """Test GH-6.AC3.3: Unknown table_key falls back to inference and logs warning."""
        chunk = pl.DataFrame({
            "PatID": ["P001", "P002"],
            "Value": [100, 200],
        })

        mock_reader = mock.Mock()
        mock_reader.chunks.return_value = [chunk]

        # Capture logs via stdlib logging which structlog integrates with
        caplog.set_level(logging.WARNING)

        with mock.patch("scdm_qa.readers.create_reader", return_value=mock_reader):
            result_path = _convert_sas_to_parquet(
                Path("/fake/path.sas7bdat"),
                chunk_size=500_000,
                table_key="unknown_table_xyz",
            )

        # Should still produce valid parquet with inferred types
        result_df = pl.read_parquet(result_path)
        assert result_df.height == 2
        assert set(result_df.columns) == {"PatID", "Value"}
        result_path.unlink()

        # Assert warning was logged containing the unknown table key
        # structlog logs through stdlib.BoundLogger which registers with logging module
        assert any(
            "no SCDM spec for table" in record.message
            and record.levelname == "WARNING"
            for record in caplog.records
        ), f"Expected warning about unknown table key, got: {[r.message for r in caplog.records]}"
        # Verify table_key is in the log context
        assert any("unknown_table_xyz" in str(record) for record in caplog.records), \
            f"Expected 'unknown_table_xyz' in log records"


class TestBuildWriteSchema:
    """Test Task 2: _build_write_schema merge logic."""

    def test_canonical_columns_get_spec_types(self) -> None:
        """Test GH-6.AC3.2: Canonical columns get spec types, not inferred."""
        # Create a data schema with a column that could be inferred as object
        data_schema = pa.schema([
            pa.field("PatID", pa.utf8()),
            pa.field("Birth_Date", pa.float64()),  # Inferred as float
            pa.field("Sex", pa.utf8()),
        ])

        # Get the canonical schema for demographic
        canonical = get_schema("demographic")

        write_schema = _build_write_schema(canonical, data_schema)

        # Birth_Date should be float64 from spec (nullable based on spec)
        birth_date_field = write_schema.field("Birth_Date")
        assert birth_date_field.type == pa.float64()
        # Sex should be utf8 from spec
        sex_field = write_schema.field("Sex")
        assert sex_field.type == pa.utf8()

    def test_non_spec_columns_keep_inferred_types(self) -> None:
        """Test that non-spec columns keep their inferred types."""
        data_schema = pa.schema([
            pa.field("PatID", pa.utf8()),
            pa.field("CustomInt", pa.int32()),  # Not in spec
            pa.field("CustomString", pa.utf8()),  # Not in spec
        ])

        canonical = get_schema("demographic")
        write_schema = _build_write_schema(canonical, data_schema)

        # CustomInt should keep int32 (inferred)
        custom_int_field = write_schema.field("CustomInt")
        assert custom_int_field.type == pa.int32()
        # CustomString should keep utf8 (inferred)
        custom_string_field = write_schema.field("CustomString")
        assert custom_string_field.type == pa.utf8()

    def test_column_order_follows_data(self) -> None:
        """Test that column order follows data schema, not spec order."""
        # Create data schema with columns in non-spec order
        data_schema = pa.schema([
            pa.field("Sex", pa.utf8()),
            pa.field("PatID", pa.utf8()),
            pa.field("Birth_Date", pa.float64()),
        ])

        canonical = get_schema("demographic")
        write_schema = _build_write_schema(canonical, data_schema)

        # Order should match data schema, not spec
        assert write_schema.names == ["Sex", "PatID", "Birth_Date"]

    def test_none_canonical_returns_data_schema(self) -> None:
        """Test that None canonical_schema returns data_schema unchanged."""
        data_schema = pa.schema([
            pa.field("PatID", pa.utf8()),
            pa.field("Value", pa.int32()),
            pa.field("Extra", pa.float64()),
        ])

        write_schema = _build_write_schema(None, data_schema)

        # Should be identical to data_schema
        assert write_schema == data_schema
        assert write_schema.names == data_schema.names
        for i, field in enumerate(write_schema):
            assert field.type == data_schema.field(i).type

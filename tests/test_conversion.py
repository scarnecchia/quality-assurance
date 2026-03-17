"""Tests for SAS-to-Parquet conversion utilities."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from scdm_qa.readers.conversion import (
    build_arrow_schema,
    convert_sas_to_parquet,
    converted_parquet,
)
from scdm_qa.schemas.models import TableSchema, ColumnDef


class TestBuildArrowSchema:
    """Test arrow schema building from SCDM specs."""

    def test_builds_schema_from_table_schema(self) -> None:
        """Test that build_arrow_schema produces correct arrow schema."""
        table_schema = TableSchema(
            table_name="Test Table",
            table_key="test_table",
            description="Test table",
            sort_order=(),
            unique_row=(),
            columns=(
                ColumnDef(
                    name="PatID",
                    col_type="Character",
                    missing_allowed=False,
                    length=10,
                    allowed_values=None,
                    definition="Patient ID",
                    example="123456",
                ),
                ColumnDef(
                    name="Age",
                    col_type="Numeric",
                    missing_allowed=True,
                    length=None,
                    allowed_values=None,
                    definition="Patient age",
                    example="45",
                ),
            ),
            conditional_rules=(),
        )

        schema = build_arrow_schema(table_schema)

        assert isinstance(schema, pa.Schema)
        assert len(schema) == 2
        assert schema.field("PatID").type == pa.utf8()
        assert schema.field("PatID").nullable is False
        assert schema.field("Age").type == pa.float64()
        assert schema.field("Age").nullable is True

    def test_filters_to_data_columns_only(self) -> None:
        """Test data_columns parameter filters spec columns."""
        table_schema = TableSchema(
            table_name="Test Table",
            table_key="test_table",
            description="Test table",
            sort_order=(),
            unique_row=(),
            columns=(
                ColumnDef(
                    name="PatID",
                    col_type="Character",
                    missing_allowed=False,
                    length=10,
                    allowed_values=None,
                    definition="Patient ID",
                    example="123456",
                ),
                ColumnDef(
                    name="Age",
                    col_type="Numeric",
                    missing_allowed=True,
                    length=None,
                    allowed_values=None,
                    definition="Patient age",
                    example="45",
                ),
                ColumnDef(
                    name="Gender",
                    col_type="Character",
                    missing_allowed=True,
                    length=1,
                    allowed_values=None,
                    definition="Gender",
                    example="M",
                ),
            ),
            conditional_rules=(),
        )

        schema = build_arrow_schema(table_schema, data_columns=("PatID", "Gender"))

        assert len(schema) == 2
        assert schema.names == ["PatID", "Gender"]

    def test_raises_on_unknown_col_type(self) -> None:
        """Test that unknown col_type raises ValueError."""
        table_schema = TableSchema(
            table_name="Test Table",
            table_key="test_table",
            description="Test table",
            sort_order=(),
            unique_row=(),
            columns=(
                ColumnDef(
                    name="PatID",
                    col_type="UnknownType",
                    missing_allowed=False,
                    length=10,
                    allowed_values=None,
                    definition="Patient ID",
                    example="123456",
                ),
            ),
            conditional_rules=(),
        )

        with pytest.raises(ValueError, match="unrecognised SCDM col_type"):
            build_arrow_schema(table_schema)

    def test_preserves_column_order(self) -> None:
        """Test that column order matches data_columns order."""
        table_schema = TableSchema(
            table_name="Test Table",
            table_key="test_table",
            description="Test table",
            sort_order=(),
            unique_row=(),
            columns=(
                ColumnDef(
                    name="A",
                    col_type="Character",
                    missing_allowed=False,
                    length=1,
                    allowed_values=None,
                    definition="Col A",
                    example="a",
                ),
                ColumnDef(
                    name="B",
                    col_type="Numeric",
                    missing_allowed=False,
                    length=None,
                    allowed_values=None,
                    definition="Col B",
                    example="1",
                ),
                ColumnDef(
                    name="C",
                    col_type="Character",
                    missing_allowed=False,
                    length=1,
                    allowed_values=None,
                    definition="Col C",
                    example="c",
                ),
            ),
            conditional_rules=(),
        )

        schema = build_arrow_schema(table_schema, data_columns=("C", "A", "B"))

        assert schema.names == ["C", "A", "B"]


class TestConvertedParquetContextManager:
    """Test converted_parquet context manager."""

    def test_context_manager_yields_path(self, tmp_path: Path) -> None:
        """Test that context manager yields a valid path."""
        pytest.importorskip("pyreadstat")

        # Create a minimal SAS file using pyreadstat
        sas_path = tmp_path / "test.sas7bdat"
        df = pl.DataFrame({"PatID": ["P001", "P002"], "Age": [25.0, 35.0]})

        try:
            import pyreadstat

            df_pandas = df.to_pandas()
            pyreadstat.write_sas7bdat(df_pandas, str(sas_path))
        except (ImportError, AttributeError):
            pytest.skip("pyreadstat.write_sas7bdat not available")

        with converted_parquet(sas_path, table_key="test_table") as pq_path:
            assert isinstance(pq_path, Path)
            assert pq_path.suffix == ".parquet"
            assert pq_path.exists()

    def test_context_manager_cleans_up_on_exit(self, tmp_path: Path) -> None:
        """Test that temp file is deleted after context manager exits."""
        pytest.importorskip("pyreadstat")

        # Create a minimal SAS file using pyreadstat
        sas_path = tmp_path / "test.sas7bdat"
        df = pl.DataFrame({"PatID": ["P001", "P002"], "Age": [25.0, 35.0]})

        try:
            import pyreadstat

            df_pandas = df.to_pandas()
            pyreadstat.write_sas7bdat(df_pandas, str(sas_path))
        except (ImportError, AttributeError):
            pytest.skip("pyreadstat.write_sas7bdat not available")

        pq_path = None
        with converted_parquet(sas_path, table_key="test_table") as temp_pq:
            pq_path = temp_pq
            assert pq_path.exists()

        # File should be deleted after context
        assert pq_path is not None
        assert not pq_path.exists()

    def test_context_manager_cleans_up_on_exception(self, tmp_path: Path) -> None:
        """Test that temp file is cleaned up even when body raises exception."""
        pytest.importorskip("pyreadstat")

        # Create a minimal SAS file
        sas_path = tmp_path / "test.sas7bdat"
        df = pl.DataFrame({"PatID": ["P001", "P002"], "Age": [25.0, 35.0]})

        try:
            import pyreadstat

            df_pandas = df.to_pandas()
            pyreadstat.write_sas7bdat(df_pandas, str(sas_path))
        except (ImportError, AttributeError):
            pytest.skip("pyreadstat.write_sas7bdat not available")

        pq_path = None
        try:
            with converted_parquet(sas_path, table_key="test_table") as temp_pq:
                pq_path = temp_pq
                raise ValueError("test exception")
        except ValueError:
            pass

        # File should still be deleted
        assert pq_path is not None
        assert not pq_path.exists()


class TestConvertSasToParquet:
    """Test convert_sas_to_parquet function."""

    def test_convert_sas_to_parquet_importable(self) -> None:
        """Test that convert_sas_to_parquet is importable from conversion module."""
        from scdm_qa.readers.conversion import convert_sas_to_parquet as func

        assert callable(func)

    def test_convert_sas_to_parquet_importable_from_readers(self) -> None:
        """Test GH-8.AC4.4: convert_sas_to_parquet importable from scdm_qa.readers."""
        from scdm_qa.readers import convert_sas_to_parquet as func

        assert callable(func)

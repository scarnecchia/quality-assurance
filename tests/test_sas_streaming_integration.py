"""Integration tests for streaming SAS-to-Parquet conversion with real SCDM data."""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq
import pyreadstat
import pytest

from scdm_qa.schemas import get_schema
from scdm_qa.validation.cross_table import (
    _SCDM_TYPE_MAP,
    _convert_sas_to_parquet,
)

_DATA_DIR = os.environ.get("SCDM_SAS_DATA_DIR", "")
_DATA_PATH = Path(_DATA_DIR) if _DATA_DIR else None
_HAS_DATA = (
    _DATA_PATH is not None
    and _DATA_PATH.is_dir()
    and any(_DATA_PATH.glob("*.sas7bdat"))
)

pytestmark = pytest.mark.skipif(
    not _HAS_DATA,
    reason="SCDM_SAS_DATA_DIR not set or contains no SAS files",
)


class TestSasStreamingIntegration:
    """Integration tests for real SAS-to-Parquet streaming conversion."""

    @staticmethod
    def _find_sas_file() -> tuple[Path, str]:
        """Find the first available SAS file and derive table_key from filename.

        Returns:
            Tuple of (file_path, table_key) where table_key is the filename stem.

        Raises:
            RuntimeError: If no SAS files are found.
        """
        assert _DATA_PATH is not None, "Data path not available"
        sas_files = list(_DATA_PATH.glob("*.sas7bdat"))
        if not sas_files:
            raise RuntimeError(f"No .sas7bdat files found in {_DATA_PATH}")
        file_path = sas_files[0]
        table_key = file_path.stem
        return file_path, table_key

    def test_real_sas_converts_with_correct_row_count(self) -> None:
        """Test GH-6.AC5.1 & GH-6.AC5.2: Real SAS file converts with correct row count.

        Verifies:
        - Conversion succeeds with small chunk_size forcing multi-chunk writes
        - Output row count matches source SAS row count
        """
        sas_path, table_key = self._find_sas_file()

        # Read source row count via pyreadstat
        _, meta = pyreadstat.read_sas7bdat(str(sas_path), metadataonly=True)
        source_row_count = meta.number_rows

        # Convert with small chunk_size to force multi-chunk writes
        parquet_path = _convert_sas_to_parquet(
            sas_path, chunk_size=100, table_key=table_key
        )

        try:
            # Read back and verify row count
            parquet_meta = pq.read_metadata(str(parquet_path))
            output_row_count = parquet_meta.num_rows

            assert output_row_count == source_row_count, (
                f"Row count mismatch: source={source_row_count}, "
                f"output={output_row_count}"
            )
        finally:
            parquet_path.unlink()

    def test_real_sas_produces_multiple_row_groups(self) -> None:
        """Test GH-6.AC5.1: Real SAS file produces multiple Parquet row groups.

        With chunk_size=100, files with >100 rows should produce multiple
        row groups, demonstrating streaming writes were used.
        """
        sas_path, table_key = self._find_sas_file()

        # Read source to check if it's large enough for multiple chunks
        _, meta = pyreadstat.read_sas7bdat(str(sas_path), metadataonly=True)
        if meta.number_rows <= 100:
            pytest.skip(
                f"SAS file has {meta.number_rows} rows, too small for multi-chunk test"
            )

        # Convert with small chunk_size
        parquet_path = _convert_sas_to_parquet(
            sas_path, chunk_size=100, table_key=table_key
        )

        try:
            # Read back and verify row group count
            parquet_meta = pq.read_metadata(str(parquet_path))
            row_group_count = parquet_meta.num_row_groups

            assert row_group_count > 1, (
                f"Expected multiple row groups with chunk_size=100, "
                f"got {row_group_count}"
            )
        finally:
            parquet_path.unlink()

    def test_real_sas_output_schema_matches_canonical_types(self) -> None:
        """Test GH-6.AC5.1: Output schema uses canonical SCDM types.

        Verifies that columns from SCDM spec use canonical types
        (Numeric -> float64, Character -> utf8).
        """
        sas_path, table_key = self._find_sas_file()

        # Get SCDM spec for this table
        try:
            table_schema = get_schema(table_key)
        except KeyError:
            pytest.skip(f"No SCDM spec for table_key={table_key}")

        # Convert
        parquet_path = _convert_sas_to_parquet(
            sas_path, chunk_size=100, table_key=table_key
        )

        try:
            # Read back and verify schema
            parquet_meta = pq.read_metadata(str(parquet_path))
            schema = parquet_meta.schema.to_arrow_schema()

            assert schema is not None
            assert len(schema) > 0, "Schema should have at least one field"

            # Build lookup of field name to type
            schema_lookup = {field.name: field.type for field in schema}

            # For each column in SCDM spec, verify its output type is canonical
            for col in table_schema.columns:
                if col.name in schema_lookup:
                    expected_type = _SCDM_TYPE_MAP.get(col.col_type)
                    if expected_type is not None:
                        actual_type = schema_lookup[col.name]
                        assert actual_type == expected_type, (
                            f"Column {col.name}: expected {expected_type}, "
                            f"got {actual_type}"
                        )
        finally:
            parquet_path.unlink()

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from scdm_qa.readers import UnsupportedFormatError, create_reader
from scdm_qa.readers.base import TableReader
from scdm_qa.readers.parquet import ParquetReader
from scdm_qa.readers.sas import SasReader


class TestParquetReader:
    @pytest.fixture()
    def sample_parquet(self, tmp_path: Path) -> Path:
        df = pl.DataFrame({
            "PatID": [f"P{i}" for i in range(100)],
            "Value": list(range(100)),
        })
        path = tmp_path / "test.parquet"
        df.write_parquet(path)
        return path

    def test_metadata_returns_correct_columns(self, sample_parquet: Path) -> None:
        reader = ParquetReader(sample_parquet)
        meta = reader.metadata()
        assert meta.column_names == ("PatID", "Value")
        assert meta.row_count == 100
        assert meta.file_format == "parquet"

    def test_chunks_yields_all_rows(self, sample_parquet: Path) -> None:
        reader = ParquetReader(sample_parquet, chunk_size=30)
        total_rows = sum(chunk.height for chunk in reader.chunks())
        assert total_rows == 100

    def test_chunks_respects_chunk_size(self, sample_parquet: Path) -> None:
        reader = ParquetReader(sample_parquet, chunk_size=30)
        chunks = list(reader.chunks())
        assert len(chunks) > 1, "expected multiple chunks for 100 rows at chunk_size=30"
        for chunk in chunks[:-1]:
            assert chunk.height == 30
        assert chunks[-1].height <= 30

    def test_implements_table_reader_protocol(self, sample_parquet: Path) -> None:
        reader = ParquetReader(sample_parquet)
        assert isinstance(reader, TableReader)


class TestCreateReader:
    def test_selects_parquet_reader(self, tmp_path: Path) -> None:
        path = tmp_path / "test.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(path)
        reader = create_reader(path)
        assert isinstance(reader, ParquetReader)

    def test_raises_on_unsupported_format(self, tmp_path: Path) -> None:
        path = tmp_path / "test.csv"
        path.touch()
        with pytest.raises(UnsupportedFormatError, match="unsupported file format"):
            create_reader(path)

    def test_selects_sas_reader_for_sas_extension(self, tmp_path: Path) -> None:
        path = tmp_path / "test.sas7bdat"
        path.touch()
        reader = create_reader(path)
        assert isinstance(reader, SasReader)


class TestSasReader:
    def test_implements_table_reader_protocol(self) -> None:
        # SasReader must implement the TableReader protocol structurally
        assert issubclass(SasReader, TableReader)

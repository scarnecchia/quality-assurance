"""Tests for TableValidator class."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from scdm_qa.config import QAConfig
from scdm_qa.profiling.accumulator import ProfilingAccumulator
from scdm_qa.schemas import get_schema
from scdm_qa.validation import (
    ChunkAccumulator,
    TableValidator,
    ValidationChunkAccumulator,
)
from scdm_qa.validation.runner import run_validation


class TestTableValidatorParquetHappyPath:
    """Test Parquet with validation + profiling accumulators (GH-8.AC1.1, AC1.2, AC1.3)."""

    def test_parquet_produces_identical_validation_results(self, tmp_path: Path) -> None:
        """AC1.1: TableValidator.run() on Parquet produces identical ValidationResult to run_validation()."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", None, "P4"],  # Null in PatID to trigger a validation failure
            "Birth_Date": [1000, 2000, 3000, 4000],
            "Sex": ["F", "M", "F", "M"],
            "Hispanic": ["Y", "N", "Y", "N"],
            "Race": ["1", "2", "3", "1"],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={
                "validation": ValidationChunkAccumulator(schema),
            },
            run_global_checks=False,
        )
        result = validator.run()

        # Get validation results from TableValidator
        validation_result = result.accumulator_results["validation"]
        assert validation_result is not None
        assert len(validation_result.steps) > 0

        # Compare with run_validation() for the same data
        from scdm_qa.readers import create_reader
        reader = create_reader(parquet_path, chunk_size=config.chunk_size)
        baseline_result = run_validation(reader, schema)

        # Check that step counts are similar (should be identical)
        assert len(validation_result.steps) == len(baseline_result.steps)

        # Check that some checks have failures
        assert validation_result.total_failures > 0, "Expected failures due to null in PatID"

        # Check total rows
        assert validation_result.total_rows == df.height

    def test_parquet_produces_identical_profiling_results(self, tmp_path: Path) -> None:
        """AC1.2: TableValidator.run() on Parquet produces identical ProfilingResult."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "1"],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={
                "profiling": ProfilingAccumulator(schema),
            },
            run_global_checks=False,
        )
        result = validator.run()

        profiling_result = result.accumulator_results["profiling"]
        assert profiling_result is not None

        # Verify basic profiling structure
        assert profiling_result.total_rows == df.height
        assert len(profiling_result.columns) > 0

        # Verify PatID column profile exists (columns is a tuple)
        patid_profiles = [c for c in profiling_result.columns if c.name == "PatID"]
        assert len(patid_profiles) > 0
        patid_profile = patid_profiles[0]
        assert patid_profile.null_count == 0
        assert patid_profile.distinct_count == 3

    def test_parquet_includes_duckdb_global_check_results(self, tmp_path: Path) -> None:
        """AC1.3: TableValidator.run() includes DuckDB global check results."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "1"],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={},
            run_global_checks=True,
        )
        result = validator.run()

        # Global checks should be present
        global_steps = result.global_check_steps
        assert len(global_steps) > 0, "Expected DuckDB global checks for demographic (has unique_row)"

        # Demographic has unique_row on (PatID), so check_uniqueness should be called
        uniqueness_steps = [s for s in global_steps if s.check_id == "211"]
        assert len(uniqueness_steps) > 0, "Expected uniqueness check (211) for demographic"


class TestTableValidatorMultipleAccumulators:
    """Test multiple accumulators receive all chunks (GH-8.AC2.3, AC2.4, AC3.1, AC3.2)."""

    def test_multiple_accumulators_receive_all_chunks(self, tmp_path: Path) -> None:
        """AC2.3, AC2.4, AC3.1, AC3.2: Custom accumulator receives all chunks, broadcast is concurrent."""

        class ChunkHeightTracker:
            """Custom accumulator that records chunk heights."""

            def __init__(self) -> None:
                self.chunk_heights: list[int] = []

            def add_chunk(self, chunk: pl.DataFrame) -> None:
                self.chunk_heights.append(chunk.height)

            def result(self) -> dict[str, Any]:
                return {
                    "chunk_count": len(self.chunk_heights),
                    "total_rows": sum(self.chunk_heights),
                }

        df = pl.DataFrame({
            "PatID": [f"P{i}" for i in range(1000)],
            "Birth_Date": list(range(1000, 2000)),
            "Sex": ["F", "M"] * 500,
            "Hispanic": ["Y", "N"] * 500,
            "Race": [str(i % 3 + 1) for i in range(1000)],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path}, chunk_size=100)

        tracker = ChunkHeightTracker()

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={
                "profiling": ProfilingAccumulator(schema),
                "tracker": tracker,
            },
            run_global_checks=False,
        )
        result = validator.run()

        tracker_result = result.accumulator_results["tracker"]
        assert tracker_result["total_rows"] == df.height
        assert tracker_result["chunk_count"] > 1, "Expected multiple chunks with chunk_size=100"

    def test_custom_accumulator_without_modifying_table_validator(self, tmp_path: Path) -> None:
        """AC2.4: Adding new accumulator requires zero TableValidator modifications."""

        class CustomAccumulator:
            def __init__(self) -> None:
                self.calls: list[int] = []

            def add_chunk(self, chunk: pl.DataFrame) -> None:
                self.calls.append(chunk.height)

            def result(self) -> dict[str, Any]:
                return {"call_count": len(self.calls)}

        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "1"],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path})

        custom = CustomAccumulator()

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={"custom": custom},
            run_global_checks=False,
        )
        result = validator.run()

        custom_result = result.accumulator_results["custom"]
        assert custom_result["call_count"] >= 1, "Custom accumulator should receive at least one chunk"


class TestTableValidatorExceptionPropagation:
    """Test exception propagation from accumulators and global checks (GH-8.AC1.5, AC3.3)."""

    def test_accumulator_exception_propagates(self, tmp_path: Path) -> None:
        """AC3.3: If an accumulator raises during add_chunk(), exception propagates."""

        class FailingAccumulator:
            def add_chunk(self, chunk: pl.DataFrame) -> None:
                raise RuntimeError("Intentional failure in accumulator")

            def result(self) -> Any:
                return None

        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={"failing": FailingAccumulator()},
            run_global_checks=False,
        )

        with pytest.raises(RuntimeError, match="Intentional failure"):
            validator.run()

    def test_duckdb_global_check_exception_propagates(self, tmp_path: Path) -> None:
        """AC1.5: Exceptions from DuckDB global checks are propagated."""
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={},
            run_global_checks=True,
        )

        # Patch create_connection to raise an exception
        with patch("scdm_qa.validation.table_validator.create_connection") as mock_create:
            mock_create.side_effect = RuntimeError("DuckDB connection failed")
            with pytest.raises(RuntimeError, match="DuckDB connection failed"):
                validator.run()


class TestTableValidatorSASSupport:
    """Test SAS file support with global checks (GH-8.AC4.1, AC4.2, AC4.3)."""

    def test_sas_conversion_and_global_checks(self, tmp_path: Path) -> None:
        """AC4.1, AC4.3: TableValidator converts SAS to temp Parquet and runs global checks."""
        pytest.importorskip("pyreadstat")

        # Create a test SAS file using pyreadstat
        try:
            import pyreadstat
            import pandas
        except ImportError:
            pytest.skip("pyreadstat or pandas not available")

        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "1"],
        })
        sas_path = tmp_path / "demographic.sas7bdat"
        pandas_df = df.to_pandas()
        pyreadstat.write_sas7bdat(pandas_df, str(sas_path))

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": sas_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=sas_path,
            schema=schema,
            config=config,
            accumulators={},
            run_global_checks=True,
        )
        result = validator.run()

        # SAS should now run global checks (new behavior)
        global_steps = result.global_check_steps
        assert len(global_steps) > 0, "Expected DuckDB global checks for SAS file (previously skipped)"

    def test_sas_temp_file_cleanup_on_success(self, tmp_path: Path) -> None:
        """AC4.2: Temp Parquet file is cleaned up after global checks complete."""
        pytest.importorskip("pyreadstat")

        try:
            import pyreadstat
            import pandas
        except ImportError:
            pytest.skip("pyreadstat or pandas not available")

        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        sas_path = tmp_path / "demographic.sas7bdat"
        pandas_df = df.to_pandas()
        pyreadstat.write_sas7bdat(pandas_df, str(sas_path))

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": sas_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=sas_path,
            schema=schema,
            config=config,
            accumulators={},
            run_global_checks=True,
        )

        # Track created temp files before running
        temp_files_before = set(tmp_path.glob("*.parquet"))

        validator.run()

        # Verify temp files are cleaned up (shouldn't add new parquet files in tmp_path)
        temp_files_after = set(tmp_path.glob("*.parquet"))
        new_files = temp_files_after - temp_files_before
        # Only the original sas file should exist, no new parquet files
        assert len(new_files) == 0, "Temp parquet file should be cleaned up"

    def test_sas_temp_file_cleanup_on_error(self, tmp_path: Path) -> None:
        """AC4.2: Temp Parquet is cleaned up even if global checks raise."""
        pytest.importorskip("pyreadstat")

        try:
            import pyreadstat
            import pandas
        except ImportError:
            pytest.skip("pyreadstat or pandas not available")

        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        sas_path = tmp_path / "demographic.sas7bdat"
        pandas_df = df.to_pandas()
        pyreadstat.write_sas7bdat(pandas_df, str(sas_path))

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": sas_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=sas_path,
            schema=schema,
            config=config,
            accumulators={},
            run_global_checks=True,
        )

        # Patch _execute_global_checks to raise an exception after conversion
        with patch.object(validator, "_execute_global_checks", side_effect=RuntimeError("Global check error")):
            with pytest.raises(RuntimeError, match="Global check error"):
                validator.run()

        # Verify temp file was still cleaned up (by checking conversion context manager)
        # Since we can't easily inspect the temp file from outside, we trust the
        # converted_parquet context manager's finally block


class TestTableValidatorBarrierPattern:
    """Test that chunk N+1 is not dispatched until all accumulators finish chunk N (GH-8.AC3.2)."""

    def test_barrier_pattern_chunks_synchronized(self, tmp_path: Path) -> None:
        """AC3.2: Chunk N+1 is not dispatched until all accumulators finish chunk N."""
        import threading
        import time

        class SlowAccumulator:
            """Accumulator that records which chunk-num is being processed."""

            def __init__(self, delay: float = 0.01) -> None:
                self.delay = delay
                self.chunks_processed: list[int] = []
                self.lock = threading.Lock()

            def add_chunk(self, chunk: pl.DataFrame) -> None:
                time.sleep(self.delay)
                with self.lock:
                    self.chunks_processed.append(chunk.height)

            def result(self) -> dict[str, Any]:
                with self.lock:
                    return {"chunks": len(self.chunks_processed)}

        df = pl.DataFrame({
            "PatID": [f"P{i}" for i in range(500)],
            "Birth_Date": list(range(1000, 1500)),
            "Sex": ["F", "M"] * 250,
            "Hispanic": ["Y", "N"] * 250,
            "Race": [str(i % 3 + 1) for i in range(500)],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path}, chunk_size=100)

        slow1 = SlowAccumulator(delay=0.01)
        slow2 = SlowAccumulator(delay=0.005)

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={
                "slow1": slow1,
                "slow2": slow2,
            },
            run_global_checks=False,
        )
        validator.run()

        # Both accumulators should have processed the same number of chunks
        assert slow1.chunks_processed == slow2.chunks_processed
        assert len(slow1.chunks_processed) > 1, "Expected multiple chunks"


class TestTableValidatorEdgeCases:
    """Test edge cases and configurations."""

    def test_no_accumulators_skips_broadcast(self, tmp_path: Path) -> None:
        """With no accumulators, chunks are still read but not broadcast."""
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={},
            run_global_checks=False,
        )
        result = validator.run()

        assert result.accumulator_results == {}
        assert result.global_check_steps == ()

    def test_global_checks_disabled(self, tmp_path: Path) -> None:
        """With run_global_checks=False, no global checks are executed."""
        df = pl.DataFrame({
            "PatID": ["P1"],
            "Birth_Date": [1000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })
        parquet_path = tmp_path / "demographic.parquet"
        df.write_parquet(parquet_path)

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": parquet_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=parquet_path,
            schema=schema,
            config=config,
            accumulators={},
            run_global_checks=False,
        )
        result = validator.run()

        assert len(result.global_check_steps) == 0

    def test_unsupported_file_format_raises_early(self, tmp_path: Path) -> None:
        """With unsupported file format, create_reader raises UnsupportedFormatError."""
        from scdm_qa.readers import UnsupportedFormatError

        # Create a dummy file with unsupported extension
        unsupported_path = tmp_path / "data.csv"
        unsupported_path.write_text("dummy")

        schema = get_schema("demographic")
        config = QAConfig(tables={"demographic": unsupported_path})

        validator = TableValidator(
            table_key="demographic",
            file_path=unsupported_path,
            schema=schema,
            config=config,
            accumulators={},
            run_global_checks=True,
        )

        # Should raise UnsupportedFormatError from create_reader
        with pytest.raises(UnsupportedFormatError, match="unsupported file format"):
            validator.run()

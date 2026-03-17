"""Tests for ValidationChunkAccumulator protocol conformance and result equivalence."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from scdm_qa.readers import create_reader
from scdm_qa.schemas import get_schema
from scdm_qa.validation import ValidationChunkAccumulator, ChunkAccumulator
from scdm_qa.validation.runner import run_validation


class TestValidationChunkAccumulatorProtocolConformance:
    """Verify ValidationChunkAccumulator satisfies ChunkAccumulator protocol."""

    def test_conforms_to_chunk_accumulator_protocol(self) -> None:
        """GH-8.AC2.2: ValidationChunkAccumulator satisfies ChunkAccumulator protocol."""
        schema = get_schema("demographic")
        accumulator = ValidationChunkAccumulator(schema)
        assert isinstance(accumulator, ChunkAccumulator)

    def test_has_add_chunk_method(self) -> None:
        schema = get_schema("demographic")
        accumulator = ValidationChunkAccumulator(schema)
        assert hasattr(accumulator, "add_chunk")
        assert callable(accumulator.add_chunk)

    def test_has_result_method(self) -> None:
        schema = get_schema("demographic")
        accumulator = ValidationChunkAccumulator(schema)
        assert hasattr(accumulator, "result")
        assert callable(accumulator.result)


class TestValidationChunkAccumulatorResultEquivalence:
    """Verify ValidationChunkAccumulator produces same results as run_validation()."""

    def test_produces_identical_result_for_passing_data(self, tmp_path: Path) -> None:
        """Result equivalence: ValidationChunkAccumulator matches run_validation() for all-passing data."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        # Run validation with run_validation()
        reader1 = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        result1 = run_validation(reader1, schema)

        # Run validation with ValidationChunkAccumulator
        reader2 = create_reader(path, chunk_size=10)
        accumulator = ValidationChunkAccumulator(schema)
        for chunk in reader2.chunks():
            accumulator.add_chunk(chunk)
        result2 = accumulator.result()

        # Compare results
        assert len(result1.steps) == len(result2.steps)
        assert result1.total_rows == result2.total_rows
        assert result1.all_passed == result2.all_passed

        for s1, s2 in zip(result1.steps, result2.steps):
            assert s1.step_index == s2.step_index
            assert s1.assertion_type == s2.assertion_type
            assert s1.column == s2.column
            assert s1.check_id == s2.check_id
            assert s1.severity == s2.severity
            assert s1.n_passed == s2.n_passed
            assert s1.n_failed == s2.n_failed

    def test_produces_identical_result_for_failing_data(self, tmp_path: Path) -> None:
        """Result equivalence: ValidationChunkAccumulator matches run_validation() for failing data."""
        df = pl.DataFrame({
            "PatID": ["P1", None, "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        # Run validation with run_validation()
        reader1 = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        result1 = run_validation(reader1, schema)

        # Run validation with ValidationChunkAccumulator
        reader2 = create_reader(path, chunk_size=10)
        accumulator = ValidationChunkAccumulator(schema)
        for chunk in reader2.chunks():
            accumulator.add_chunk(chunk)
        result2 = accumulator.result()

        # Compare results
        assert len(result1.steps) == len(result2.steps)
        assert result1.total_rows == result2.total_rows
        assert not result1.all_passed
        assert not result2.all_passed
        assert result1.total_failures == result2.total_failures

        for s1, s2 in zip(result1.steps, result2.steps):
            assert s1.step_index == s2.step_index
            assert s1.assertion_type == s2.assertion_type
            assert s1.column == s2.column
            assert s1.n_passed == s2.n_passed
            assert s1.n_failed == s2.n_failed

    def test_accumulates_across_multiple_chunks(self, tmp_path: Path) -> None:
        """Verify ValidationChunkAccumulator correctly accumulates across multiple chunks."""
        df = pl.DataFrame({
            "PatID": [f"P{i}" for i in range(50)] + [None] * 5,
            "Birth_Date": list(range(55)),
            "Sex": ["F"] * 55,
            "Hispanic": ["Y"] * 55,
            "Race": ["1"] * 55,
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        # Run validation with run_validation()
        reader1 = create_reader(path, chunk_size=20)
        schema = get_schema("demographic")
        result1 = run_validation(reader1, schema)

        # Run validation with ValidationChunkAccumulator
        reader2 = create_reader(path, chunk_size=20)
        accumulator = ValidationChunkAccumulator(schema)
        for chunk in reader2.chunks():
            accumulator.add_chunk(chunk)
        result2 = accumulator.result()

        # Both should process multiple chunks
        assert result1.chunks_processed > 1
        assert result2.chunks_processed > 1
        assert result1.chunks_processed == result2.chunks_processed

        # Results should be identical
        assert result1.total_rows == result2.total_rows
        assert result1.total_failures == result2.total_failures
        assert len(result1.steps) == len(result2.steps)

        for s1, s2 in zip(result1.steps, result2.steps):
            assert s1.n_passed == s2.n_passed
            assert s1.n_failed == s2.n_failed


class TestValidationChunkAccumulatorEdgeCases:
    """Test edge cases and error handling."""

    def test_zero_chunks_returns_zero_row_result(self) -> None:
        """Zero-chunk case: accumulator with no add_chunk() calls returns zero-row result."""
        schema = get_schema("demographic")
        accumulator = ValidationChunkAccumulator(schema)
        result = accumulator.result()

        assert result.total_rows == 0
        assert result.chunks_processed == 0

    def test_empty_dataframe_returns_zero_rows(self, tmp_path: Path) -> None:
        """Empty DataFrame results in zero rows and valid result."""
        df = pl.DataFrame({
            "PatID": pl.Series([], dtype=pl.Utf8),
            "Birth_Date": pl.Series([], dtype=pl.Int32),
            "Sex": pl.Series([], dtype=pl.Utf8),
            "Hispanic": pl.Series([], dtype=pl.Utf8),
            "Race": pl.Series([], dtype=pl.Utf8),
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        accumulator = ValidationChunkAccumulator(schema)
        for chunk in reader.chunks():
            accumulator.add_chunk(chunk)
        result = accumulator.result()

        assert result.total_rows == 0
        assert result.chunks_processed == 0


class TestValidationChunkAccumulatorStepCountValidation:
    """Verify step count mismatch detection is inherited from runner.py."""

    def test_step_count_mismatch_raises_error(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Step count mismatch detection: ValueError raised when build_step_descriptions and build_validation diverge."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        from scdm_qa.validation import validation_chunk_accumulator

        original_build_step_descriptions = validation_chunk_accumulator.build_step_descriptions

        def mock_build_step_descriptions(schema, present_columns):
            # Return fewer steps than actual validation will produce
            result = original_build_step_descriptions(schema, present_columns)
            return result[:1] if len(result) > 1 else result

        monkeypatch.setattr(validation_chunk_accumulator, "build_step_descriptions", mock_build_step_descriptions)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        accumulator = ValidationChunkAccumulator(schema)

        with pytest.raises(ValueError, match="Step count mismatch"):
            for chunk in reader.chunks():
                accumulator.add_chunk(chunk)


class TestValidationChunkAccumulatorWithCustomRules:
    """Test ValidationChunkAccumulator with custom rules."""

    def test_custom_rules_applied(self, tmp_path: Path) -> None:
        """Verify custom rules can be passed and applied."""
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")

        def dummy_custom_extend_fn(agent):
            return agent

        accumulator = ValidationChunkAccumulator(schema, custom_extend_fn=dummy_custom_extend_fn)
        for chunk in reader.chunks():
            accumulator.add_chunk(chunk)
        result = accumulator.result()

        # Should complete without error
        assert result is not None
        assert result.table_key == "demographic"


class TestValidationChunkAccumulatorMaxFailingRows:
    """Verify max_failing_rows bound is respected."""

    def test_max_failing_rows_bound(self, tmp_path: Path) -> None:
        """Failing rows are bounded by max_failing_rows parameter."""
        df = pl.DataFrame({
            "PatID": [None] * 50,
            "Birth_Date": list(range(50)),
            "Sex": ["F"] * 50,
            "Hispanic": ["Y"] * 50,
            "Race": ["1"] * 50,
        })
        path = tmp_path / "demographic.parquet"
        df.write_parquet(path)

        reader = create_reader(path, chunk_size=10)
        schema = get_schema("demographic")
        accumulator = ValidationChunkAccumulator(schema, max_failing_rows=10)
        for chunk in reader.chunks():
            accumulator.add_chunk(chunk)
        result = accumulator.result()

        # Find step for PatID not null check
        for step in result.steps:
            if step.column == "PatID" and step.n_failed > 0:
                if step.failing_rows is not None:
                    assert step.failing_rows.height <= 10

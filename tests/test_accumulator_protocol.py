"""Tests for ChunkAccumulator protocol."""

from __future__ import annotations

from typing import Any

import polars as pl
import pytest

from scdm_qa.profiling.accumulator import ProfilingAccumulator
from scdm_qa.schemas import get_schema
from scdm_qa.validation import ChunkAccumulator


class TestChunkAccumulatorProtocol:
    def test_protocol_is_importable(self) -> None:
        assert ChunkAccumulator is not None

    def test_profiling_accumulator_satisfies_protocol(self) -> None:
        schema = get_schema("demographic")
        acc = ProfilingAccumulator(schema)
        assert isinstance(acc, ChunkAccumulator)

    def test_minimal_class_satisfies_protocol(self) -> None:
        class MinimalAccumulator:
            def add_chunk(self, chunk: pl.DataFrame) -> None:
                pass

            def result(self) -> Any:
                return None

        acc = MinimalAccumulator()
        assert isinstance(acc, ChunkAccumulator)

    def test_class_missing_add_chunk_does_not_satisfy(self) -> None:
        class MissingAddChunk:
            def result(self) -> Any:
                return None

        acc = MissingAddChunk()
        assert not isinstance(acc, ChunkAccumulator)

    def test_class_missing_result_does_not_satisfy(self) -> None:
        class MissingResult:
            def add_chunk(self, chunk: pl.DataFrame) -> None:
                pass

        acc = MissingResult()
        assert not isinstance(acc, ChunkAccumulator)

    def test_protocol_works_with_profiling_accumulator_in_practice(self) -> None:
        schema = get_schema("demographic")
        acc: ChunkAccumulator = ProfilingAccumulator(schema)

        chunk1 = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        })
        chunk2 = pl.DataFrame({
            "PatID": ["P3"],
            "Birth_Date": [3000],
            "Sex": ["F"],
            "Hispanic": ["Y"],
            "Race": ["1"],
        })

        acc.add_chunk(chunk1)
        acc.add_chunk(chunk2)
        result = acc.result()

        assert result is not None

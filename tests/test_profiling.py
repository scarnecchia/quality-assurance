from __future__ import annotations

import polars as pl

from scdm_qa.profiling.accumulator import ProfilingAccumulator
from scdm_qa.schemas import get_schema


class TestCompletenessRate:
    def test_computes_completeness_across_chunks(self) -> None:
        schema = get_schema("demographic")
        acc = ProfilingAccumulator(schema)

        acc.add_chunk(pl.DataFrame({
            "PatID": ["P1", None, "P3"],
            "Birth_Date": [1000, 2000, 3000],
            "Sex": ["F", "M", "F"],
            "Hispanic": ["Y", "N", "Y"],
            "Race": ["1", "2", "3"],
        }))

        result = acc.result()
        patid_profile = next(c for c in result.columns if c.name == "PatID")
        assert patid_profile.null_count == 1
        assert patid_profile.total_count == 3
        assert abs(patid_profile.completeness_pct - 66.67) < 1.0


class TestValueDistribution:
    def test_tracks_enum_frequencies(self) -> None:
        schema = get_schema("demographic")
        acc = ProfilingAccumulator(schema)

        acc.add_chunk(pl.DataFrame({
            "PatID": ["P1", "P2", "P3", "P4"],
            "Birth_Date": [1, 2, 3, 4],
            "Sex": ["F", "F", "M", "F"],
            "Hispanic": ["Y", "N", "Y", "Y"],
            "Race": ["1", "2", "1", "3"],
        }))

        result = acc.result()
        sex_profile = next(c for c in result.columns if c.name == "Sex")
        assert sex_profile.value_frequencies is not None
        assert sex_profile.value_frequencies["F"] == 3
        assert sex_profile.value_frequencies["M"] == 1


class TestDateRange:
    def test_tracks_min_max_across_chunks(self) -> None:
        schema = get_schema("demographic")
        acc = ProfilingAccumulator(schema)

        acc.add_chunk(pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }))
        acc.add_chunk(pl.DataFrame({
            "PatID": ["P3", "P4"],
            "Birth_Date": [500, 3000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }))

        result = acc.result()
        bdate = next(c for c in result.columns if c.name == "Birth_Date")
        assert bdate.min_value == "500"
        assert bdate.max_value == "3000"


class TestCardinality:
    def test_counts_distinct_across_chunks(self) -> None:
        schema = get_schema("demographic")
        acc = ProfilingAccumulator(schema)

        acc.add_chunk(pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1, 2],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }))
        acc.add_chunk(pl.DataFrame({
            "PatID": ["P2", "P3"],  # P2 is duplicate
            "Birth_Date": [3, 4],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }))

        result = acc.result()
        patid = next(c for c in result.columns if c.name == "PatID")
        assert patid.distinct_count == 3  # P1, P2, P3

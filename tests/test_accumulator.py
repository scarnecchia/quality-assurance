from __future__ import annotations

import polars as pl

from scdm_qa.validation.accumulator import ValidationAccumulator


class TestAccumulatorSumsAcrossChunks:
    def test_sums_pass_fail_counts(self) -> None:
        acc = ValidationAccumulator("test", "Test Table")

        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null", None, None)],
            n_passed={1: 90},
            n_failed={1: 10},
            extracts={},
        )
        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null", None, None)],
            n_passed={1: 95},
            n_failed={1: 5},
            extracts={},
        )

        result = acc.result()
        assert result.total_rows == 200
        assert result.chunks_processed == 2
        assert result.steps[0].n_passed == 185
        assert result.steps[0].n_failed == 15


class TestAccumulatorBoundsFailingRows:
    def test_caps_failing_rows_at_limit(self) -> None:
        acc = ValidationAccumulator("test", "Test Table", max_failing_rows=5)

        large_extract = pl.DataFrame({"PatID": [f"P{i}" for i in range(10)]})
        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null", None, None)],
            n_passed={1: 90},
            n_failed={1: 10},
            extracts={1: large_extract},
        )

        result = acc.result()
        assert result.steps[0].failing_rows is not None
        assert result.steps[0].failing_rows.height <= 5


class TestAccumulatorMultipleSteps:
    def test_tracks_independent_steps(self) -> None:
        acc = ValidationAccumulator("test", "Test Table")

        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[
                (1, "col_vals_not_null", "PatID", "PatID not null", None, None),
                (2, "col_vals_in_set", "Sex", "Sex in set", None, None),
            ],
            n_passed={1: 100, 2: 95},
            n_failed={1: 0, 2: 5},
            extracts={},
        )

        result = acc.result()
        assert len(result.steps) == 2
        assert result.steps[0].n_failed == 0
        assert result.steps[1].n_failed == 5
        assert not result.all_passed
        assert result.total_failures == 5


class TestAccumulatorPropagatesCheckId:
    def test_check_id_none_for_standard_checks(self) -> None:
        acc = ValidationAccumulator("test", "Test Table")
        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_not_null", "PatID", "PatID not null", None, None)],
            n_passed={1: 100},
            n_failed={1: 0},
            extracts={},
        )
        result = acc.result()
        assert result.steps[0].check_id is None
        assert result.steps[0].severity is None

    def test_check_id_preserved_when_set(self) -> None:
        acc = ValidationAccumulator("test", "Test Table")
        acc.add_chunk_results(
            chunk_row_count=100,
            step_descriptions=[(1, "col_vals_regex", "NDC", "NDC leading spaces", "122", "Warn")],
            n_passed={1: 95},
            n_failed={1: 5},
            extracts={},
        )
        result = acc.result()
        assert result.steps[0].check_id == "122"
        assert result.steps[0].severity == "Warn"

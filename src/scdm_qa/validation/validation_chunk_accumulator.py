"""Per-chunk validation accumulator conforming to ChunkAccumulator protocol."""

from __future__ import annotations

import pointblank as pb
import polars as pl
import structlog

from scdm_qa.schemas.custom_rules import ExtendFn
from scdm_qa.schemas.models import TableSchema
from scdm_qa.schemas.validation import build_validation
from scdm_qa.validation.accumulator import ValidationAccumulator
from scdm_qa.validation.results import ValidationResult
from scdm_qa.validation.runner import build_step_descriptions

log = structlog.get_logger(__name__)


class ValidationChunkAccumulator:
    """Wraps build_validation -> interrogate -> accumulate behind ChunkAccumulator."""

    def __init__(
        self,
        schema: TableSchema,
        *,
        thresholds: pb.Thresholds | None = None,
        max_failing_rows: int = 500,
        custom_extend_fn: ExtendFn | None = None,
    ) -> None:
        self._schema = schema
        self._thresholds = thresholds
        self._max_failing_rows = max_failing_rows
        self._custom_extend_fn = custom_extend_fn
        self._accumulator = ValidationAccumulator(
            table_key=schema.table_key,
            table_name=schema.table_name,
            max_failing_rows=max_failing_rows,
        )
        self._step_descriptions: list[tuple[int, str, str, str, str | None, str | None]] = []
        self._chunk_num = 0

    def add_chunk(self, chunk: pl.DataFrame) -> None:
        self._chunk_num += 1

        log.info(
            "validating chunk",
            table=self._schema.table_key,
            chunk=self._chunk_num,
            rows=chunk.height,
        )

        if self._chunk_num == 1:
            self._step_descriptions = build_step_descriptions(
                self._schema, set(chunk.columns)
            )

        validation = build_validation(
            chunk, self._schema, thresholds=self._thresholds
        )
        if self._custom_extend_fn is not None:
            from scdm_qa.schemas.custom_rules import apply_custom_rules
            validation = apply_custom_rules(
                validation, chunk, self._custom_extend_fn
            )

        result = validation.interrogate(
            collect_extracts=True,
            extract_limit=self._max_failing_rows,
        )

        n_passed = result.n_passed()
        n_failed = result.n_failed()

        if self._chunk_num == 1:
            num_descs = len(self._step_descriptions)
            num_results = len(n_passed)
            if num_descs != num_results:
                raise ValueError(
                    f"Step count mismatch for table '{self._schema.table_key}': "
                    f"build_step_descriptions generated {num_descs} steps, "
                    f"but build_validation produced {num_results} steps in pointblank results. "
                    f"This indicates the two code paths have drifted. "
                    f"Both must iterate columns and rules in the same order."
                )

        extracts: dict[int, pl.DataFrame] = {}
        for step_idx in n_failed:
            failed_count = n_failed[step_idx]
            if failed_count is not None and failed_count > 0:
                extract = result.get_data_extracts(i=step_idx, frame=True)
                if (
                    extract is not None
                    and hasattr(extract, "height")
                    and extract.height > 0
                ):
                    extracts[step_idx] = extract

        self._accumulator.add_chunk_results(
            chunk_row_count=chunk.height,
            step_descriptions=self._step_descriptions,
            n_passed=n_passed,
            n_failed=n_failed,
            extracts=extracts,
        )

    def result(self) -> ValidationResult:
        final = self._accumulator.result()
        if final.chunks_processed == 0:
            log.warning(
                "validation found no chunks to process",
                table=self._schema.table_key,
            )
        log.info(
            "validation complete",
            table=self._schema.table_key,
            total_rows=final.total_rows,
            chunks=final.chunks_processed,
            total_failures=final.total_failures,
        )
        return final

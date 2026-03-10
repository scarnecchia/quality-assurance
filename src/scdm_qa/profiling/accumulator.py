from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

import polars as pl

from scdm_qa.profiling.results import ColumnProfile, ProfilingResult
from scdm_qa.schemas.models import TableSchema


@dataclass
class _ColumnAccum:
    name: str
    col_type: str
    total_count: int = 0
    null_count: int = 0
    distinct_values: set[str] = field(default_factory=set)
    min_value: object = None
    max_value: object = None
    value_counter: Counter = field(default_factory=Counter)
    is_enumerated: bool = False
    max_distinct_track: int = 10_000


class ProfilingAccumulator:
    def __init__(
        self,
        schema: TableSchema,
        *,
        max_distinct_track: int = 10_000,
    ) -> None:
        self._schema = schema
        self._max_distinct_track = max_distinct_track
        self._total_rows = 0
        self._columns: dict[str, _ColumnAccum] = {}

        for col_def in schema.columns:
            is_enum = col_def.allowed_values is not None
            self._columns[col_def.name] = _ColumnAccum(
                name=col_def.name,
                col_type=col_def.col_type,
                is_enumerated=is_enum,
                max_distinct_track=max_distinct_track,
            )

    def add_chunk(self, chunk: pl.DataFrame) -> None:
        self._total_rows += chunk.height

        for col_name, accum in self._columns.items():
            if col_name not in chunk.columns:
                accum.total_count += chunk.height
                accum.null_count += chunk.height
                continue

            series = chunk[col_name]
            accum.total_count += len(series)
            accum.null_count += series.null_count()

            non_null = series.drop_nulls()
            if len(non_null) == 0:
                continue

            if accum.is_enumerated:
                for val, count in non_null.value_counts().iter_rows():
                    accum.value_counter[str(val)] += count

            if len(accum.distinct_values) < accum.max_distinct_track:
                new_vals = {str(v) for v in non_null.unique().to_list()}
                accum.distinct_values.update(new_vals)

            chunk_min = non_null.min()
            chunk_max = non_null.max()

            if accum.min_value is None or (chunk_min is not None and chunk_min < accum.min_value):
                accum.min_value = chunk_min
            if accum.max_value is None or (chunk_max is not None and chunk_max > accum.max_value):
                accum.max_value = chunk_max

    def result(self) -> ProfilingResult:
        profiles: list[ColumnProfile] = []
        for col_def in self._schema.columns:
            accum = self._columns.get(col_def.name)
            if accum is None:
                continue

            value_freqs = dict(accum.value_counter) if accum.is_enumerated and accum.value_counter else None

            profiles.append(
                ColumnProfile(
                    name=accum.name,
                    col_type=accum.col_type,
                    total_count=accum.total_count,
                    null_count=accum.null_count,
                    distinct_count=len(accum.distinct_values),
                    min_value=str(accum.min_value) if accum.min_value is not None else None,
                    max_value=str(accum.max_value) if accum.max_value is not None else None,
                    value_frequencies=value_freqs,
                )
            )

        return ProfilingResult(
            table_key=self._schema.table_key,
            table_name=self._schema.table_name,
            total_rows=self._total_rows,
            columns=tuple(profiles),
        )

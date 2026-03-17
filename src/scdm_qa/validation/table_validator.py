"""Table-level L1 validation orchestrator with concurrent chunk broadcasting."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import structlog

from scdm_qa.config import QAConfig
from scdm_qa.readers import create_reader
from scdm_qa.readers.conversion import converted_parquet
from scdm_qa.schemas.checks import (
    get_date_ordering_checks_for_table,
    get_not_populated_checks_for_table,
)
from scdm_qa.schemas.models import TableSchema
from scdm_qa.validation.accumulator_protocol import ChunkAccumulator
from scdm_qa.validation.duckdb_utils import create_connection
from scdm_qa.validation.global_checks import (
    check_cause_of_death,
    check_date_ordering,
    check_enc_combinations,
    check_enrollment_gaps,
    check_not_populated,
    check_overlapping_spans,
    check_sort_order,
    check_uniqueness,
)
from scdm_qa.validation.results import StepResult

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class TableValidatorResult:
    """Combined output from all accumulators plus global checks."""

    accumulator_results: dict[str, Any]
    global_check_steps: tuple[StepResult, ...]


class TableValidator:
    """Orchestrates L1 per-table validation: chunk broadcasting + DuckDB global checks."""

    def __init__(
        self,
        table_key: str,
        file_path: Path,
        schema: TableSchema,
        config: QAConfig,
        accumulators: dict[str, ChunkAccumulator],
        *,
        run_global_checks: bool = True,
    ) -> None:
        self._table_key = table_key
        self._file_path = file_path
        self._schema = schema
        self._config = config
        self._accumulators = accumulators
        self._run_global_checks = run_global_checks

    def run(self) -> TableValidatorResult:
        reader = create_reader(self._file_path, chunk_size=self._config.chunk_size)

        self._broadcast_chunks(reader)

        global_steps = self._execute_all_global_checks() if self._run_global_checks else []

        accumulator_results = {
            name: acc.result() for name, acc in self._accumulators.items()
        }

        return TableValidatorResult(
            accumulator_results=accumulator_results,
            global_check_steps=tuple(global_steps),
        )

    def _broadcast_chunks(self, reader: Any) -> None:
        """Fan out each chunk to all accumulators concurrently."""
        acc_list = list(self._accumulators.values())
        if not acc_list:
            for _ in reader.chunks():
                pass
            return

        with ThreadPoolExecutor(max_workers=len(acc_list)) as executor:
            for chunk_num, chunk in enumerate(reader.chunks(), start=1):
                futures = {
                    executor.submit(acc.add_chunk, chunk): name
                    for name, acc in self._accumulators.items()
                }
                for future in as_completed(futures):
                    future.result()

    def _execute_all_global_checks(self) -> list[StepResult]:
        """Execute DuckDB global checks against a Parquet view of the full table."""
        global_steps: list[StepResult] = []
        is_parquet = self._file_path.suffix.lower() == ".parquet"
        is_sas = self._file_path.suffix.lower() == ".sas7bdat"

        if is_parquet:
            self._execute_global_checks(self._file_path, global_steps)
        elif is_sas:
            with converted_parquet(
                self._file_path,
                self._config.chunk_size,
                table_key=self._table_key,
            ) as parquet_path:
                self._execute_global_checks(parquet_path, global_steps)
        else:
            log.warning(
                "skipping global checks for unsupported format",
                table=self._table_key,
                file=str(self._file_path),
            )

        return global_steps

    def _execute_global_checks(
        self, parquet_path: Path, global_steps: list[StepResult]
    ) -> None:
        """Register Parquet as DuckDB view and run all applicable checks."""
        conn: duckdb.DuckDBPyConnection | None = None
        try:
            conn = create_connection(
                memory_limit=self._config.duckdb_memory_limit,
                threads=self._config.duckdb_threads,
                temp_directory=self._config.duckdb_temp_directory,
            )
            safe_path = str(parquet_path).replace("'", "''")
            conn.execute(
                f'CREATE VIEW "{self._table_key}" AS SELECT * FROM '
                f"read_parquet('{safe_path}')"
            )
            log.debug("registered global check view", table=self._table_key)

            schema = self._schema
            table_key = self._table_key
            max_rows = self._config.max_failing_rows

            if schema.unique_row:
                step = check_uniqueness(conn, table_key, schema, max_failing_rows=max_rows)
                if step is not None:
                    global_steps.append(step)

            if schema.sort_order:
                step = check_sort_order(conn, table_key, schema)
                if step is not None:
                    global_steps.append(step)

            if get_not_populated_checks_for_table(schema.table_key):
                global_steps.extend(check_not_populated(conn, table_key, schema))

            if get_date_ordering_checks_for_table(schema.table_key):
                global_steps.extend(
                    check_date_ordering(conn, table_key, schema, max_failing_rows=max_rows)
                )

            if schema.table_key == "cause_of_death":
                global_steps.extend(
                    check_cause_of_death(conn, table_key, schema, max_failing_rows=max_rows)
                )

            if schema.table_key == "enrollment":
                step = check_overlapping_spans(conn, table_key, schema, max_failing_rows=max_rows)
                if step is not None:
                    global_steps.append(step)
                step = check_enrollment_gaps(conn, table_key, schema, max_failing_rows=max_rows)
                if step is not None:
                    global_steps.append(step)

            if schema.table_key == "encounter":
                global_steps.extend(
                    check_enc_combinations(conn, table_key, schema, max_failing_rows=max_rows)
                )

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as e:
                    log.warning("failed to close DuckDB connection", error=str(e))

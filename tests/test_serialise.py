from __future__ import annotations

import json
from datetime import datetime

import polars as pl

from scdm_qa.profiling.results import ColumnProfile, ProfilingResult
from scdm_qa.reporting.serialise import (
    serialise_profiling,
    serialise_run,
    serialise_step,
    serialise_validation,
)
from scdm_qa.validation.results import StepResult, ValidationResult


def _make_validation_result(
    *, with_failures: bool = False, check_id: str | None = None, severity: str | None = None
) -> ValidationResult:
    failing_rows = None
    n_failed = 0
    if with_failures:
        failing_rows = pl.DataFrame({"PatID": [None, None]})
        n_failed = 2

    return ValidationResult(
        table_key="demographic",
        table_name="Demographic Table",
        steps=(
            StepResult(
                step_index=1,
                assertion_type="col_vals_not_null",
                column="PatID",
                description="PatID not null",
                n_passed=98,
                n_failed=n_failed,
                failing_rows=failing_rows,
                check_id=check_id,
                severity=severity,
            ),
        ),
        total_rows=100,
        chunks_processed=1,
    )


def _make_profiling_result() -> ProfilingResult:
    return ProfilingResult(
        table_key="demographic",
        table_name="Demographic Table",
        total_rows=100,
        columns=(
            ColumnProfile(
                name="PatID",
                col_type="Character",
                total_count=100,
                null_count=2,
                distinct_count=98,
                min_value="P001",
                max_value="P100",
                value_frequencies=None,
            ),
        ),
    )


class TestSerialiseStep:
    def test_serialise_step_with_no_failures(self) -> None:
        """Test serialise_step with zero failures."""
        step = StepResult(
            step_index=1,
            assertion_type="col_vals_not_null",
            column="PatID",
            description="PatID not null",
            n_passed=100,
            n_failed=0,
            failing_rows=None,
            check_id="101",
            severity="Fail",
        )
        result = serialise_step(step, max_failing_rows=500)
        assert result["check_id"] == "101"
        assert result["n_passed"] == 100
        assert result["n_failed"] == 0
        assert result["failing_rows"] == []

    def test_serialise_step_pass_rate_calculation(self) -> None:
        """Test that pass_rate is calculated correctly from f_passed."""
        step = StepResult(
            step_index=1,
            assertion_type="col_vals_not_null",
            column="PatID",
            description="PatID not null",
            n_passed=98,
            n_failed=2,
            failing_rows=None,
        )
        result = serialise_step(step, max_failing_rows=500)
        assert abs(result["pass_rate"] - 0.98) < 0.001

    def test_serialise_step_truncates_failing_rows(self) -> None:
        """Test that failing_rows are truncated to max_failing_rows."""
        failing_rows = pl.DataFrame(
            {
                "PatID": [f"P{i:03d}" for i in range(20)],
                "Issue": ["missing"] * 20,
            }
        )
        step = StepResult(
            step_index=1,
            assertion_type="col_vals_not_null",
            column="PatID",
            description="PatID not null",
            n_passed=80,
            n_failed=20,
            failing_rows=failing_rows,
        )
        result = serialise_step(step, max_failing_rows=5)
        assert len(result["failing_rows"]) == 5

    def test_serialise_step_none_check_id_and_severity(self) -> None:
        """Test that None check_id and severity are preserved."""
        step = StepResult(
            step_index=1,
            assertion_type="col_vals_not_null",
            column="PatID",
            description="PatID not null",
            n_passed=100,
            n_failed=0,
            failing_rows=None,
            check_id=None,
            severity=None,
        )
        result = serialise_step(step, max_failing_rows=500)
        assert result["check_id"] is None
        assert result["severity"] is None


class TestSerialiseValidation:
    def test_serialise_validation_basic(self) -> None:
        """Test serialise_validation produces correct structure."""
        vr = _make_validation_result()
        result = serialise_validation(vr, max_failing_rows=500)
        assert result["table_key"] == "demographic"
        assert result["table_name"] == "Demographic Table"
        assert result["total_rows"] == 100
        assert result["chunks_processed"] == 1
        assert len(result["steps"]) == 1

    def test_serialise_validation_steps_are_serialised(self) -> None:
        """Test that steps within validation are properly serialised."""
        vr = _make_validation_result(check_id="101", severity="Warn")
        result = serialise_validation(vr, max_failing_rows=500)
        assert result["steps"][0]["check_id"] == "101"
        assert result["steps"][0]["severity"] == "Warn"


class TestSerialiseProfileing:
    def test_serialise_profiling_basic(self) -> None:
        """Test serialise_profiling produces correct structure."""
        pr = _make_profiling_result()
        result = serialise_profiling(pr)
        assert result["table_key"] == "demographic"
        assert result["table_name"] == "Demographic Table"
        assert result["total_rows"] == 100
        assert len(result["columns"]) == 1

    def test_serialise_profiling_column_fields(self) -> None:
        """Test that column fields are correctly serialised."""
        pr = _make_profiling_result()
        result = serialise_profiling(pr)
        col = result["columns"][0]
        assert col["name"] == "PatID"
        assert col["col_type"] == "Character"
        assert col["total_count"] == 100
        assert col["null_count"] == 2
        assert col["distinct_count"] == 98
        assert col["min_value"] == "P001"
        assert col["max_value"] == "P100"

    def test_serialise_profiling_completeness(self) -> None:
        """Test that completeness and completeness_pct are calculated correctly."""
        pr = _make_profiling_result()
        result = serialise_profiling(pr)
        col = result["columns"][0]
        assert abs(col["completeness"] - 0.98) < 0.001
        assert abs(col["completeness_pct"] - 98.0) < 0.1

    def test_serialise_profiling_empty_columns(self) -> None:
        """Test serialise_profiling with empty columns (cross-table case)."""
        pr = ProfilingResult(
            table_key="cross_table",
            table_name="Cross-Table Checks",
            total_rows=0,
            columns=(),
        )
        result = serialise_profiling(pr)
        assert result["columns"] == []


class TestSerialiseRun:
    def test_serialise_run_schema_version(self) -> None:
        """Test that schema_version is set to '1.0'."""
        vr = _make_validation_result()
        pr = _make_profiling_result()
        result = serialise_run([(vr, pr)])
        assert result["schema_version"] == "1.0"

    def test_serialise_run_generated_at_is_iso(self) -> None:
        """Test that generated_at is a valid ISO timestamp."""
        vr = _make_validation_result()
        pr = _make_profiling_result()
        result = serialise_run([(vr, pr)])
        assert "generated_at" in result
        # Should not raise
        datetime.fromisoformat(result["generated_at"])

    def test_serialise_run_json_serialisable(self) -> None:
        """Test that output is JSON-serialisable (AC5.1)."""
        vr = _make_validation_result(with_failures=True)
        pr = _make_profiling_result()
        result = serialise_run([(vr, pr)])
        # Should not raise TypeError
        json.dumps(result)

    def test_serialise_run_tables_structure(self) -> None:
        """Test that tables dict contains validation and profiling."""
        vr = _make_validation_result()
        pr = _make_profiling_result()
        result = serialise_run([(vr, pr)])
        assert "demographic" in result["tables"]
        assert "validation" in result["tables"]["demographic"]
        assert "profiling" in result["tables"]["demographic"]

    def test_serialise_run_summary_counts(self) -> None:
        """Test that summary counts are calculated correctly."""
        vr = _make_validation_result(with_failures=True, check_id="101", severity="Fail")
        pr = _make_profiling_result()
        result = serialise_run([(vr, pr)])
        summary = result["summary"]
        assert summary["total_checks"] == 1
        assert summary["total_failures"] == 2

    def test_serialise_run_summary_by_severity(self) -> None:
        """Test that by_severity breakdown is correct."""
        vr = _make_validation_result(with_failures=True, check_id="101", severity="Fail")
        pr = _make_profiling_result()
        result = serialise_run([(vr, pr)])
        by_severity = result["summary"]["by_severity"]
        assert by_severity["Fail"] == 1
        assert by_severity["Warn"] == 0
        assert by_severity["Note"] == 0
        assert by_severity["pass"] == 0

    def test_serialise_run_summary_by_severity_pass(self) -> None:
        """Test that by_severity includes passing steps."""
        vr = _make_validation_result(with_failures=False, check_id="101", severity="Warn")
        pr = _make_profiling_result()
        result = serialise_run([(vr, pr)])
        by_severity = result["summary"]["by_severity"]
        assert by_severity["pass"] == 1

    def test_serialise_run_multiple_tables(self) -> None:
        """Test that multiple tables are all present in output."""
        vr1 = _make_validation_result()
        pr1 = _make_profiling_result()

        vr2 = ValidationResult(
            table_key="encounter",
            table_name="Encounter Table",
            steps=(
                StepResult(
                    step_index=1,
                    assertion_type="col_vals_not_null",
                    column="EncID",
                    description="EncID not null",
                    n_passed=950,
                    n_failed=50,
                    failing_rows=None,
                    check_id="102",
                    severity="Fail",
                ),
            ),
            total_rows=1000,
            chunks_processed=2,
        )
        pr2 = ProfilingResult(
            table_key="encounter",
            table_name="Encounter Table",
            total_rows=1000,
            columns=(),
        )

        result = serialise_run([(vr1, pr1), (vr2, pr2)])
        assert "demographic" in result["tables"]
        assert "encounter" in result["tables"]
        assert result["summary"]["total_checks"] == 2
        assert result["summary"]["total_failures"] == 50

    def test_serialise_run_empty_results(self) -> None:
        """Test that empty results list produces valid structure."""
        result = serialise_run([])
        assert result["schema_version"] == "1.0"
        assert result["tables"] == {}
        assert result["summary"]["total_checks"] == 0
        assert result["summary"]["total_failures"] == 0


class TestSerialiseRunAC4:
    def test_ac4_1_schema_version_set(self) -> None:
        """qa-dashboard.AC4.1: JSON output includes schema_version field set to '1.0'."""
        vr = _make_validation_result()
        pr = _make_profiling_result()
        result = serialise_run([(vr, pr)])
        assert result["schema_version"] == "1.0"

    def test_ac4_2_failing_rows_truncated(self) -> None:
        """qa-dashboard.AC4.2: Failing rows truncated to max_failing_rows limit."""
        failing_rows = pl.DataFrame({"PatID": [f"P{i:03d}" for i in range(20)]})
        step = StepResult(
            step_index=1,
            assertion_type="col_vals_not_null",
            column="PatID",
            description="PatID not null",
            n_passed=80,
            n_failed=20,
            failing_rows=failing_rows,
            check_id="101",
            severity="Fail",
        )
        vr = ValidationResult(
            table_key="test",
            table_name="Test Table",
            steps=(step,),
            total_rows=100,
            chunks_processed=1,
        )
        pr = ProfilingResult(
            table_key="test",
            table_name="Test Table",
            total_rows=100,
            columns=(),
        )
        result = serialise_run([(vr, pr)], max_failing_rows=5)
        serialised_step = result["tables"]["test"]["validation"]["steps"][0]
        assert len(serialised_step["failing_rows"]) == 5

    def test_ac4_3_none_fields_handled_gracefully(self) -> None:
        """qa-dashboard.AC4.3: Serialisation handles null check_id and severity gracefully."""
        step = StepResult(
            step_index=1,
            assertion_type="col_vals_not_null",
            column="PatID",
            description="PatID not null",
            n_passed=100,
            n_failed=0,
            failing_rows=None,
            check_id=None,
            severity=None,
        )
        vr = ValidationResult(
            table_key="test",
            table_name="Test Table",
            steps=(step,),
            total_rows=100,
            chunks_processed=1,
        )
        pr = ProfilingResult(
            table_key="test",
            table_name="Test Table",
            total_rows=100,
            columns=(),
        )
        result = serialise_run([(vr, pr)])
        serialised_step = result["tables"]["test"]["validation"]["steps"][0]
        assert serialised_step["check_id"] is None
        assert serialised_step["severity"] is None


class TestSerialiseRunAC5:
    def test_ac5_1_json_serialisable(self) -> None:
        """qa-dashboard.AC5.1: JSON data is serialisable (embedded in <script> blocks)."""
        vr = _make_validation_result(with_failures=True)
        pr = _make_profiling_result()
        result = serialise_run([(vr, pr)])
        # Should not raise TypeError
        json_str = json.dumps(result)
        assert isinstance(json_str, str)
        # Should be parseable back
        parsed = json.loads(json_str)
        assert parsed["schema_version"] == "1.0"

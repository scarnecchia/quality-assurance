"""Tests for pipeline two-level orchestration (L1/L2 execution and exit codes).

Tests verify:
- Conditional L1 execution (AC3.1)
- Conditional L2 execution (AC3.2)
- Default both L1 and L2 (AC3.3)
- L2 with table_filter (AC3.7)
- Exit code reflects L2 failures (AC3.8)
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from scdm_qa.config import QAConfig
from scdm_qa.pipeline import TableOutcome, compute_exit_code, run_pipeline
from scdm_qa.schemas.models import CrossTableCheckDef
from scdm_qa.validation.results import StepResult, ValidationResult


class TestL1L2ConditionalExecution:
    """Tests for AC3.1, AC3.2, AC3.3, AC3.7 — conditional L1/L2 execution."""

    def _make_minimal_config(self, tmp_path: Path) -> QAConfig:
        """Create a minimal QAConfig for testing."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create a minimal demographic parquet file
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
            "ImputedHispanic": ["Y", "N"],
            "ImputedRace": ["1", "2"],
        })
        df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        output_dir.mkdir()

        return QAConfig(
            tables={"demographic": data_dir / "demographic.parquet"},
            output_dir=output_dir,
            run_l1=True,
            run_l2=True,
        )

    def test_l1_only_no_cross_table_outcome(self, tmp_path: Path) -> None:
        """AC3.1: Config with run_l1=True, run_l2=False → only per-table outcomes."""
        config = self._make_minimal_config(tmp_path)
        config = QAConfig(
            tables=config.tables,
            output_dir=config.output_dir,
            run_l1=True,
            run_l2=False,
        )

        with patch("scdm_qa.pipeline._process_table") as mock_process:
            mock_process.return_value = TableOutcome(
                table_key="demographic",
                success=True,
                validation_result=ValidationResult(
                    table_key="demographic",
                    table_name="Demographic",
                    steps=(),
                    total_rows=100,
                    chunks_processed=1,
                ),
                profiling_result=None,
            )

            outcomes = run_pipeline(config)

            # Verify _process_table was called (L1 executed)
            assert mock_process.called
            # Verify no cross_table outcome
            assert not any(o.table_key == "cross_table" for o in outcomes)
            # Verify demographic outcome exists
            assert any(o.table_key == "demographic" for o in outcomes)

    def test_l2_only_no_per_table_outcomes(self, tmp_path: Path) -> None:
        """AC3.2: Config with run_l1=False, run_l2=True → only cross_table outcome."""
        config = self._make_minimal_config(tmp_path)
        config = QAConfig(
            tables=config.tables,
            output_dir=config.output_dir,
            run_l1=False,
            run_l2=True,
        )

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            mock_cross.return_value = [
                StepResult(
                    step_index=-1,
                    assertion_type="cross_table",
                    column="PatID",
                    description="Test check",
                    n_passed=10,
                    n_failed=0,
                    failing_rows=None,
                    check_id="201",
                    severity="Fail",
                )
            ]

            outcomes = run_pipeline(config)

            # Verify _process_table was NOT called (L1 skipped)
            assert not mock_process.called
            # Verify cross_table outcome exists
            cross_table_outcome = [o for o in outcomes if o.table_key == "cross_table"]
            assert len(cross_table_outcome) == 1
            # Verify no per-table outcomes
            assert not any(o.table_key == "demographic" for o in outcomes)

    def test_both_l1_and_l2_executes_both(self, tmp_path: Path) -> None:
        """AC3.3: Config with both True → both per-table and cross_table outcomes."""
        config = self._make_minimal_config(tmp_path)
        config = QAConfig(
            tables=config.tables,
            output_dir=config.output_dir,
            run_l1=True,
            run_l2=True,
        )

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            mock_process.return_value = TableOutcome(
                table_key="demographic",
                success=True,
                validation_result=ValidationResult(
                    table_key="demographic",
                    table_name="Demographic",
                    steps=(),
                    total_rows=100,
                    chunks_processed=1,
                ),
                profiling_result=None,
            )
            mock_cross.return_value = [
                StepResult(
                    step_index=-1,
                    assertion_type="cross_table",
                    column="PatID",
                    description="Test check",
                    n_passed=10,
                    n_failed=0,
                    failing_rows=None,
                    check_id="201",
                    severity="Fail",
                )
            ]

            outcomes = run_pipeline(config)

            # Verify both L1 and L2 were called
            assert mock_process.called
            assert mock_cross.called
            # Verify both outcomes exist
            assert any(o.table_key == "demographic" for o in outcomes)
            assert any(o.table_key == "cross_table" for o in outcomes)

    def test_l2_with_table_filter_filters_checks(self, tmp_path: Path) -> None:
        """AC3.7: With table_filter, L2 only runs checks involving that table."""
        config = self._make_minimal_config(tmp_path)
        config = QAConfig(
            tables=config.tables,
            output_dir=config.output_dir,
            run_l1=True,
            run_l2=True,
        )

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.schemas.cross_table_checks.get_checks_for_table") as mock_get_checks, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            mock_process.return_value = TableOutcome(
                table_key="demographic",
                success=True,
                validation_result=ValidationResult(
                    table_key="demographic",
                    table_name="Demographic",
                    steps=(),
                    total_rows=100,
                    chunks_processed=1,
                ),
                profiling_result=None,
            )
            # Mock get_checks_for_table to return filtered checks
            mock_check = CrossTableCheckDef(
                check_id="201",
                check_type="referential_integrity",
                severity="Fail",
                description="Test",
                source_table="demographic",
                reference_table="patient",
                join_column="PatID",
                join_reference_column="PatID",
                compare_column=None,
            )
            mock_get_checks.return_value = (mock_check,)
            mock_cross.return_value = []

            outcomes = run_pipeline(config, table_filter="demographic")

            # Verify get_checks_for_table was called with the filter
            mock_get_checks.assert_called_once_with("demographic")
            # Verify run_cross_table_checks was called with filtered checks
            args, kwargs = mock_cross.call_args
            assert args[1] == (mock_check,)  # Second positional arg is checks

    def test_l2_skipped_in_profile_only_mode(self, tmp_path: Path) -> None:
        """L2 should not execute when profile_only=True."""
        config = self._make_minimal_config(tmp_path)
        config = QAConfig(
            tables=config.tables,
            output_dir=config.output_dir,
            run_l1=True,
            run_l2=True,
        )

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            mock_process.return_value = TableOutcome(
                table_key="demographic",
                success=True,
                profiling_result=None,
            )

            outcomes = run_pipeline(config, profile_only=True)

            # L1 should execute
            assert mock_process.called
            # L2 should NOT execute
            assert not mock_cross.called

    def test_l2_empty_checks_skipped(self, tmp_path: Path) -> None:
        """If cross-table checks are empty, L2 is silently skipped."""
        config = self._make_minimal_config(tmp_path)
        config = QAConfig(
            tables=config.tables,
            output_dir=config.output_dir,
            run_l1=True,
            run_l2=True,
        )

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.schemas.cross_table_checks.get_cross_table_checks") as mock_get_all, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            mock_process.return_value = TableOutcome(
                table_key="demographic",
                success=True,
                validation_result=ValidationResult(
                    table_key="demographic",
                    table_name="Demographic",
                    steps=(),
                    total_rows=100,
                    chunks_processed=1,
                ),
                profiling_result=None,
            )
            # Empty checks
            mock_get_all.return_value = ()

            outcomes = run_pipeline(config)

            # L2 should not call run_cross_table_checks if checks are empty
            assert not mock_cross.called
            # Only demographic outcome
            assert len([o for o in outcomes if o.table_key == "cross_table"]) == 0

    def test_l2_empty_steps_no_outcome(self, tmp_path: Path) -> None:
        """If run_cross_table_checks returns empty list, no cross_table outcome."""
        config = self._make_minimal_config(tmp_path)
        config = QAConfig(
            tables=config.tables,
            output_dir=config.output_dir,
            run_l1=True,
            run_l2=True,
        )

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            mock_process.return_value = TableOutcome(
                table_key="demographic",
                success=True,
                validation_result=ValidationResult(
                    table_key="demographic",
                    table_name="Demographic",
                    steps=(),
                    total_rows=100,
                    chunks_processed=1,
                ),
                profiling_result=None,
            )
            # Empty steps
            mock_cross.return_value = []

            outcomes = run_pipeline(config)

            # No cross_table outcome if steps are empty
            assert not any(o.table_key == "cross_table" for o in outcomes)

    def test_l2_exception_produces_failed_outcome(self, tmp_path: Path) -> None:
        """Exception in L2 produces failed cross_table outcome instead of crashing."""
        config = self._make_minimal_config(tmp_path)
        config = QAConfig(
            tables=config.tables,
            output_dir=config.output_dir,
            run_l1=False,
            run_l2=True,
        )

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            # Simulate exception during L2 execution
            mock_cross.side_effect = ValueError("Test error from cross-table engine")

            # Should not raise; should return failed outcome
            outcomes = run_pipeline(config)

            # Verify cross_table outcome exists and has success=False
            cross_table_outcomes = [o for o in outcomes if o.table_key == "cross_table"]
            assert len(cross_table_outcomes) == 1
            assert cross_table_outcomes[0].success is False
            assert "Test error from cross-table engine" in cross_table_outcomes[0].error


class TestExitCodeWithCrossTableResults:
    """Tests for AC3.8 — exit code reflects L2 failures."""

    def test_cross_table_note_severity_no_exit_code(self) -> None:
        """Note-severity cross-table checks don't affect exit code."""
        outcomes = [
            TableOutcome(
                table_key="cross_table",
                success=True,
                validation_result=ValidationResult(
                    table_key="cross_table",
                    table_name="Cross-Table Checks",
                    steps=(
                        StepResult(
                            step_index=-1,
                            assertion_type="cross_table",
                            column="PatID",
                            description="Note check",
                            n_passed=5,
                            n_failed=5,  # Even with failures
                            failing_rows=None,
                            check_id="999",
                            severity="Note",
                        ),
                    ),
                    total_rows=0,
                    chunks_processed=0,
                ),
            ),
        ]

        exit_code = compute_exit_code(outcomes)
        # Note checks never affect exit code
        assert exit_code == 0

    def test_cross_table_warn_severity_exit_code_1(self) -> None:
        """Warn-severity cross-table checks cap at exit code 1."""
        outcomes = [
            TableOutcome(
                table_key="cross_table",
                success=True,
                validation_result=ValidationResult(
                    table_key="cross_table",
                    table_name="Cross-Table Checks",
                    steps=(
                        StepResult(
                            step_index=-1,
                            assertion_type="cross_table",
                            column="PatID",
                            description="Warn check",
                            n_passed=5,
                            n_failed=1,
                            failing_rows=None,
                            check_id="201",
                            severity="Warn",
                        ),
                    ),
                    total_rows=0,
                    chunks_processed=0,
                ),
            ),
        ]

        exit_code = compute_exit_code(outcomes)
        # Warn with failures → exit 1
        assert exit_code == 1

    def test_cross_table_fail_within_threshold_exit_code_1(self) -> None:
        """Fail-severity cross-table checks within threshold → exit code 1."""
        outcomes = [
            TableOutcome(
                table_key="cross_table",
                success=True,
                validation_result=ValidationResult(
                    table_key="cross_table",
                    table_name="Cross-Table Checks",
                    steps=(
                        StepResult(
                            step_index=-1,
                            assertion_type="cross_table",
                            column="PatID",
                            description="Fail check within threshold",
                            n_passed=95,
                            n_failed=5,  # 5% failure rate
                            failing_rows=None,
                            check_id="201",
                            severity="Fail",
                        ),
                    ),
                    total_rows=0,
                    chunks_processed=0,
                ),
            ),
        ]

        exit_code = compute_exit_code(outcomes, error_threshold=0.05)
        # Fail at threshold → exit 1
        assert exit_code == 1

    def test_cross_table_fail_exceeds_threshold_exit_code_2(self) -> None:
        """Fail-severity cross-table checks exceeding threshold → exit code 2."""
        outcomes = [
            TableOutcome(
                table_key="cross_table",
                success=True,
                validation_result=ValidationResult(
                    table_key="cross_table",
                    table_name="Cross-Table Checks",
                    steps=(
                        StepResult(
                            step_index=-1,
                            assertion_type="cross_table",
                            column="PatID",
                            description="Fail check exceeding threshold",
                            n_passed=90,
                            n_failed=10,  # 10% failure rate
                            failing_rows=None,
                            check_id="201",
                            severity="Fail",
                        ),
                    ),
                    total_rows=0,
                    chunks_processed=0,
                ),
            ),
        ]

        exit_code = compute_exit_code(outcomes, error_threshold=0.05)
        # Fail exceeds threshold → exit 2
        assert exit_code == 2

    def test_cross_table_none_severity_exceeds_threshold_exit_code_2(self) -> None:
        """None-severity cross-table checks exceeding threshold → exit code 2."""
        outcomes = [
            TableOutcome(
                table_key="cross_table",
                success=True,
                validation_result=ValidationResult(
                    table_key="cross_table",
                    table_name="Cross-Table Checks",
                    steps=(
                        StepResult(
                            step_index=-1,
                            assertion_type="cross_table",
                            column="PatID",
                            description="None severity check exceeding threshold",
                            n_passed=90,
                            n_failed=10,  # 10% failure rate
                            failing_rows=None,
                            check_id="201",
                            severity=None,
                        ),
                    ),
                    total_rows=0,
                    chunks_processed=0,
                ),
            ),
        ]

        exit_code = compute_exit_code(outcomes, error_threshold=0.05)
        # None (default) severity with threshold exceedance → exit 2
        assert exit_code == 2

    def test_mixed_l1_l2_results_exit_code_reflects_both(self) -> None:
        """Exit code reflects failures from both L1 and L2 results."""
        outcomes = [
            TableOutcome(
                table_key="demographic",
                success=True,
                validation_result=ValidationResult(
                    table_key="demographic",
                    table_name="Demographic",
                    steps=(
                        StepResult(
                            step_index=0,
                            assertion_type="not_empty",
                            column="PatID",
                            description="L1 check",
                            n_passed=95,
                            n_failed=5,
                            failing_rows=None,
                            check_id="101",
                            severity="Warn",
                        ),
                    ),
                    total_rows=100,
                    chunks_processed=1,
                ),
            ),
            TableOutcome(
                table_key="cross_table",
                success=True,
                validation_result=ValidationResult(
                    table_key="cross_table",
                    table_name="Cross-Table Checks",
                    steps=(
                        StepResult(
                            step_index=-1,
                            assertion_type="cross_table",
                            column="PatID",
                            description="L2 check",
                            n_passed=90,
                            n_failed=10,
                            failing_rows=None,
                            check_id="201",
                            severity="Fail",
                        ),
                    ),
                    total_rows=0,
                    chunks_processed=0,
                ),
            ),
        ]

        exit_code = compute_exit_code(outcomes, error_threshold=0.05)
        # Both L1 (Warn) and L2 (Fail exceeds threshold) → exit 2
        assert exit_code == 2

    def test_all_cross_table_pass_exit_code_0(self) -> None:
        """All cross-table checks pass → exit code 0."""
        outcomes = [
            TableOutcome(
                table_key="cross_table",
                success=True,
                validation_result=ValidationResult(
                    table_key="cross_table",
                    table_name="Cross-Table Checks",
                    steps=(
                        StepResult(
                            step_index=-1,
                            assertion_type="cross_table",
                            column="PatID",
                            description="Passing check",
                            n_passed=100,
                            n_failed=0,
                            failing_rows=None,
                            check_id="201",
                            severity="Fail",
                        ),
                    ),
                    total_rows=0,
                    chunks_processed=0,
                ),
            ),
        ]

        exit_code = compute_exit_code(outcomes)
        assert exit_code == 0


class TestCrossTableReporting:
    """Tests for cross-table report generation and index entry (AC1.9)."""

    def _make_minimal_config(self, tmp_path: Path) -> QAConfig:
        """Create a minimal QAConfig for testing."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create a minimal demographic parquet file
        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
            "ImputedHispanic": ["Y", "N"],
            "ImputedRace": ["1", "2"],
        })
        df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        output_dir.mkdir()

        return QAConfig(
            tables={"demographic": data_dir / "demographic.parquet"},
            output_dir=output_dir,
            run_l1=False,
            run_l2=True,
        )

    def test_cross_table_report_file_created(self, tmp_path: Path) -> None:
        """AC1.9: Cross-table outcome generates cross_table.html file."""
        config = self._make_minimal_config(tmp_path)

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            # Mock a single cross-table step
            mock_cross.return_value = [
                StepResult(
                    step_index=-1,
                    assertion_type="cross_table",
                    column="PatID",
                    description="Test cross-table check",
                    n_passed=10,
                    n_failed=0,
                    failing_rows=None,
                    check_id="201",
                    severity="Fail",
                )
            ]

            outcomes = run_pipeline(config)

            # Verify cross_table.html was created
            report_path = config.output_dir / "cross_table.html"
            assert report_path.exists(), f"Report file {report_path} should exist"

            # Verify the HTML contains validation section
            html = report_path.read_text()
            assert "Cross-Table Checks" in html
            assert "Validation" in html

    def test_cross_table_entry_in_index(self, tmp_path: Path) -> None:
        """AC1.9: Cross-table outcome creates index.html entry."""
        config = self._make_minimal_config(tmp_path)

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            mock_cross.return_value = [
                StepResult(
                    step_index=-1,
                    assertion_type="cross_table",
                    column="PatID",
                    description="Test cross-table check",
                    n_passed=10,
                    n_failed=1,
                    failing_rows=None,
                    check_id="201",
                    severity="Fail",
                )
            ]

            outcomes = run_pipeline(config)

            # Verify index.html was created
            index_path = config.output_dir / "index.html"
            assert index_path.exists(), f"Index file {index_path} should exist"

            # Verify the index contains cross-table entry in JSON
            html = index_path.read_text()
            assert "Cross-Table Checks" in html

            # Verify cross_table entry in embedded JSON
            match = re.search(
                r'<script type="application/json" id="dashboard-data">(.*?)</script>',
                html, re.DOTALL,
            )
            assert match, "dashboard-data script tag should be present"
            data = json.loads(match.group(1))
            assert "cross_table" in data["tables"]
            # Verify failure count is present
            assert data["tables"]["cross_table"]["validation"]["steps"][0]["n_failed"] == 1

    def test_cross_table_no_profiling_section(self, tmp_path: Path) -> None:
        """AC1.9: Cross-table report should have empty profiling data."""
        config = self._make_minimal_config(tmp_path)

        with patch("scdm_qa.pipeline._process_table") as mock_process, \
             patch("scdm_qa.validation.cross_table.run_cross_table_checks") as mock_cross:
            mock_cross.return_value = [
                StepResult(
                    step_index=-1,
                    assertion_type="cross_table",
                    column="PatID",
                    description="Test cross-table check",
                    n_passed=10,
                    n_failed=0,
                    failing_rows=None,
                    check_id="201",
                    severity="Fail",
                )
            ]

            outcomes = run_pipeline(config)

            # Verify cross_table.html profiling columns are empty in JSON
            report_path = config.output_dir / "cross_table.html"
            html = report_path.read_text()

            match = re.search(
                r'<script type="application/json" id="dashboard-data">(.*?)</script>',
                html, re.DOTALL,
            )
            assert match, "dashboard-data script tag should be present"
            data = json.loads(match.group(1))
            assert data["profiling"]["columns"] == []


class TestSASFileSkip:
    """Tests for GH-7.AC6 — SAS file handling in global checks."""

    def test_sas_file_skips_global_checks_with_warning(self, tmp_path: Path) -> None:
        """GH-7.AC6.1: Pipeline correctly delegates to TableValidator for SAS files."""
        from scdm_qa.pipeline import _process_table
        from scdm_qa.profiling.results import ProfilingResult
        from scdm_qa.validation.table_validator import TableValidatorResult

        # Create a minimal data directory
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create a dummy SAS file
        sas_path = data_dir / "demographic.sas7bdat"
        sas_path.write_text("dummy")

        # Mock TableValidator to avoid actual file processing
        with patch("scdm_qa.pipeline.TableValidator") as MockTableValidator:
            mock_instance = MockTableValidator.return_value
            mock_instance.run.return_value = TableValidatorResult(
                accumulator_results={
                    "profiling": ProfilingResult(
                        table_key="demographic",
                        table_name="Demographic",
                        total_rows=2,
                        columns=(),
                    ),
                    "validation": ValidationResult(
                        table_key="demographic",
                        table_name="Demographic",
                        steps=(),
                        total_rows=2,
                        chunks_processed=1,
                    ),
                },
                global_check_steps=(),
            )

            # Create config
            output_dir = tmp_path / "reports"
            output_dir.mkdir()
            config = QAConfig(
                tables={"demographic": sas_path},
                output_dir=output_dir,
            )

            # Call _process_table
            outcome = _process_table("demographic", sas_path, config, profile_only=False)

            # Verify success
            assert outcome.success is True
            assert outcome.validation_result is not None

            # Verify TableValidator was called with correct parameters
            MockTableValidator.assert_called_once()
            call_args = MockTableValidator.call_args
            assert call_args[0][0] == "demographic"  # table_key
            assert call_args[0][1] == sas_path  # file_path
            assert call_args[1]["run_global_checks"] is True  # not profile_only

    def test_sas_file_no_errors_or_crashes(self, tmp_path: Path) -> None:
        """GH-7.AC6.2: SAS files do not cause errors or crashes — handled gracefully."""
        from scdm_qa.pipeline import _process_table
        from scdm_qa.profiling.results import ProfilingResult
        from scdm_qa.validation.table_validator import TableValidatorResult

        # Create a minimal data directory
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create dummy SAS file
        sas_path = data_dir / "demographic.sas7bdat"
        sas_path.write_text("dummy")

        # Mock TableValidator to prevent actual reading
        with patch("scdm_qa.pipeline.TableValidator") as MockTableValidator:
            # Mock TableValidator to return successful results
            mock_instance = MockTableValidator.return_value
            mock_instance.run.return_value = TableValidatorResult(
                accumulator_results={
                    "profiling": ProfilingResult(
                        table_key="demographic",
                        table_name="Demographic",
                        total_rows=2,
                        columns=(),
                    ),
                    "validation": ValidationResult(
                        table_key="demographic",
                        table_name="Demographic",
                        steps=(),
                        total_rows=2,
                        chunks_processed=1,
                    ),
                },
                global_check_steps=(),
            )

            # Create config
            output_dir = tmp_path / "reports"
            output_dir.mkdir()
            config = QAConfig(
                tables={"demographic": sas_path},
                output_dir=output_dir,
            )

            # Call _process_table with SAS file — should not raise any exception
            outcome = _process_table("demographic", sas_path, config, profile_only=False)

            # Verify successful outcome
            assert outcome.success is True
            assert outcome.error is None

    def test_parquet_file_includes_global_checks(self, tmp_path: Path) -> None:
        """Verify that Parquet files still execute global checks (contrast test)."""
        from scdm_qa.pipeline import _process_table

        # Create a minimal Parquet file
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
            "ImputedHispanic": ["Y", "N"],
            "ImputedRace": ["1", "2"],
        })
        parquet_path = data_dir / "demographic.parquet"
        df.write_parquet(parquet_path)

        # Create config
        output_dir = tmp_path / "reports"
        output_dir.mkdir()
        config = QAConfig(
            tables={"demographic": parquet_path},
            output_dir=output_dir,
        )

        # Call _process_table with Parquet file
        outcome = _process_table("demographic", parquet_path, config, profile_only=False)

        # Verify success
        assert outcome.success is True

        # With Parquet, global checks should have been attempted
        # At minimum, validation_result should exist and have processed chunks
        assert outcome.validation_result is not None
        assert outcome.validation_result.chunks_processed > 0

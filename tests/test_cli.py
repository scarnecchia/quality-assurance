from __future__ import annotations

from pathlib import Path

import polars as pl
from typer.testing import CliRunner

from scdm_qa.cli import app
from scdm_qa.pipeline import TableOutcome, compute_exit_code
from scdm_qa.validation.results import StepResult, ValidationResult

runner = CliRunner()


class TestCLIHelp:
    def test_help_shows_subcommands(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "profile" in result.output
        assert "schema" in result.output
        assert "serve" in result.output


class TestSchemaCommand:
    def test_lists_all_tables(self) -> None:
        result = runner.invoke(app, ["schema"])
        assert result.exit_code == 0
        assert "19 SCDM tables" in result.output

    def test_shows_specific_table(self) -> None:
        result = runner.invoke(app, ["schema", "demographic"])
        assert result.exit_code == 0
        assert "PatID" in result.output


class TestRunCommandL1L2Flags:
    """Tests for --l1-only and --l2-only CLI flags."""

    def _make_minimal_config(self, tmp_path: Path) -> Path:
        """Helper to create minimal test data and config."""
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
        df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\n'
        )
        return config_file

    def test_l1_only_flag_succeeds(self, tmp_path: Path) -> None:
        config_file = self._make_minimal_config(tmp_path)
        result = runner.invoke(app, ["run", str(config_file), "--l1-only"])
        assert result.exit_code == 0

    def test_l2_only_flag_succeeds(self, tmp_path: Path) -> None:
        config_file = self._make_minimal_config(tmp_path)
        result = runner.invoke(app, ["run", str(config_file), "--l2-only"])
        assert result.exit_code == 0

    def test_no_flags_succeeds(self, tmp_path: Path) -> None:
        config_file = self._make_minimal_config(tmp_path)
        result = runner.invoke(app, ["run", str(config_file)])
        assert result.exit_code == 0

    def test_l1_only_and_l2_only_together_exits_2(self, tmp_path: Path) -> None:
        config_file = self._make_minimal_config(tmp_path)
        result = runner.invoke(app, ["run", str(config_file), "--l1-only", "--l2-only"])
        assert result.exit_code == 2
        assert "mutually exclusive" in result.output

    def test_l1_only_overrides_toml_run_l2_true(self, tmp_path: Path) -> None:
        """--l1-only should override TOML config run_l1/run_l2 values."""
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
        df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\nrun_l1 = true\nrun_l2 = true\n'
        )

        # Without a way to directly inspect config passed to run_pipeline,
        # we can at least verify the command succeeds without error
        result = runner.invoke(app, ["run", str(config_file), "--l1-only"])
        assert result.exit_code == 0

    def test_l2_only_overrides_toml_config(self, tmp_path: Path) -> None:
        """--l2-only should override TOML config run_l1/run_l2 values."""
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
        df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\nrun_l1 = true\nrun_l2 = true\n'
        )

        result = runner.invoke(app, ["run", str(config_file), "--l2-only"])
        assert result.exit_code == 0


class TestRunCommand:
    def _make_config_and_data(self, tmp_path: Path, *, with_nulls: bool = False) -> Path:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        patids = ["P1", "P2", "P3", "P4", "P5"]
        if with_nulls:
            patids[2] = None

        df = pl.DataFrame({
            "PatID": patids,
            "Birth_Date": [1000, 2000, 3000, 4000, 5000],
            "Sex": ["F", "M", "F", "M", "F"],
            "Hispanic": ["Y", "N", "Y", "N", "Y"],
            "Race": ["1", "2", "3", "1", "2"],
            "ImputedHispanic": ["Y", "N", "U", "Y", "N"],
            "ImputedRace": ["1", "2", "3", "4", "5"],
        })
        df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\nchunk_size = 2\n'
        )
        return config_file

    def test_produces_reports_for_clean_data(self, tmp_path: Path) -> None:
        config_file = self._make_config_and_data(tmp_path)
        result = runner.invoke(app, ["run", str(config_file)])
        assert result.exit_code == 0
        assert (tmp_path / "reports" / "demographic.html").exists()
        assert (tmp_path / "reports" / "index.html").exists()

    def test_exit_code_2_when_failures_exceed_threshold(self, tmp_path: Path) -> None:
        config_file = self._make_config_and_data(tmp_path, with_nulls=True)
        result = runner.invoke(app, ["run", str(config_file)])
        # 1 null out of 5 = 20% failure rate > default 5% threshold → exit 2
        assert result.exit_code == 2

    def test_missing_config_exits_2(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["run", str(tmp_path / "missing.toml")])
        assert result.exit_code == 2

    def test_exit_code_1_when_failures_within_threshold(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create data with 1 null out of 100 rows = 1% failure rate < 5% threshold
        patids = ["P" + str(i) for i in range(1, 100)]
        patids.append(None)

        df = pl.DataFrame({
            "PatID": patids,
            "Birth_Date": list(range(1000, 1100)),
            "Sex": ["F" if i % 2 == 0 else "M" for i in range(100)],
            "Hispanic": ["Y" if i % 2 == 0 else "N" for i in range(100)],
            "Race": [str((i % 3) + 1) for i in range(100)],
            "ImputedHispanic": ["Y" if i % 2 == 0 else "N" for i in range(100)],
            "ImputedRace": [str((i % 5) + 1) for i in range(100)],
        })
        df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\nchunk_size = 10\n'
        )
        result = runner.invoke(app, ["run", str(config_file)])
        # 1 null out of 100 = 1% failure rate < 5% threshold → exit 1
        assert result.exit_code == 1


class TestRunCommandTableFilter:
    def test_table_filter_option(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create two tables
        pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
            "ImputedHispanic": ["Y", "N"],
            "ImputedRace": ["1", "2"],
        }).write_parquet(data_dir / "demographic.parquet")

        pl.DataFrame({
            "PatID": ["P1", "P2"],
            "EnrollmentID": ["E1", "E2"],
            "Enrollment_Date": [1000, 2000],
        }).write_parquet(data_dir / "enrollment.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\n'
            f'demographic = "{data_dir / "demographic.parquet"}"\n'
            f'enrollment = "{data_dir / "enrollment.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\n'
        )
        result = runner.invoke(app, ["run", str(config_file), "--table", "demographic"])
        assert result.exit_code == 0
        # Only demographic report should be created, not enrollment
        assert (tmp_path / "reports" / "demographic.html").exists()
        assert not (tmp_path / "reports" / "enrollment.html").exists()


class TestRunCommandTableIsolation:
    def test_one_table_failure_doesnt_block_others(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Good table
        pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }).write_parquet(data_dir / "demographic.parquet")

        # Bad table (missing file to trigger error)
        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\n'
            f'demographic = "{data_dir / "demographic.parquet"}"\n'
            f'enrollment = "{data_dir / "nonexistent.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\n'
        )
        result = runner.invoke(app, ["run", str(config_file)])
        # Should still create demographic report even though enrollment fails
        assert result.exit_code == 2
        assert "ERROR" in result.output
        assert (tmp_path / "reports" / "demographic.html").exists()


class TestProfileCommand:
    def test_runs_profiling_only(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }).write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\n'
        )
        result = runner.invoke(app, ["profile", str(config_file)])
        assert result.exit_code == 0
        assert "profiled" in result.output


class TestServeCommand:
    def test_nonexistent_dir_exits_2(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["serve", str(tmp_path / "nope")])
        assert result.exit_code == 2


class TestComputeExitCode:
    """Tests for compute_exit_code() severity-aware exit code computation."""

    def _make_step(
        self,
        *,
        n_failed: int = 0,
        n_passed: int = 10,
        severity: str | None = None,
        check_id: str | None = None,
    ) -> StepResult:
        """Helper to create a StepResult for testing."""
        return StepResult(
            step_index=1,
            assertion_type="col_vals_not_null",
            column="PatID",
            description="Test assertion",
            n_passed=n_passed,
            n_failed=n_failed,
            failing_rows=None,
            check_id=check_id,
            severity=severity,
        )

    def _make_validation_result(self, steps: list[StepResult]) -> ValidationResult:
        """Helper to create a ValidationResult."""
        total_rows = steps[0].n_total if steps else 0
        return ValidationResult(
            table_key="demographic",
            table_name="Demographic",
            steps=tuple(steps),
            total_rows=total_rows,
            chunks_processed=1,
        )

    def test_all_pass_returns_0(self) -> None:
        """Clean data with no failures → exit 0."""
        step = self._make_step(n_failed=0, n_passed=10)
        vr = self._make_validation_result([step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        assert compute_exit_code([outcome]) == 0

    def test_processing_error_returns_2(self) -> None:
        """Processing error (success=False) → exit 2."""
        outcome = TableOutcome(table_key="demographic", success=False, error="file not found")
        assert compute_exit_code([outcome]) == 2

    def test_note_severity_does_not_escalate(self) -> None:
        """Note-severity failures do not escalate exit code → exit 0."""
        step = self._make_step(
            n_failed=5,
            n_passed=5,
            severity="Note",
            check_id="111",
        )
        vr = self._make_validation_result([step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        assert compute_exit_code([outcome]) == 0

    def test_warn_severity_below_threshold_returns_1(self) -> None:
        """Warn-severity failures below threshold → exit 1."""
        step = self._make_step(
            n_failed=1,
            n_passed=99,
            severity="Warn",
            check_id="123",
        )
        vr = self._make_validation_result([step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        # 1% failure rate < 5% threshold
        assert compute_exit_code([outcome]) == 1

    def test_warn_severity_exceeds_threshold_returns_1(self) -> None:
        """Warn-severity failures exceeding threshold → exit 1 (caps at 1)."""
        step = self._make_step(
            n_failed=10,
            n_passed=90,
            severity="Warn",
            check_id="123",
        )
        vr = self._make_validation_result([step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        # 10% failure rate > 5% threshold, but Warn checks cap at exit 1
        assert compute_exit_code([outcome], error_threshold=0.05) == 1

    def test_fail_severity_below_threshold_returns_1(self) -> None:
        """Fail-severity failures below threshold → exit 1."""
        step = self._make_step(
            n_failed=1,
            n_passed=99,
            severity="Fail",
            check_id="226",
        )
        vr = self._make_validation_result([step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        # 1% failure rate < 5% threshold
        assert compute_exit_code([outcome]) == 1

    def test_fail_severity_exceeds_threshold_returns_2(self) -> None:
        """Fail-severity failures exceeding threshold → exit 2."""
        step = self._make_step(
            n_failed=10,
            n_passed=90,
            severity="Fail",
            check_id="226",
        )
        vr = self._make_validation_result([step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        # 10% failure rate > 5% threshold
        assert compute_exit_code([outcome], error_threshold=0.05) == 2

    def test_none_severity_behaves_like_fail(self) -> None:
        """None severity (backward compat) behaves like Fail."""
        step = self._make_step(
            n_failed=1,
            n_passed=99,
            severity=None,
        )
        vr = self._make_validation_result([step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        # 1% failure rate < 5% threshold → exit 1
        assert compute_exit_code([outcome]) == 1

    def test_none_severity_exceeds_threshold_returns_2(self) -> None:
        """None severity exceeding threshold → exit 2."""
        step = self._make_step(
            n_failed=10,
            n_passed=90,
            severity=None,
        )
        vr = self._make_validation_result([step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        # 10% failure rate > 5% threshold
        assert compute_exit_code([outcome], error_threshold=0.05) == 2

    def test_mixed_severities_note_and_fail(self) -> None:
        """Mixed: Note failures ignored, Fail failures below threshold → exit 1."""
        note_step = self._make_step(
            n_failed=5,
            n_passed=5,
            severity="Note",
            check_id="111",
        )
        fail_step = self._make_step(
            n_failed=1,
            n_passed=99,
            severity="Fail",
            check_id="226",
        )
        vr = self._make_validation_result([note_step, fail_step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        # Note is ignored, Fail at 1% < 5% → exit 1
        assert compute_exit_code([outcome]) == 1

    def test_mixed_severities_note_and_warn(self) -> None:
        """Mixed: Note failures ignored, Warn failures below threshold → exit 1."""
        note_step = self._make_step(
            n_failed=10,
            n_passed=10,
            severity="Note",
            check_id="111",
        )
        warn_step = self._make_step(
            n_failed=2,
            n_passed=98,
            severity="Warn",
            check_id="122",
        )
        vr = self._make_validation_result([note_step, warn_step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        # Note is ignored, Warn at 2% < 5% → exit 1
        assert compute_exit_code([outcome]) == 1

    def test_custom_error_threshold(self) -> None:
        """Custom error_threshold parameter is respected."""
        step = self._make_step(
            n_failed=5,
            n_passed=95,
            severity="Fail",
        )
        vr = self._make_validation_result([step])
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=vr)
        # 5% failure rate: below 10% threshold → exit 1
        assert compute_exit_code([outcome], error_threshold=0.10) == 1
        # 5% failure rate: equals 5% threshold (not > error_threshold) → exit 1
        assert compute_exit_code([outcome], error_threshold=0.05) == 1
        # 6% failure rate: exceeds 5% threshold → exit 2
        step_high = self._make_step(
            n_failed=6,
            n_passed=94,
            severity="Fail",
        )
        vr_high = self._make_validation_result([step_high])
        outcome_high = TableOutcome(table_key="demographic", success=True, validation_result=vr_high)
        assert compute_exit_code([outcome_high], error_threshold=0.05) == 2

    def test_multiple_tables_all_pass(self) -> None:
        """Multiple tables, all pass → exit 0."""
        step1 = self._make_step(n_failed=0, n_passed=10)
        vr1 = self._make_validation_result([step1])
        outcome1 = TableOutcome(table_key="demographic", success=True, validation_result=vr1)

        step2 = self._make_step(n_failed=0, n_passed=10)
        vr2 = self._make_validation_result([step2])
        outcome2 = TableOutcome(table_key="enrollment", success=True, validation_result=vr2)

        assert compute_exit_code([outcome1, outcome2]) == 0

    def test_multiple_tables_one_fails(self) -> None:
        """Multiple tables, one exceeds threshold → exit 2."""
        step1 = self._make_step(n_failed=0, n_passed=10)
        vr1 = self._make_validation_result([step1])
        outcome1 = TableOutcome(table_key="demographic", success=True, validation_result=vr1)

        step2 = self._make_step(n_failed=10, n_passed=90, severity="Fail")
        vr2 = self._make_validation_result([step2])
        outcome2 = TableOutcome(table_key="enrollment", success=True, validation_result=vr2)

        assert compute_exit_code([outcome1, outcome2], error_threshold=0.05) == 2

    def test_no_validation_result(self) -> None:
        """Outcome with no validation_result (profile-only) → exit 0."""
        outcome = TableOutcome(table_key="demographic", success=True, validation_result=None)
        assert compute_exit_code([outcome]) == 0

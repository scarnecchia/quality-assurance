from __future__ import annotations

from pathlib import Path

import polars as pl

from scdm_qa.profiling.results import ColumnProfile, ProfilingResult
from scdm_qa.reporting.builder import save_table_report
from scdm_qa.reporting.index import make_report_summary, save_index
from scdm_qa.validation.results import StepResult, ValidationResult


def _make_validation_result(*, with_failures: bool = False) -> ValidationResult:
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


class TestSaveTableReport:
    def test_creates_html_file(self, tmp_path: Path) -> None:
        vr = _make_validation_result()
        pr = _make_profiling_result()
        path = save_table_report(tmp_path, "demographic", vr, pr)
        assert path.exists()
        assert path.suffix == ".html"

    def test_html_contains_validation_section(self, tmp_path: Path) -> None:
        vr = _make_validation_result()
        pr = _make_profiling_result()
        path = save_table_report(tmp_path, "demographic", vr, pr)
        html = path.read_text()
        assert "Validation" in html
        assert "PatID" in html

    def test_html_contains_failing_rows_when_present(self, tmp_path: Path) -> None:
        vr = _make_validation_result(with_failures=True)
        pr = _make_profiling_result()
        path = save_table_report(tmp_path, "demographic", vr, pr)
        html = path.read_text()
        assert "Failing Row" in html

    def test_html_contains_placeholder_for_empty_steps(self, tmp_path: Path) -> None:
        vr = ValidationResult(
            table_key="demographic",
            table_name="Demographic Table",
            steps=(),
            total_rows=100,
            chunks_processed=1,
        )
        pr = _make_profiling_result()
        path = save_table_report(tmp_path, "demographic", vr, pr)
        html = path.read_text()
        assert "No validation steps" in html

    def test_html_contains_profiling_section(self, tmp_path: Path) -> None:
        vr = _make_validation_result()
        pr = _make_profiling_result()
        path = save_table_report(tmp_path, "demographic", vr, pr)
        html = path.read_text()
        assert "Profile" in html or "Completeness" in html


class TestSaveTableReport__CheckID:
    def test_renders_check_id_when_set(self, tmp_path: Path) -> None:
        """Verify that check_id is rendered in the validation table HTML."""
        failing_rows = pl.DataFrame({"PatID": [None]})
        vr = ValidationResult(
            table_key="demographic",
            table_name="Demographic Table",
            steps=(
                StepResult(
                    step_index=1,
                    assertion_type="col_vals_not_null",
                    column="PatID",
                    description="PatID not null",
                    n_passed=99,
                    n_failed=1,
                    failing_rows=failing_rows,
                    check_id="122",
                    severity="Fail",
                ),
            ),
            total_rows=100,
            chunks_processed=1,
        )
        pr = _make_profiling_result()
        path = save_table_report(tmp_path, "demographic", vr, pr)
        html = path.read_text()
        assert "122" in html, "check_id '122' should render in HTML"

    def test_renders_empty_dash_for_none_check_id(self, tmp_path: Path) -> None:
        """Verify that None check_id renders as '—' in the validation table."""
        vr = ValidationResult(
            table_key="demographic",
            table_name="Demographic Table",
            steps=(
                StepResult(
                    step_index=1,
                    assertion_type="col_vals_not_null",
                    column="PatID",
                    description="PatID not null",
                    n_passed=100,
                    n_failed=0,
                    failing_rows=None,
                    check_id=None,
                ),
            ),
            total_rows=100,
            chunks_processed=1,
        )
        pr = _make_profiling_result()
        path = save_table_report(tmp_path, "demographic", vr, pr)
        html = path.read_text()
        # Check that the empty-dash placeholder appears in the validation table
        assert "—" in html, "Empty check_id should render as '—'"


class TestSaveIndex:
    def test_creates_index_html(self, tmp_path: Path) -> None:
        summaries = [
            make_report_summary("demographic", "Demographic", 100, 5, 0),
            make_report_summary("encounter", "Encounter", 1000, 8, 3),
        ]
        path = save_index(tmp_path, summaries)
        assert path.exists()
        html = path.read_text()
        assert "demographic.html" in html
        assert "encounter.html" in html
        assert "PASS" in html
        assert "FAIL" in html

from __future__ import annotations

import json
import re
from pathlib import Path

import polars as pl

from scdm_qa.profiling.results import ColumnProfile, ProfilingResult
from scdm_qa.reporting.dashboard import save_dashboard
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
                check_id="122",
                severity="Fail",
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


def _make_cross_table_results(
    *, n_failed: int = 1
) -> tuple[ValidationResult, ProfilingResult]:
    """Create identical cross-table VR and PR fixtures for cross-table tests."""
    cross_table_vr = ValidationResult(
        table_key="cross_table",
        table_name="Cross-Table Checks",
        steps=(
            StepResult(
                step_index=-1,
                assertion_type="cross_table",
                column="PatID",
                description="Cross-table check",
                n_passed=10,
                n_failed=n_failed,
                failing_rows=None,
                check_id="201",
                severity="Fail",
            ),
        ),
        total_rows=0,
        chunks_processed=0,
    )
    cross_table_pr = ProfilingResult(
        table_key="cross_table",
        table_name="Cross-Table Checks",
        total_rows=0,
        columns=(),
    )
    return cross_table_vr, cross_table_pr


class TestSaveDashboard:
    """Tests for the new save_dashboard function."""

    def test_creates_index_and_detail_files(self, tmp_path: Path) -> None:
        """save_dashboard produces index.html and detail HTML files."""
        vr = _make_validation_result()
        pr = _make_profiling_result()
        results = [(vr, pr)]

        save_dashboard(tmp_path, results)

        # Verify index.html exists
        index_path = tmp_path / "index.html"
        assert index_path.exists()

        # Verify detail page for demographic exists
        detail_path = tmp_path / "demographic.html"
        assert detail_path.exists()

    def test_html_contains_dashboard_data_script(self, tmp_path: Path) -> None:
        """Dashboard HTML contains embedded JSON in <script type="application/json"> block."""
        vr = _make_validation_result()
        pr = _make_profiling_result()
        results = [(vr, pr)]

        save_dashboard(tmp_path, results)

        index_path = tmp_path / "index.html"
        html = index_path.read_text()

        # Verify embedded JSON exists
        match = re.search(
            r'<script type="application/json" id="dashboard-data">(.*?)</script>',
            html,
            re.DOTALL,
        )
        assert match, "dashboard-data script tag should be present"

        # Verify JSON is valid
        data = json.loads(match.group(1))
        assert "tables" in data
        assert "demographic" in data["tables"]

    def test_html_self_contained(self, tmp_path: Path) -> None:
        """Dashboard HTML is self-contained with no external CDN links."""
        vr = _make_validation_result()
        pr = _make_profiling_result()
        results = [(vr, pr)]

        save_dashboard(tmp_path, results)

        index_path = tmp_path / "index.html"
        html = index_path.read_text()

        # Check for external CDN patterns in resource URLs
        # Allow data: URIs and relative paths and namespaces (e.g. SVG)
        suspicious_patterns = [
            'src="https://cdn',
            'href="https://cdn',
            'src="https://unpkg',
            'href="https://unpkg',
            'src="http://cdn',
            'href="http://cdn',
            '<script src="https://',
            '<link href="https://',
        ]
        for pattern in suspicious_patterns:
            assert pattern not in html, f"HTML should not contain external CDN: {pattern}"


class TestSaveDashboard__CrossTable:
    """Tests for cross-table detail page in new dashboard."""

    def test_cross_table_detail_page_created(self, tmp_path: Path) -> None:
        """Cross-table results generate cross_table.html detail page."""
        cross_table_vr, cross_table_pr = _make_cross_table_results()
        results = [(cross_table_vr, cross_table_pr)]

        save_dashboard(tmp_path, results)

        # Verify cross_table.html exists
        detail_path = tmp_path / "cross_table.html"
        assert detail_path.exists()

        html = detail_path.read_text()
        assert "Cross-Table Checks" in html

    def test_cross_table_same_format(self, tmp_path: Path) -> None:
        """Cross-table detail page contains same Tabulator format as L1 pages."""
        cross_table_vr, cross_table_pr = _make_cross_table_results()
        results = [(cross_table_vr, cross_table_pr)]

        save_dashboard(tmp_path, results)

        detail_path = tmp_path / "cross_table.html"
        html = detail_path.read_text()

        # Verify embedded JSON exists
        match = re.search(
            r'<script type="application/json" id="dashboard-data">(.*?)</script>',
            html,
            re.DOTALL,
        )
        assert match, "dashboard-data script tag should be present in cross-table page"

        # Verify JSON contains validation checks in Tabulator format
        data = json.loads(match.group(1))
        # Detail page contains just the table data, not wrapped in "tables"
        assert "validation" in data
        assert "steps" in data["validation"]

    def test_cross_table_no_profiling(self, tmp_path: Path) -> None:
        """Cross-table detail page has empty profiling columns in JSON."""
        cross_table_vr, cross_table_pr = _make_cross_table_results(n_failed=0)
        results = [(cross_table_vr, cross_table_pr)]

        save_dashboard(tmp_path, results)

        detail_path = tmp_path / "cross_table.html"
        html = detail_path.read_text()

        # Extract and verify JSON
        match = re.search(
            r'<script type="application/json" id="dashboard-data">(.*?)</script>',
            html,
            re.DOTALL,
        )
        data = json.loads(match.group(1))

        # Profiling should be empty
        assert data["profiling"]["columns"] == []


class TestSaveDashboard__EdgeCases:
    """Tests for edge cases in dashboard generation."""

    def test_empty_results(self, tmp_path: Path) -> None:
        """Empty results list handled gracefully."""
        results: list[tuple[ValidationResult, ProfilingResult]] = []
        # Should not raise
        save_dashboard(tmp_path, results)

    def test_all_passing(self, tmp_path: Path) -> None:
        """All checks passing shows 100% pass rate in index."""
        vr = ValidationResult(
            table_key="demographic",
            table_name="Demographic",
            steps=(
                StepResult(
                    step_index=1,
                    assertion_type="col_vals_not_null",
                    column="PatID",
                    description="PatID not null",
                    n_passed=100,
                    n_failed=0,
                    failing_rows=None,
                    check_id="122",
                    severity="Fail",
                ),
            ),
            total_rows=100,
            chunks_processed=1,
        )
        pr = _make_profiling_result()
        results = [(vr, pr)]

        save_dashboard(tmp_path, results)

        index_path = tmp_path / "index.html"
        html = index_path.read_text()

        # Verify pass rate in JSON (index has tables structure)
        match = re.search(
            r'<script type="application/json" id="dashboard-data">(.*?)</script>',
            html,
            re.DOTALL,
        )
        data = json.loads(match.group(1))
        # In the index, all passes means pass_rate should be 1.0
        demographic_table = data["tables"]["demographic"]
        validation_steps = demographic_table["validation"]["steps"]
        # Check that all steps have n_failed=0
        assert all(step["n_failed"] == 0 for step in validation_steps)

    def test_with_failing_rows(self, tmp_path: Path) -> None:
        """Failing rows appear in detail page JSON."""
        vr = _make_validation_result(with_failures=True)
        pr = _make_profiling_result()
        results = [(vr, pr)]

        save_dashboard(tmp_path, results, max_failing_rows=10)

        detail_path = tmp_path / "demographic.html"
        html = detail_path.read_text()

        # Verify failing rows in detail page JSON
        match = re.search(
            r'<script type="application/json" id="dashboard-data">(.*?)</script>',
            html,
            re.DOTALL,
        )
        data = json.loads(match.group(1))
        # Detail page has just the table data structure (not wrapped in "tables")
        validation_steps = data["validation"]["steps"]
        # At least one step with failures
        has_failures = any(step.get("n_failed", 0) > 0 for step in validation_steps)
        assert has_failures


class TestSaveDashboard__ProfileOnly:
    """Tests for profile-only mode (no validation)."""

    def test_profile_only_produces_dashboard(self, tmp_path: Path) -> None:
        """Profile-only results (empty ValidationResult) produce valid dashboard."""
        empty_vr = ValidationResult(
            table_key="demographic",
            table_name="Demographic",
            steps=(),
            total_rows=100,
            chunks_processed=0,
        )
        pr = _make_profiling_result()
        results = [(empty_vr, pr)]

        save_dashboard(tmp_path, results)

        # Both files should exist
        index_path = tmp_path / "index.html"
        detail_path = tmp_path / "demographic.html"
        assert index_path.exists()
        assert detail_path.exists()

        # Detail page should have profiling data
        html = detail_path.read_text()
        match = re.search(
            r'<script type="application/json" id="dashboard-data">(.*?)</script>',
            html,
            re.DOTALL,
        )
        data = json.loads(match.group(1))
        assert len(data["profiling"]["columns"]) > 0
        assert data["profiling"]["columns"][0]["name"] == "PatID"

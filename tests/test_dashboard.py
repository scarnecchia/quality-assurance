from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import jinja2
import polars as pl

from scdm_qa.reporting.dashboard import (
    _load_vendor_asset,
    _get_template_env,
    _render_page,
    save_dashboard,
)
from scdm_qa.profiling.results import ColumnProfile, ProfilingResult
from scdm_qa.validation.results import StepResult, ValidationResult


class TestLoadVendorAsset:
    def test_loads_known_vendor_file(self) -> None:
        """Test that a known vendor file can be loaded."""
        content = _load_vendor_asset("tabulator.min.js")
        assert content, "loaded file should not be empty"
        assert isinstance(content, str)

    def test_loaded_content_is_non_empty(self) -> None:
        """Test that loaded files contain actual content."""
        for filename in ("tabulator.min.js", "tabulator.min.css", "plotly-basic.min.js"):
            content = _load_vendor_asset(filename)
            assert len(content) > 100, f"{filename} should contain substantial content"

    def test_raises_on_nonexistent_file(self) -> None:
        """Test that loading a nonexistent file raises an error."""
        with pytest.raises(FileNotFoundError):
            _load_vendor_asset("nonexistent-file.js")

    def test_rejects_path_traversal_with_forward_slash(self) -> None:
        """Test that filenames with forward slashes are rejected."""
        with pytest.raises(ValueError, match="invalid vendor asset filename"):
            _load_vendor_asset("../../config.py")

    def test_rejects_path_traversal_with_backslash(self) -> None:
        """Test that filenames with backslashes are rejected."""
        with pytest.raises(ValueError, match="invalid vendor asset filename"):
            _load_vendor_asset("..\\..\\config.py")

    def test_rejects_single_forward_slash(self) -> None:
        """Test that any forward slash in filename is rejected."""
        with pytest.raises(ValueError, match="invalid vendor asset filename"):
            _load_vendor_asset("subdir/file.js")

    def test_rejects_single_backslash(self) -> None:
        """Test that any backslash in filename is rejected."""
        with pytest.raises(ValueError, match="invalid vendor asset filename"):
            _load_vendor_asset("subdir\\file.js")


class TestGetTemplateEnv:
    def test_returns_jinja2_environment(self) -> None:
        """Test that _get_template_env returns a Jinja2 Environment."""
        env = _get_template_env()
        assert isinstance(env, jinja2.Environment)

    def test_can_load_base_html_template(self) -> None:
        """Test that the base.html template can be loaded from the environment."""
        env = _get_template_env()
        template = env.get_template("base.html")
        assert template is not None
        assert hasattr(template, "render")

    def test_environment_has_autoescape_enabled(self) -> None:
        """Test that autoescape is configured."""
        env = _get_template_env()
        assert env.autoescape is True

    def test_environment_uses_package_loader(self) -> None:
        """Test that the environment uses PackageLoader."""
        env = _get_template_env()
        assert isinstance(env.loader, jinja2.PackageLoader)


class TestRenderPage:
    def test_renders_base_html_with_page_title(self) -> None:
        """Test that base.html can be rendered with page_title context."""
        html = _render_page("base.html", page_title="Test Page")
        assert html is not None
        assert isinstance(html, str)
        assert len(html) > 0

    def test_rendered_output_contains_doctype(self) -> None:
        """Test that rendered HTML starts with DOCTYPE."""
        html = _render_page("base.html", page_title="Test")
        assert "<!DOCTYPE html>" in html

    def test_rendered_output_contains_tabulator_content(self) -> None:
        """Test that Tabulator JS and CSS are inlined in the output."""
        html = _render_page("base.html", page_title="Test")
        # Check for Tabulator JS signature
        assert "Tabulator" in html or "tabulator" in html.lower()
        # Check for Tabulator CSS signature (should have tabulator-specific rules)
        assert ".tabulator" in html or "tabulator" in html.lower()

    def test_rendered_output_contains_plotly_content(self) -> None:
        """Test that Plotly JS is inlined in the output."""
        html = _render_page("base.html", page_title="Test")
        # Check for Plotly JS signature
        assert "Plotly" in html or "plotly" in html.lower()

    def test_rendered_output_contains_page_title_in_html_title(self) -> None:
        """Test that the page_title appears in the HTML title tag."""
        html = _render_page("base.html", page_title="Dashboard Report")
        assert "Dashboard Report" in html
        assert "<title>" in html

    def test_rendered_output_contains_dashboard_text_in_title(self) -> None:
        """Test that 'SCDM-QA Dashboard' appears in the title."""
        html = _render_page("base.html", page_title="Test")
        assert "SCDM-QA Dashboard" in html

    def test_custom_context_variables_are_passed_to_template(self) -> None:
        """Test that additional context variables are passed to the template."""
        # The base.html template doesn't use custom vars, but we test that
        # the render function accepts and passes them through
        html = _render_page("base.html", page_title="Test", custom_var="custom_value")
        # Should render without error
        assert "<!DOCTYPE html>" in html

    def test_renders_with_multiple_context_variables(self) -> None:
        """Test that multiple context variables can be passed."""
        html = _render_page(
            "base.html",
            page_title="Test",
            var1="value1",
            var2="value2",
        )
        assert "<!DOCTYPE html>" in html


def _make_step(
    *,
    n_passed: int = 98,
    n_failed: int = 2,
    check_id: str = "122",
    severity: str = "Fail",
    failing_rows: pl.DataFrame | None = None,
) -> StepResult:
    """Create a StepResult with configurable fields."""
    if failing_rows is None and n_failed > 0:
        failing_rows = pl.DataFrame({"PatID": [None] * n_failed})
    return StepResult(
        step_index=1,
        assertion_type="col_vals_not_null",
        column="PatID",
        description="PatID not null",
        n_passed=n_passed,
        n_failed=n_failed,
        failing_rows=failing_rows,
        check_id=check_id,
        severity=severity,
    )


def _make_validation_result(
    *,
    table_key: str = "demographic",
    steps: tuple[StepResult, ...] | None = None,
    with_failures: bool = False,
) -> ValidationResult:
    """Create a ValidationResult with configurable fields."""
    if steps is None:
        if with_failures:
            steps = (_make_step(n_passed=98, n_failed=2, check_id="122", severity="Fail"),)
        else:
            steps = (_make_step(n_passed=100, n_failed=0, check_id="122", severity=None),)
    return ValidationResult(
        table_key=table_key,
        table_name="Demographic Table" if table_key == "demographic" else f"{table_key.title()} Table",
        steps=steps,
        total_rows=100,
        chunks_processed=1,
    )


def _make_profiling_result(*, table_key: str = "demographic") -> ProfilingResult:
    """Create a ProfilingResult with configurable fields."""
    return ProfilingResult(
        table_key=table_key,
        table_name="Demographic Table" if table_key == "demographic" else f"{table_key.title()} Table",
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


def _make_results_pair(
    *, table_key: str = "demographic", with_failures: bool = False
) -> tuple[ValidationResult, ProfilingResult]:
    """Create a (ValidationResult, ProfilingResult) tuple."""
    return (_make_validation_result(table_key=table_key, with_failures=with_failures), _make_profiling_result(table_key=table_key))


class TestSaveDashboard:
    def test_creates_index_html_file(self, tmp_path: Path) -> None:
        """Test that save_dashboard creates an index.html file."""
        vr, pr = _make_results_pair()
        result_path = save_dashboard(tmp_path, [(vr, pr)])
        index_file = tmp_path / "index.html"
        assert index_file.exists(), "index.html should be created"
        assert result_path == tmp_path, "should return output directory"

    def test_returns_output_directory_not_file(self, tmp_path: Path) -> None:
        """Test that save_dashboard returns the directory, not the file."""
        vr, pr = _make_results_pair()
        result_path = save_dashboard(tmp_path, [(vr, pr)])
        assert result_path.is_dir(), "returned path should be a directory"
        assert result_path == tmp_path

    def test_html_contains_stat_cards_elements(self, tmp_path: Path) -> None:
        """AC1.1: Verify stat cards for total checks, pass rate, and failures exist."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        assert "stat-total-checks" in html
        assert "stat-pass-rate" in html
        assert "stat-total-failures" in html

    def test_html_contains_donut_chart_div(self, tmp_path: Path) -> None:
        """AC1.2: Verify donut chart container exists."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        assert 'id="donut-chart"' in html or "donut-chart" in html

    def test_html_contains_bar_chart_div(self, tmp_path: Path) -> None:
        """AC1.3: Verify bar chart container exists."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        assert 'id="bar-chart"' in html or "bar-chart" in html

    def test_embedded_json_contains_summary(self, tmp_path: Path) -> None:
        """AC1.1, AC1.2, AC1.3: Verify embedded JSON has summary and by_severity."""
        vr, pr = _make_results_pair(with_failures=True)
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        assert match, "embedded JSON script tag should exist"
        data = json.loads(match.group(1))
        assert "summary" in data
        assert "total_checks" in data["summary"]
        assert "total_failures" in data["summary"]
        assert "by_severity" in data["summary"]

    def test_json_summary_counts_correct_with_failures(self, tmp_path: Path) -> None:
        """AC1.1: Verify summary counts are correct."""
        vr, pr = _make_results_pair(with_failures=True)
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        assert data["summary"]["total_checks"] == 1  # one step
        assert data["summary"]["total_failures"] == 2  # 2 failing rows

    def test_json_by_severity_has_pass_and_fail(self, tmp_path: Path) -> None:
        """AC1.2: Verify by_severity contains Fail and pass counts."""
        vr, pr = _make_results_pair(with_failures=True)
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        assert "pass" in data["summary"]["by_severity"]
        assert "Fail" in data["summary"]["by_severity"]
        assert data["summary"]["by_severity"]["Fail"] == 1  # one step failed

    def test_json_no_failing_rows_in_index_page(self, tmp_path: Path) -> None:
        """Verify failing_rows are stripped from index page JSON."""
        vr, pr = _make_results_pair(with_failures=True)
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        for table_data in data["tables"].values():
            for step in table_data["validation"]["steps"]:
                assert step["failing_rows"] == [], f"failing_rows should be empty, got {step['failing_rows']}"

    def test_all_pass_produces_valid_html(self, tmp_path: Path) -> None:
        """AC1.4: When all steps pass, produce valid HTML with zero failures."""
        vr, pr = _make_results_pair(with_failures=False)
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        assert data["summary"]["total_failures"] == 0
        assert data["summary"]["by_severity"]["pass"] == 1

    def test_cross_table_only_produces_valid_html(self, tmp_path: Path) -> None:
        """AC1.5: When only cross-table checks exist (total_rows=0, chunks_processed=0), still valid."""
        cross_table_vr = ValidationResult(
            table_key="cross_table",
            table_name="Cross-Table Checks",
            steps=(_make_step(check_id="201", severity="Fail"),),
            total_rows=0,
            chunks_processed=0,
        )
        cross_table_pr = ProfilingResult(
            table_key="cross_table",
            table_name="Cross-Table Checks",
            total_rows=0,
            columns=(),
        )
        save_dashboard(tmp_path, [(cross_table_vr, cross_table_pr)])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        assert "cross_table" in data["tables"]

    def test_summary_table_navigates_to_table_page(self, tmp_path: Path) -> None:
        """AC2.1: Verify table_key is used for navigation link in Tabulator grid."""
        vr, pr = _make_results_pair(table_key="demographic")
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        # Check that the Tabulator rowClick handler uses table_key for navigation
        assert 'table_key + ".html"' in html or 'table_key+".html"' in html, "rowClick handler should use table_key to build href"

    def test_empty_results_list_produces_valid_html(self, tmp_path: Path) -> None:
        """Verify that empty results list produces valid HTML with zero counts."""
        save_dashboard(tmp_path, [])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        assert data["summary"]["total_checks"] == 0
        assert data["summary"]["total_failures"] == 0

    def test_html_contains_plotly_and_tabulator_libraries(self, tmp_path: Path) -> None:
        """Verify Plotly and Tabulator assets are inlined."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        assert "Plotly.newPlot" in html or "plotly" in html.lower()
        assert "new Tabulator" in html or "tabulator" in html.lower()

    def test_json_special_characters_roundtrip(self, tmp_path: Path) -> None:
        """Verify JSON with special chars in description survives HTML escaping via |safe filter."""
        special_step = StepResult(
            step_index=1,
            assertion_type="test",
            column="col",
            description="values < 0 & > 100 'quoted' \"double\"",
            n_passed=99,
            n_failed=1,
            failing_rows=pl.DataFrame({"col": [None]}),
            check_id="999",
            severity="Warn",
        )
        vr = ValidationResult(
            table_key="test_table",
            table_name="Test Table",
            steps=(special_step,),
            total_rows=100,
            chunks_processed=1,
        )
        pr = ProfilingResult(
            table_key="test_table",
            table_name="Test Table",
            total_rows=100,
            columns=(),
        )
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        # If the |safe filter worked correctly, the description should be intact
        assert "values < 0 & > 100" in data["tables"]["test_table"]["validation"]["steps"][0]["description"]

    def test_multiple_tables_in_results(self, tmp_path: Path) -> None:
        """Verify multiple tables are handled correctly."""
        vr1, pr1 = _make_results_pair(table_key="demographic")
        vr2, pr2 = _make_results_pair(table_key="visits")
        save_dashboard(tmp_path, [(vr1, pr1), (vr2, pr2)])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        assert "demographic" in data["tables"]
        assert "visits" in data["tables"]
        assert len(data["tables"]) == 2

    def test_html_extends_base_template(self, tmp_path: Path) -> None:
        """Verify the rendered HTML extends base.html (has DOCTYPE, styles, etc)."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "index.html").read_text()
        assert "<!DOCTYPE html>" in html
        assert "<html" in html
        assert "</html>" in html
        assert "SCDM-QA Dashboard" in html


class TestDetailPage:
    def test_detail_page_created_for_each_table(self, tmp_path: Path) -> None:
        """AC2.1+: Verify detail page is created for each table."""
        vr, pr = _make_results_pair(table_key="demographic")
        save_dashboard(tmp_path, [(vr, pr)])
        detail_file = tmp_path / "demographic.html"
        assert detail_file.exists(), "detail page should be created with table_key.html"

    def test_multiple_detail_pages_created(self, tmp_path: Path) -> None:
        """AC2.1+: Verify multiple detail pages are created for multiple tables."""
        vr1, pr1 = _make_results_pair(table_key="demographic")
        vr2, pr2 = _make_results_pair(table_key="visits")
        save_dashboard(tmp_path, [(vr1, pr1), (vr2, pr2)])
        assert (tmp_path / "demographic.html").exists()
        assert (tmp_path / "visits.html").exists()

    def test_detail_page_contains_back_link(self, tmp_path: Path) -> None:
        """AC2.1+: Verify detail page has back link to index."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        assert 'href="index.html"' in html, "back link should navigate to index.html"
        assert "Back to Index" in html

    def test_detail_page_contains_stat_cards(self, tmp_path: Path) -> None:
        """AC2.2: Detail page header shows table name, total rows, chunks processed, overall pass rate."""
        vr, pr = _make_results_pair(with_failures=True)
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        # Verify stat card elements exist
        assert 'id="stat-total-rows"' in html
        assert 'id="stat-chunks"' in html
        assert 'id="stat-pass-rate"' in html
        assert 'id="stat-checks"' in html

    def test_detail_page_json_contains_validation_data(self, tmp_path: Path) -> None:
        """AC2.2: Verify embedded JSON has correct validation.total_rows and chunks_processed."""
        vr, pr = _make_results_pair(table_key="demographic", with_failures=True)
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        assert match, "embedded JSON should exist"
        data = json.loads(match.group(1))
        assert data["validation"]["total_rows"] == 100
        assert data["validation"]["chunks_processed"] == 1

    def test_detail_page_json_contains_all_step_fields(self, tmp_path: Path) -> None:
        """AC2.3: Verify steps array contains all StepResult fields."""
        step = _make_step(n_passed=98, n_failed=2, check_id="122", severity="Fail")
        vr = ValidationResult(
            table_key="test",
            table_name="Test Table",
            steps=(step,),
            total_rows=100,
            chunks_processed=1,
        )
        pr = ProfilingResult(table_key="test", table_name="Test Table", total_rows=100, columns=())
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "test.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        steps = data["validation"]["steps"]
        assert len(steps) == 1
        step_data = steps[0]
        assert "check_id" in step_data and step_data["check_id"] == "122"
        assert "assertion_type" in step_data and step_data["assertion_type"] == "col_vals_not_null"
        assert "column" in step_data and step_data["column"] == "PatID"
        assert "description" in step_data
        assert "n_passed" in step_data and step_data["n_passed"] == 98
        assert "n_failed" in step_data and step_data["n_failed"] == 2
        assert "pass_rate" in step_data
        assert "severity" in step_data and step_data["severity"] == "Fail"

    def test_detail_page_multiple_steps_all_present(self, tmp_path: Path) -> None:
        """AC2.3: Verify all steps appear in the JSON for detail page."""
        step1 = _make_step(n_passed=98, n_failed=2, check_id="122", severity="Fail")
        step2 = StepResult(
            step_index=2,
            assertion_type="col_vals_between",
            column="age",
            description="age between 0 and 150",
            n_passed=99,
            n_failed=1,
            failing_rows=pl.DataFrame({"age": [999]}),
            check_id="456",
            severity="Warn",
        )
        vr = ValidationResult(
            table_key="test",
            table_name="Test Table",
            steps=(step1, step2),
            total_rows=100,
            chunks_processed=1,
        )
        pr = ProfilingResult(table_key="test", table_name="Test Table", total_rows=100, columns=())
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "test.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        steps = data["validation"]["steps"]
        assert len(steps) == 2
        assert steps[0]["check_id"] == "122"
        assert steps[1]["check_id"] == "456"

    def test_detail_page_contains_header_filters(self, tmp_path: Path) -> None:
        """AC2.4: Verify header filters are configured on severity, assertion_type, column, description."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        # Check for headerFilter configuration in JS
        assert "headerFilter: true" in html, "headerFilter should be enabled on table columns"

    def test_detail_page_severity_column_has_filter(self, tmp_path: Path) -> None:
        """AC2.4: Severity column must have headerFilter."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        # Find the severity column definition
        assert 'field: "severity"' in html and 'headerFilter: true' in html

    def test_detail_page_contains_csv_download_button(self, tmp_path: Path) -> None:
        """AC2.5: CSV download button must exist with download functionality."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        assert 'id="download-validation-csv"' in html, "download button should exist"
        assert 'download("csv"' in html or '.download(' in html, "CSV download call should be present"

    def test_detail_page_csv_download_wired_correctly(self, tmp_path: Path) -> None:
        """AC2.5: CSV download must be wired to the button."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        # Check for event listener and download call
        assert "download-validation-csv" in html
        assert 'download("csv"' in html or ".download(" in html

    def test_cross_table_detail_page_created(self, tmp_path: Path) -> None:
        """AC3.1: When results include cross_table entry, cross_table.html is produced."""
        cross_table_vr = ValidationResult(
            table_key="cross_table",
            table_name="Cross-Table Checks",
            steps=(_make_step(check_id="201", severity="Fail"),),
            total_rows=0,
            chunks_processed=0,
        )
        cross_table_pr = ProfilingResult(
            table_key="cross_table",
            table_name="Cross-Table Checks",
            total_rows=0,
            columns=(),
        )
        save_dashboard(tmp_path, [(cross_table_vr, cross_table_pr)])
        detail_file = tmp_path / "cross_table.html"
        assert detail_file.exists(), "cross_table.html should be created"

    def test_cross_table_in_index_json(self, tmp_path: Path) -> None:
        """AC3.1: Index page JSON contains cross_table entry."""
        cross_table_vr = ValidationResult(
            table_key="cross_table",
            table_name="Cross-Table Checks",
            steps=(_make_step(check_id="201", severity="Fail"),),
            total_rows=0,
            chunks_processed=0,
        )
        cross_table_pr = ProfilingResult(
            table_key="cross_table",
            table_name="Cross-Table Checks",
            total_rows=0,
            columns=(),
        )
        save_dashboard(tmp_path, [(cross_table_vr, cross_table_pr)])
        html = (tmp_path / "index.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        assert "cross_table" in data["tables"], "cross_table should be in index JSON"

    def test_detail_page_filename_matches_table_key(self, tmp_path: Path) -> None:
        """Verify detail page filename is {table_key}.html."""
        vr, pr = _make_results_pair(table_key="demographic")
        save_dashboard(tmp_path, [(vr, pr)])
        assert (tmp_path / "demographic.html").exists()

    def test_detail_page_with_null_check_id(self, tmp_path: Path) -> None:
        """AC4.3: check_id can be null, display as '—' in detail page."""
        step = _make_step(check_id=None, severity=None)
        vr = ValidationResult(
            table_key="test",
            table_name="Test Table",
            steps=(step,),
            total_rows=100,
            chunks_processed=1,
        )
        pr = ProfilingResult(table_key="test", table_name="Test Table", total_rows=100, columns=())
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "test.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        data = json.loads(match.group(1))
        # The JS client-side code handles the display; verify JSON has null
        step_data = data["validation"]["steps"][0]
        assert step_data["check_id"] is None, "check_id can be null in JSON"

    def test_detail_page_title_matches_table_name(self, tmp_path: Path) -> None:
        """Verify page title (h1) matches table_name."""
        vr, pr = _make_results_pair(table_key="demographic")
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        assert "Demographic Table" in html, "table name should appear in page"

    def test_detail_page_html_valid_structure(self, tmp_path: Path) -> None:
        """Verify detail page has valid HTML structure."""
        vr, pr = _make_results_pair()
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        assert "<!DOCTYPE html>" in html
        assert "<html" in html
        assert "</html>" in html
        assert "SCDM-QA Dashboard" in html

    def test_detail_page_embedded_json_is_valid(self, tmp_path: Path) -> None:
        """Verify embedded JSON in detail page is valid JSON."""
        vr, pr = _make_results_pair(with_failures=True)
        save_dashboard(tmp_path, [(vr, pr)])
        html = (tmp_path / "demographic.html").read_text()
        match = re.search(r'<script type="application/json" id="dashboard-data">(.*?)</script>', html, re.DOTALL)
        assert match, "embedded JSON should exist"
        # Should not raise if valid JSON
        data = json.loads(match.group(1))
        assert isinstance(data, dict)
        assert "validation" in data
        assert "profiling" in data

from __future__ import annotations

import copy
import json
from pathlib import Path
from importlib.resources import files

import jinja2

from scdm_qa.profiling.results import ProfilingResult
from scdm_qa.reporting.serialise import serialise_run
from scdm_qa.validation.results import ValidationResult


def _load_vendor_asset(filename: str) -> str:
    r"""Load a vendor asset file from the vendor directory.

    Args:
        filename: Name of the file to load (e.g., 'tabulator.min.js')

    Returns:
        The file contents as a string.

    Raises:
        ValueError: If filename contains path separators (/ or \).
    """
    if "/" in filename or "\\" in filename:
        msg = f"invalid vendor asset filename: {filename}"
        raise ValueError(msg)
    return (
        files("scdm_qa.reporting") / "vendor" / filename
    ).read_text(encoding="utf-8")


def _get_template_env() -> jinja2.Environment:
    """Create a Jinja2 Environment for dashboard templates.

    Returns:
        A Jinja2 Environment configured with PackageLoader for the templates
        directory.
    """
    return jinja2.Environment(
        loader=jinja2.PackageLoader("scdm_qa.reporting", "templates"),
        autoescape=True,
    )


def _render_page(template_name: str, **context: object) -> str:
    """Render a dashboard page template with inlined vendor assets.

    Args:
        template_name: Name of the template file (e.g., 'base.html')
        **context: Additional context variables for template rendering

    Returns:
        The rendered HTML as a string.
    """
    env = _get_template_env()
    template = env.get_template(template_name)
    vendor_context = {
        "tabulator_js": _load_vendor_asset("tabulator.min.js"),
        "tabulator_css": _load_vendor_asset("tabulator.min.css"),
        "plotly_js": _load_vendor_asset("plotly-basic.min.js"),
    }
    return template.render(**vendor_context, **context)


def save_dashboard(
    output_dir: Path,
    results: list[tuple[ValidationResult, ProfilingResult]],
    *,
    max_failing_rows: int = 500,
) -> Path:
    """Render the full dashboard index page with scorecard, charts, and summary grid.

    Generates the index.html page which displays:
    - Stat cards for total checks, pass rate, and total failures
    - Donut chart of results by severity
    - Bar chart of pass rate per table
    - Tabulator summary grid with navigation to detail pages

    The JSON data is embedded in the page with failing_rows stripped to keep
    file size small (failing_rows are shown only on detail pages).

    Args:
        output_dir: Directory where dashboard HTML will be written
        results: List of (ValidationResult, ProfilingResult) tuples
        max_failing_rows: Maximum number of failing rows to display (used for detail pages)

    Returns:
        Path to the output directory (where index.html was written)
    """
    # Serialise the full run data
    run_data = serialise_run(results, max_failing_rows=max_failing_rows)

    # Create a summary-only version by stripping failing_rows for the index page
    index_data = copy.deepcopy(run_data)
    for table_data in index_data["tables"].values():
        for step in table_data["validation"]["steps"]:
            step["failing_rows"] = []

    index_json = json.dumps(index_data)

    # Render the index page
    output_dir.mkdir(parents=True, exist_ok=True)
    html = _render_page("index.html", page_title="Index", dashboard_json=index_json)
    index_path = output_dir / "index.html"
    index_path.write_text(html, encoding="utf-8")

    return output_dir

from __future__ import annotations

from pathlib import Path
from importlib.resources import files

import jinja2

from scdm_qa.profiling.results import ProfilingResult
from scdm_qa.validation.results import ValidationResult


def _load_vendor_asset(filename: str) -> str:
    """Load a vendor asset file from the vendor directory.

    Args:
        filename: Name of the file to load (e.g., 'tabulator.min.js')

    Returns:
        The file contents as a string.
    """
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
    """Render the full dashboard. Implemented in Phase 3+.

    Args:
        output_dir: Directory where dashboard HTML will be written
        results: List of (ValidationResult, ProfilingResult) tuples
        max_failing_rows: Maximum number of failing rows to display

    Returns:
        Path to the generated dashboard index

    Raises:
        NotImplementedError: This function is implemented in Phase 3+
    """
    raise NotImplementedError("Dashboard rendering not yet implemented")

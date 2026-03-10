from __future__ import annotations

from pathlib import Path
from typing import TypedDict

import jinja2


class ReportSummary(TypedDict):
    table_key: str
    table_name: str
    filename: str
    total_rows: int
    step_count: int
    total_failures: int
    status: str
    status_class: str


_INDEX_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>SCDM-QA Report Index</title>
    <style>
        body { font-family: system-ui, sans-serif; max-width: 800px; margin: 2rem auto; padding: 0 1rem; }
        h1 { border-bottom: 2px solid #333; padding-bottom: 0.5rem; }
        table { border-collapse: collapse; width: 100%; }
        th, td { text-align: left; padding: 0.5rem; border-bottom: 1px solid #ddd; }
        th { background: #f5f5f5; }
        a { color: #0066cc; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .pass { color: #28a745; }
        .warn { color: #ffc107; }
        .fail { color: #dc3545; }
    </style>
</head>
<body>
    <h1>SCDM-QA Report Index</h1>
    <p>{{ report_count }} table{{ 's' if report_count != 1 else '' }} validated.</p>
    <table>
        <thead>
            <tr>
                <th>Table</th>
                <th>Rows</th>
                <th>Steps</th>
                <th>Failures</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
            {% for report in reports %}
            <tr>
                <td><a href="{{ report.filename }}">{{ report.table_name }}</a></td>
                <td>{{ "{:,}".format(report.total_rows) }}</td>
                <td>{{ report.step_count }}</td>
                <td>{{ "{:,}".format(report.total_failures) }}</td>
                <td class="{{ report.status_class }}">{{ report.status }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""


def save_index(
    output_dir: Path,
    report_summaries: list[ReportSummary],
) -> Path:
    template = jinja2.Template(_INDEX_TEMPLATE)
    html = template.render(
        report_count=len(report_summaries),
        reports=report_summaries,
    )
    index_path = output_dir / "index.html"
    index_path.write_text(html, encoding="utf-8")
    return index_path


def make_report_summary(
    table_key: str,
    table_name: str,
    total_rows: int,
    step_count: int,
    total_failures: int,
) -> ReportSummary:
    if total_failures == 0:
        status = "PASS"
        status_class = "pass"
    else:
        status = "FAIL"
        status_class = "fail"

    return {
        "table_key": table_key,
        "table_name": table_name,
        "filename": f"{table_key}.html",
        "total_rows": total_rows,
        "step_count": step_count,
        "total_failures": total_failures,
        "status": status,
        "status_class": status_class,
    }

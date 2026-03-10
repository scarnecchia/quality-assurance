from __future__ import annotations

from pathlib import Path

import polars as pl
from great_tables import GT, loc, style

from scdm_qa.profiling.results import ProfilingResult
from scdm_qa.validation.results import ValidationResult


def build_validation_table(result: ValidationResult) -> GT:
    rows = []
    for step in result.steps:
        rows.append({
            "Step": step.check_id or str(step.step_index),
            "Check": step.assertion_type,
            "Column": step.column,
            "Description": step.description,
            "Total": step.n_total,
            "Passed": step.n_passed,
            "Failed": step.n_failed,
            "Pass Rate": step.f_passed,
        })

    if not rows:
        rows.append({
            "Step": "—", "Check": "—", "Column": "—", "Description": "No validation steps",
            "Total": 0, "Passed": 0, "Failed": 0, "Pass Rate": 1.0,
        })

    df = pl.DataFrame(rows)

    gt = (
        GT(df)
        .tab_header(
            title=f"Validation: {result.table_name}",
            subtitle=f"{result.total_rows:,} rows across {result.chunks_processed} chunks",
        )
        .fmt_number(columns=["Total", "Passed", "Failed"], use_seps=True, decimals=0)
        .fmt_percent(columns=["Pass Rate"], decimals=1)
        .tab_style(
            style=style.fill(color="#d4edda"),
            locations=loc.body(columns=["Pass Rate"], rows=pl.col("Pass Rate") >= 0.99),
        )
        .tab_style(
            style=style.fill(color="#fff3cd"),
            locations=loc.body(columns=["Pass Rate"], rows=(pl.col("Pass Rate") >= 0.95) & (pl.col("Pass Rate") < 0.99)),
        )
        .tab_style(
            style=style.fill(color="#f8d7da"),
            locations=loc.body(columns=["Pass Rate"], rows=pl.col("Pass Rate") < 0.95),
        )
    )

    return gt


def build_profiling_table(result: ProfilingResult) -> GT:
    rows = []
    for col in result.columns:
        rows.append({
            "Column": col.name,
            "Type": col.col_type,
            "Completeness": col.completeness,
            "Distinct": col.distinct_count,
            "Min": col.min_value or "—",
            "Max": col.max_value or "—",
        })

    df = pl.DataFrame(rows)

    gt = (
        GT(df)
        .tab_header(
            title=f"Profile: {result.table_name}",
            subtitle=f"{result.total_rows:,} total rows",
        )
        .fmt_percent(columns=["Completeness"], decimals=1)
        .fmt_number(columns=["Distinct"], use_seps=True, decimals=0)
        .tab_style(
            style=style.fill(color="#f8d7da"),
            locations=loc.body(columns=["Completeness"], rows=pl.col("Completeness") < 0.95),
        )
    )

    return gt


def build_failing_rows_table(result: ValidationResult) -> list[tuple[str, GT]]:
    tables: list[tuple[str, GT]] = []
    for step in result.steps:
        if step.failing_rows is not None and step.failing_rows.height > 0:
            gt = (
                GT(step.failing_rows.head(100))
                .tab_header(
                    title=f"Failing Rows: {step.description}",
                    subtitle=f"Check {step.check_id or step.step_index} — {step.n_failed:,} total failures (showing up to 100)",
                )
            )
            tables.append((f"step_{step.step_index}", gt))
    return tables


def save_table_report(
    output_dir: Path,
    table_key: str,
    validation_result: ValidationResult,
    profiling_result: ProfilingResult,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"{table_key}.html"

    validation_gt = build_validation_table(validation_result)
    profiling_gt = build_profiling_table(profiling_result) if profiling_result.columns else None
    failing_tables = build_failing_rows_table(validation_result)

    csv_download_js = """
    <script>
    function downloadCSV(tableId, filename) {
        var table = document.getElementById(tableId);
        if (!table) return;
        var rows = table.querySelectorAll('tr');
        var csv = [];
        rows.forEach(function(row) {
            var cols = row.querySelectorAll('td, th');
            var rowData = [];
            cols.forEach(function(col) { rowData.push('"' + col.textContent.replace(/"/g, '""') + '"'); });
            csv.push(rowData.join(','));
        });
        var blob = new Blob([csv.join('\\n')], {type: 'text/csv'});
        var a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = filename;
        a.click();
    }
    </script>
    <style>
    details { margin: 1rem 0; }
    summary { cursor: pointer; font-weight: bold; padding: 0.5rem; background: #f5f5f5; border-radius: 4px; }
    .download-btn { margin: 0.5rem 0; padding: 0.3rem 0.8rem; cursor: pointer; }
    </style>
    """

    parts: list[str] = [
        "<!DOCTYPE html>",
        "<html lang='en'>",
        f"<head><meta charset='utf-8'><title>SCDM-QA Report: {table_key}</title>{csv_download_js}</head>",
        "<body>",
        f"<h1>SCDM-QA Report: {validation_result.table_name}</h1>",
        "<h2>Validation Summary</h2>",
        validation_gt.as_raw_html(inline_css=False),
    ]

    if profiling_gt:
        parts.extend([
            "<h2>Data Profile</h2>",
            profiling_gt.as_raw_html(inline_css=False),
        ])

    if failing_tables:
        parts.append("<h2>Failing Row Extracts</h2>")
        for i, (label, gt) in enumerate(failing_tables):
            table_id = f"failing_{i}"
            parts.append(f"<details><summary>{label} — click to expand</summary>")
            parts.append(f'<button class="download-btn" onclick="downloadCSV(\'{table_id}\', \'{label}.csv\')">Download CSV</button>')
            parts.append(f'<div id="{table_id}">')
            parts.append(gt.as_raw_html(inline_css=False))
            parts.append("</div></details>")

    parts.extend(["</body>", "</html>"])

    report_path.write_text("\n".join(parts), encoding="utf-8")
    return report_path

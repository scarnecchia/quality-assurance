# SCDM-QA Implementation Plan — Phase 7: Reporting

**Goal:** Self-contained HTML reports combining validation and profiling results, with an index page linking all table reports for multi-table runs.

**Architecture:** A `ReportBuilder` takes a `ValidationResult` and `ProfilingResult` and produces two great_tables GT objects — one for the validation summary (pass/fail per step with conditional red/yellow/green colouring) and one for the profiling summary (completeness, distributions, ranges). These are combined into a single self-contained HTML file per table. An index page is generated from a Jinja2 template linking all per-table reports.

**Tech Stack:** Python >=3.12, great-tables 0.21.x, jinja2 (for index template), polars 1.38.x

**Scope:** 8 phases from original design (phase 7 of 8)

**Codebase verified:** 2026-03-09

---

## Acceptance Criteria Coverage

This phase implements and tests:

### scdm-qa.AC4: Interactive HTML dashboard
- **scdm-qa.AC4.1 Success:** Pointblank HTML report shows pass/fail summary with threshold-based colouring per validation step
- **scdm-qa.AC4.2 Success:** Failing row extracts downloadable from report (bounded, capped at configurable limit)
- **scdm-qa.AC4.3 Success:** Index page links all table reports for multi-table runs

---

## Investigation Findings

**great_tables API:**
- `GT(polars_df)` accepts Polars DataFrames directly
- `tab_style(style=style.fill(color="red"), locations=loc.body(..., rows=condition))` for conditional colouring
- `fmt_percent(scale_values=True)` for completeness/pass rates
- `fmt_number(compact=True)` for large counts
- `as_raw_html(inline_css=True)` returns self-contained HTML fragment
- `save("file.html")` writes complete HTML file
- `tab_spanner()` for column grouping
- `tab_header(title=..., subtitle=...)` for titles

**Jinja2 not included in great_tables dependencies** — add it to pyproject.toml for index template rendering.

**Codebase state:** `src/scdm_qa/validation/` has results, accumulator, runner, global_checks from Phases 4-5. `src/scdm_qa/profiling/` has results and accumulator from Phase 6. No `reporting/` directory exists.

---

<!-- START_TASK_1 -->
### Task 1: Add jinja2 dependency

**Files:**
- Modify: `pyproject.toml`

**Step 1: Add jinja2 to dependencies**

Add `"jinja2>=3,<4"` to the `dependencies` list in pyproject.toml.

**Step 2: Verify operationally**

Run: `uv sync`
Expected: jinja2 installs without errors.

**Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add jinja2 dependency for report index template"
```
<!-- END_TASK_1 -->

<!-- START_SUBCOMPONENT_A (tasks 2-4) -->
<!-- START_TASK_2 -->
### Task 2: Create report builder for validation summary

**Files:**
- Create: `src/scdm_qa/reporting/__init__.py`
- Create: `src/scdm_qa/reporting/builder.py`

**Step 1: Create the files**

Create `src/scdm_qa/reporting/builder.py`:

```python
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
            "Step": step.step_index,
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
            "Step": 0, "Check": "—", "Column": "—", "Description": "No validation steps",
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
                    subtitle=f"Step {step.step_index} — {step.n_failed:,} total failures (showing up to 100)",
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
    profiling_gt = build_profiling_table(profiling_result)
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
        validation_gt.as_raw_html(inline_css=True),
        "<h2>Data Profile</h2>",
        profiling_gt.as_raw_html(inline_css=True),
    ]

    if failing_tables:
        parts.append("<h2>Failing Row Extracts</h2>")
        for i, (label, gt) in enumerate(failing_tables):
            table_id = f"failing_{i}"
            parts.append(f"<details><summary>{label} — click to expand</summary>")
            parts.append(f'<button class="download-btn" onclick="downloadCSV(\'{table_id}\', \'{label}.csv\')">Download CSV</button>')
            parts.append(f'<div id="{table_id}">')
            parts.append(gt.as_raw_html(inline_css=True))
            parts.append("</div></details>")

    parts.extend(["</body>", "</html>"])

    report_path.write_text("\n".join(parts), encoding="utf-8")
    return report_path
```

Create `src/scdm_qa/reporting/__init__.py`:

```python
from scdm_qa.reporting.builder import save_table_report

__all__ = ["save_table_report"]
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.reporting import save_table_report; print('reporting imported OK')"`
Expected: `reporting imported OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/reporting/__init__.py src/scdm_qa/reporting/builder.py
git commit -m "feat: add report builder with validation and profiling HTML tables"
```
<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Create index page generator

**Files:**
- Create: `src/scdm_qa/reporting/index.py`

**Step 1: Create the file**

```python
from __future__ import annotations

from pathlib import Path

import jinja2

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
    report_summaries: list[dict],
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
) -> dict:
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
```

**Step 2: Verify operationally**

Run: `uv run python -c "from scdm_qa.reporting.index import save_index, make_report_summary; print('index generator imported OK')"`
Expected: `index generator imported OK`

**Step 3: Commit**

```bash
git add src/scdm_qa/reporting/index.py
git commit -m "feat: add index page generator for multi-table report linking"
```
<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Test report builder and index

**Verifies:** scdm-qa.AC4.1, scdm-qa.AC4.2, scdm-qa.AC4.3

**Files:**
- Create: `tests/test_reporting.py`

**Implementation:**

Tests verify that the report builder produces valid HTML files with validation and profiling sections, that failing row extracts appear in the output, and that the index page links all table reports.

**Testing:**
- scdm-qa.AC4.1: Generated HTML contains validation table with pass/fail columns
- scdm-qa.AC4.2: Generated HTML contains failing row extract section when failures exist
- scdm-qa.AC4.3: Index page contains links to all table report files

```python
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
```

**Verification:**

Run: `uv run pytest tests/test_reporting.py -v`
Expected: All tests pass.

**Commit:** `test: add report builder and index page tests`
<!-- END_TASK_4 -->
<!-- END_SUBCOMPONENT_A -->

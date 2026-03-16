# QA Dashboard Implementation Plan — Phase 2: Vendor Assets & Base Template

**Goal:** Set up vendored JS/CSS files and the shared Jinja2 base template for the dashboard.

**Architecture:** Download Tabulator 6.4.0 (JS+CSS) and Plotly-basic 3.4.0 (JS) into `src/scdm_qa/reporting/vendor/`. Create a `base.html` Jinja2 template in `src/scdm_qa/reporting/templates/` that inlines these assets. Configure `pyproject.toml` to include both directories in the built package.

**Tech Stack:** Jinja2 (PackageLoader), Tabulator 6.4.0, Plotly-basic 3.4.0, uv_build

**Scope:** 6 phases from original design (phase 2 of 6)

**Codebase verified:** 2026-03-10

---

## Acceptance Criteria Coverage

This phase implements and tests:

### qa-dashboard.AC5: Self-contained HTML
- **qa-dashboard.AC5.2 Success:** Tabulator JS+CSS and Plotly JS are inlined in each HTML file (no CDN links)
- **qa-dashboard.AC5.3 Success:** HTML files open correctly in a browser without network access

---

<!-- START_TASK_1 -->
### Task 1: Download and vendor Tabulator and Plotly assets

**Files:**
- Create: `src/scdm_qa/reporting/vendor/tabulator.min.js`
- Create: `src/scdm_qa/reporting/vendor/tabulator.min.css`
- Create: `src/scdm_qa/reporting/vendor/plotly-basic.min.js`
- Create: `src/scdm_qa/reporting/vendor/VERSIONS.md`

**Implementation:**

Create the vendor directory and download the assets:

```bash
mkdir -p src/scdm_qa/reporting/vendor

curl -o src/scdm_qa/reporting/vendor/tabulator.min.js \
  https://unpkg.com/tabulator-tables@6.4.0/dist/js/tabulator.min.js

curl -o src/scdm_qa/reporting/vendor/tabulator.min.css \
  https://unpkg.com/tabulator-tables@6.4.0/dist/css/tabulator.min.css

curl -o src/scdm_qa/reporting/vendor/plotly-basic.min.js \
  https://cdn.plot.ly/plotly-basic-3.4.0.min.js
```

Create `src/scdm_qa/reporting/vendor/VERSIONS.md`:

```markdown
# Vendored Assets

| Library | Version | Source |
|---------|---------|--------|
| Tabulator | 6.4.0 | https://unpkg.com/tabulator-tables@6.4.0/ |
| Plotly.js (basic) | 3.4.0 | https://cdn.plot.ly/plotly-basic-3.4.0.min.js |

Last updated: 2026-03-10
```

**Verification:**
Run: `ls -lh src/scdm_qa/reporting/vendor/`
Expected: Four files — `tabulator.min.js` (~420KB), `tabulator.min.css`, `plotly-basic.min.js` (~1.1MB), `VERSIONS.md`

Run: `head -1 src/scdm_qa/reporting/vendor/tabulator.min.js`
Expected: Should contain JavaScript content (not an HTML error page)

**Commit:** `chore: vendor Tabulator 6.4.0 and Plotly-basic 3.4.0 assets`

<!-- END_TASK_1 -->

<!-- START_TASK_2 -->
### Task 2: Create base.html Jinja2 template

**Files:**
- Create: `src/scdm_qa/reporting/templates/base.html`

**Implementation:**

Create the templates directory and write `base.html`. This is the shared layout that all dashboard pages extend. It inlines vendored assets and provides content blocks.

The template receives these context variables from the Python render function:
- `tabulator_js` (str): contents of `tabulator.min.js`
- `tabulator_css` (str): contents of `tabulator.min.css`
- `plotly_js` (str): contents of `plotly-basic.min.js`
- `page_title` (str): page title

Template structure:

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ page_title }} — SCDM-QA Dashboard</title>
    <style>
{{ tabulator_css }}
    </style>
    <style>
        :root {
            --color-pass: #28a745;
            --color-warn: #ffc107;
            --color-fail: #dc3545;
            --color-note: #6c757d;
            --color-bg: #ffffff;
            --color-bg-alt: #f8f9fa;
            --color-border: #dee2e6;
            --color-text: #212529;
            --color-text-muted: #6c757d;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: system-ui, -apple-system, sans-serif;
            color: var(--color-text);
            background: var(--color-bg);
            line-height: 1.5;
            padding: 2rem;
            max-width: 1400px;
            margin: 0 auto;
        }
        h1 { font-size: 1.75rem; margin-bottom: 1.5rem; border-bottom: 2px solid var(--color-text); padding-bottom: 0.5rem; }
        h2 { font-size: 1.25rem; margin: 1.5rem 0 0.75rem; }
        .stat-cards { display: flex; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
        .stat-card {
            background: var(--color-bg-alt);
            border: 1px solid var(--color-border);
            border-radius: 8px;
            padding: 1rem 1.5rem;
            min-width: 180px;
            flex: 1;
        }
        .stat-card .label { font-size: 0.85rem; color: var(--color-text-muted); }
        .stat-card .value { font-size: 1.75rem; font-weight: 700; }
        .charts-row { display: flex; gap: 1.5rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
        .chart-container { flex: 1; min-width: 300px; }
        .severity-fail { color: var(--color-fail); }
        .severity-warn { color: var(--color-warn); }
        .severity-note { color: var(--color-note); }
        .severity-pass { color: var(--color-pass); }
        .back-link { display: inline-block; margin-bottom: 1rem; color: #0066cc; text-decoration: none; }
        .back-link:hover { text-decoration: underline; }
        .section { margin-bottom: 2rem; }
        .download-btn {
            display: inline-block;
            margin: 0.5rem 0;
            padding: 0.4rem 0.8rem;
            background: var(--color-bg-alt);
            border: 1px solid var(--color-border);
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85rem;
        }
        .download-btn:hover { background: var(--color-border); }
        .collapsible-header {
            cursor: pointer;
            user-select: none;
            padding: 0.5rem;
            background: var(--color-bg-alt);
            border: 1px solid var(--color-border);
            border-radius: 4px;
            margin-bottom: 0.5rem;
        }
        .collapsible-header::before { content: "▶ "; }
        .collapsible-header.open::before { content: "▼ "; }
        .collapsible-body { display: none; padding: 0.5rem 0; }
        .collapsible-body.open { display: block; }
        .low-completeness { background-color: #fff3f3; }
    </style>
    {% block extra_css %}{% endblock %}
</head>
<body>
    {% block content %}{% endblock %}

    <script>
{{ tabulator_js }}
    </script>
    <script>
{{ plotly_js }}
    </script>
    {% block extra_js %}{% endblock %}
</body>
</html>
```

Key design decisions:
- CSS variables for severity colours — reusable across index and detail pages
- Content blocks (`extra_css`, `content`, `extra_js`) for child templates to extend
- Vendor JS loaded at end of body for faster initial render
- All styles are self-contained — no external references

**Verification:**
Run: `cat src/scdm_qa/reporting/templates/base.html | head -5`
Expected: `<!DOCTYPE html>` followed by template content

**Commit:** `feat: add base.html Jinja2 dashboard template`

<!-- END_TASK_2 -->

<!-- START_TASK_3 -->
### Task 3: Configure pyproject.toml for package data inclusion

**Files:**
- Modify: `pyproject.toml`

**Implementation:**

The `uv_build` backend typically auto-includes non-Python files within the package source tree. **Verify auto-inclusion first** by building and inspecting the wheel — explicit config should only be added if the files are missing.

Steps:
1. Run `uv build` and inspect the wheel to check if template and vendor files are auto-included
2. If files ARE included automatically: no `pyproject.toml` changes needed — skip to verification
3. If files are NOT included: consult `uv_build` documentation for the correct package-data configuration syntax and add it to `pyproject.toml`

The implementor must verify by building and inspecting, not by assuming either outcome.

**Verification:**
Run: `uv build`
Expected: Build succeeds

Run: `python -c "import zipfile; z=zipfile.ZipFile(list(__import__('pathlib').Path('dist').glob('*.whl'))[0]); [print(n) for n in z.namelist() if 'vendor' in n or 'templates' in n]"`
Expected: Lists paths including `scdm_qa/reporting/vendor/tabulator.min.js`, `scdm_qa/reporting/templates/base.html`, etc.

If files are NOT included, the implementor must adjust the build config. Check `uv_build` docs for the correct package-data syntax.

**Commit:** `chore: configure package data for dashboard templates and vendor assets`

<!-- END_TASK_3 -->

<!-- START_TASK_4 -->
### Task 4: Add dashboard.py with asset loading and template rendering

**Verifies:** qa-dashboard.AC5.2, qa-dashboard.AC5.3

**Files:**
- Create: `src/scdm_qa/reporting/dashboard.py`

**Implementation:**

Create the dashboard module that loads vendored assets and renders templates. This is the foundation that Phase 3+ will build on.

For now, implement only the asset-loading and template-environment infrastructure:

1. `_load_vendor_asset(filename: str) -> str` — reads a file from the `vendor/` directory relative to this module. Uses `importlib.resources` (Python 3.12+) to locate files within the installed package:
   ```python
   from importlib.resources import files

   def _load_vendor_asset(filename: str) -> str:
       return (
           files("scdm_qa.reporting") / "vendor" / filename
       ).read_text(encoding="utf-8")
   ```

2. `_get_template_env() -> jinja2.Environment` — creates a Jinja2 `Environment` with `PackageLoader`:
   ```python
   def _get_template_env() -> jinja2.Environment:
       return jinja2.Environment(
           loader=jinja2.PackageLoader("scdm_qa.reporting", "templates"),
           autoescape=True,
       )
   ```

3. `_render_page(template_name: str, **context: object) -> str` — loads vendor assets, merges them into context, renders template:
   ```python
   def _render_page(template_name: str, **context: object) -> str:
       env = _get_template_env()
       template = env.get_template(template_name)
       vendor_context = {
           "tabulator_js": _load_vendor_asset("tabulator.min.js"),
           "tabulator_css": _load_vendor_asset("tabulator.min.css"),
           "plotly_js": _load_vendor_asset("plotly-basic.min.js"),
       }
       return template.render(**vendor_context, **context)
   ```

4. A placeholder `save_dashboard()` function signature (will be fleshed out in Phase 3):
   ```python
   def save_dashboard(
       output_dir: Path,
       results: list[tuple[ValidationResult, ProfilingResult]],
       *,
       max_failing_rows: int = 500,
   ) -> Path:
       """Render the full dashboard. Implemented in Phase 3+."""
       raise NotImplementedError("Dashboard rendering not yet implemented")
   ```

Imports:
```python
from __future__ import annotations

from pathlib import Path
from importlib.resources import files

import jinja2

from scdm_qa.profiling.results import ProfilingResult
from scdm_qa.validation.results import ValidationResult
```

**Verification:**
Run: `python -c "from scdm_qa.reporting.dashboard import _render_page; print('ok')"`
Expected: `ok`

Run a quick render test:
```python
python -c "
from scdm_qa.reporting.dashboard import _render_page
html = _render_page('base.html', page_title='Test')
assert '<!DOCTYPE html>' in html
assert 'Tabulator' in html or 'tabulator' in html.lower()
assert 'Plotly' in html or 'plotly' in html.lower()
print('base template renders with inlined assets')
"
```
Expected: `base template renders with inlined assets`

**Commit:** `feat: add dashboard.py with asset loading and template environment`

<!-- END_TASK_4 -->

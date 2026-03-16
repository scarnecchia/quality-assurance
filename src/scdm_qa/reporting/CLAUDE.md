# Reporting

Last verified: 2026-03-11

## Purpose
Renders self-contained HTML dashboard reports from validation and profiling results.
All JS/CSS assets are vendored and inlined so reports work offline with zero external requests.

## Contracts
- **Exposes**: `save_dashboard(output_dir, results, max_failing_rows=500) -> Path`, `serialise_run(results, max_failing_rows=500) -> dict`
- **Guarantees**: Produces index.html (scorecard, charts, summary grid) plus one {table_key}.html detail page per table. All output is self-contained HTML. JSON schema version is "1.0".
- **Expects**: `results` is `list[tuple[ValidationResult, ProfilingResult]]`. Output directory is created if missing.

## Dependencies
- **Uses**: `scdm_qa.validation.results` (StepResult, ValidationResult), `scdm_qa.profiling.results` (ProfilingResult), Jinja2
- **Used by**: `scdm_qa.pipeline` (sole caller of `save_dashboard`)
- **Boundary**: No direct data reading or validation logic; receives only result objects

## Key Decisions
- Vendored Tabulator 6.4.0 + Plotly-basic 3.4.0: Avoids CDN dependency for offline/air-gapped environments
- Single `save_dashboard` call at end of pipeline (not per-table): Enables cross-table summary on index page
- Index page strips failing_rows from embedded JSON: Keeps index.html small; detail pages carry full data

## Key Files
- `serialise.py` - Converts result dataclasses to JSON-serialisable dicts
- `dashboard.py` - Template rendering, vendor asset loading, file output
- `templates/` - Jinja2 templates (base.html, index.html, detail.html)
- `vendor/` - Vendored JS/CSS assets (do not edit manually)

## Invariants
- Vendor assets are loaded via `importlib.resources`, never relative paths
- Template rendering always inlines all vendor assets (no external refs)
- `serialise_run` output includes `schema_version` for forward compatibility

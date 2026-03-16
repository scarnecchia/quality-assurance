# scdm-qa

Validates health data tables conforming to the [Sentinel Common Data Model (SCDM)](https://www.sentinelinitiative.org/methods-data-tools/sentinel-common-data-model) and produces an interactive HTML dashboard with per-column pass/fail summaries, profiling statistics, and failing row extracts.

Validation runs in two phases:

- **L1 (per-table)** — schema conformance, column types, nullability, value sets, uniqueness, sort order
- **L2 (cross-table)** — referential integrity and consistency checks across related tables

Supports Parquet and SAS7BDAT files. Processes data in bounded-memory chunks, so it works on datasets of any size.

## Installation

Requires Python 3.12+.

### With uv (recommended)

Install [uv](https://docs.astral.sh/uv/), then:

```bash
uv sync
```

### With pip

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

pip install .
```

## Configuration

Create a TOML config file mapping SCDM table names to data file paths:

```toml
[tables]
demographic = "/path/to/demographic.parquet"
encounter = "/path/to/encounter.parquet"
enrollment = "/path/to/enrollment.parquet"
dispensing = "/path/to/dispensing.sas7bdat"

[options]
output_dir = "./qa-reports"
```

### Options

All options are optional. Defaults shown below.

| Option | Default | Description |
|--------|---------|-------------|
| `output_dir` | `./qa-reports` | Directory for HTML reports |
| `chunk_size` | `500000` | Rows per chunk (controls memory usage) |
| `max_failing_rows` | `500` | Max failing rows to collect per check |
| `error_threshold` | `0.05` | Failure rate above which a check is an error (affects exit code) |
| `custom_rules_dir` | — | Directory containing custom rule extension files |
| `log_file` | — | Path to write structured JSON logs |
| `verbose` | `false` | Enable verbose console output |
| `run_l1` | `true` | Run per-table (L1) validation |
| `run_l2` | `true` | Run cross-table (L2) validation |

## Usage

All examples below show direct `scdm-qa` commands. If you installed with **uv**, prefix with `uv run`:

```bash
uv run scdm-qa run config.toml
```

If you installed with **pip** into an activated virtual environment, run directly:

```bash
scdm-qa run config.toml
```

### Validate tables

```bash
scdm-qa run config.toml
```

Runs both L1 and L2 validation on all configured tables and writes an interactive HTML dashboard to `output_dir`. The dashboard includes a scorecard, summary charts, and per-table detail pages with failing row extracts.

Validate a single table (L1 only — L2 requires multiple tables):

```bash
scdm-qa run config.toml --table demographic
```

Run only per-table or only cross-table checks:

```bash
scdm-qa run config.toml --l1-only
scdm-qa run config.toml --l2-only
```

These flags are mutually exclusive. They can also be set via `run_l1` / `run_l2` in the config file.

#### Exit codes

- **0** — all checks pass (Note-severity checks are informational and never escalate the exit code)
- **1** — some failures exist but all within the error threshold (warnings)
- **2** — processing errors or at least one check exceeds the error threshold

### Profile only

```bash
scdm-qa profile config.toml
```

Runs column profiling (completeness, cardinality, min/max, value frequencies) without validation rules. Produces the same HTML dashboard but with only the profiling section populated.

Profile a single table:

```bash
scdm-qa profile config.toml --table demographic
```

### View schema definitions

```bash
scdm-qa schema              # list all 19 SCDM tables
scdm-qa schema demographic  # show columns for a specific table
```

### Browse reports

```bash
scdm-qa serve ./qa-reports/
scdm-qa serve ./qa-reports/ --port 9090  # custom port
```

Launches a local HTTP server and opens the report dashboard in your browser.

## Custom rules

To add project-specific validation rules, create a Python file named `{table_key}_rules.py` in a directory and point `custom_rules_dir` at it.

The file must define an `extend_validation` function:

```python
# custom_rules/demographic_rules.py

def extend_validation(validation, data):
    return validation.col_vals_not_null(columns="Birth_Date")
```

Rules are appended to the auto-generated validation chain for that table.

## Development

```bash
uv run pytest          # run tests
uv run scdm-qa --help  # see all commands
```

Without uv, activate the virtual environment first:

```bash
source .venv/bin/activate
pytest                 # run tests
scdm-qa --help         # see all commands
```

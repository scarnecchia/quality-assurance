# scdm-qa

Validates health data tables conforming to the [Sentinel Common Data Model (SCDM)](https://www.sentinelinitiative.org/methods-data-tools/sentinel-common-data-model) and produces HTML reports with per-column pass/fail summaries, profiling statistics, and failing row extracts.

Supports Parquet and SAS7BDAT files. Processes data in bounded-memory chunks, so it works on datasets of any size.

## Installation

Requires Python 3.12+.

### With uv (recommended)

Install [uv](https://docs.astral.sh/uv/), then:

```bash
uv sync
```

For faster global uniqueness checks on Parquet files, install the optional DuckDB dependency:

```bash
uv sync --extra duckdb
```

### With pip

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

pip install .
```

With the optional DuckDB dependency:

```bash
pip install ".[duckdb]"
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

## Usage

### Validate tables

```bash
scdm-qa run config.toml
```

Validates all configured tables against the SCDM spec and writes HTML reports to `output_dir`. Each table gets its own report, plus an `index.html` linking them all.

Validate a single table:

```bash
scdm-qa run config.toml --table demographic
```

Exit codes:
- **0** — all checks pass
- **1** — some failures exist but all within the error threshold (warnings)
- **2** — processing errors or at least one check exceeds the error threshold

### Profile only

```bash
scdm-qa profile config.toml
```

Runs column profiling (completeness, cardinality, min/max, value frequencies) without validation rules. Produces the same HTML reports but with only the profiling section populated.

### View schema definitions

```bash
scdm-qa schema              # list all 19 SCDM tables
scdm-qa schema demographic  # show columns for a specific table
```

### Browse reports

```bash
scdm-qa serve ./qa-reports/
```

Launches a local HTTP server and opens the report index in your browser.

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

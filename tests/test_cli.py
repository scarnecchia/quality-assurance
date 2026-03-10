from __future__ import annotations

from pathlib import Path

import polars as pl
from typer.testing import CliRunner

from scdm_qa.cli import app

runner = CliRunner()


class TestCLIHelp:
    def test_help_shows_subcommands(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "profile" in result.output
        assert "schema" in result.output
        assert "serve" in result.output


class TestSchemaCommand:
    def test_lists_all_tables(self) -> None:
        result = runner.invoke(app, ["schema"])
        assert result.exit_code == 0
        assert "19 SCDM tables" in result.output

    def test_shows_specific_table(self) -> None:
        result = runner.invoke(app, ["schema", "demographic"])
        assert result.exit_code == 0
        assert "PatID" in result.output


class TestRunCommand:
    def _make_config_and_data(self, tmp_path: Path, *, with_nulls: bool = False) -> Path:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        patids = ["P1", "P2", "P3", "P4", "P5"]
        if with_nulls:
            patids[2] = None

        df = pl.DataFrame({
            "PatID": patids,
            "Birth_Date": [1000, 2000, 3000, 4000, 5000],
            "Sex": ["F", "M", "F", "M", "F"],
            "Hispanic": ["Y", "N", "Y", "N", "Y"],
            "Race": ["1", "2", "3", "1", "2"],
        })
        df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\nchunk_size = 2\n'
        )
        return config_file

    def test_produces_reports_for_clean_data(self, tmp_path: Path) -> None:
        config_file = self._make_config_and_data(tmp_path)
        result = runner.invoke(app, ["run", str(config_file)])
        assert result.exit_code == 0
        assert (tmp_path / "reports" / "demographic.html").exists()
        assert (tmp_path / "reports" / "index.html").exists()

    def test_exit_code_2_when_failures_exceed_threshold(self, tmp_path: Path) -> None:
        config_file = self._make_config_and_data(tmp_path, with_nulls=True)
        result = runner.invoke(app, ["run", str(config_file)])
        # 1 null out of 5 = 20% failure rate > default 5% threshold → exit 2
        assert result.exit_code == 2

    def test_missing_config_exits_2(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["run", str(tmp_path / "missing.toml")])
        assert result.exit_code == 2


class TestRunCommandTableIsolation:
    def test_one_table_failure_doesnt_block_others(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Good table
        pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }).write_parquet(data_dir / "demographic.parquet")

        # Bad table (missing file to trigger error)
        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\n'
            f'demographic = "{data_dir / "demographic.parquet"}"\n'
            f'enrollment = "{data_dir / "nonexistent.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\n'
        )
        result = runner.invoke(app, ["run", str(config_file)])
        # Should still create demographic report even though enrollment fails
        assert (tmp_path / "reports" / "demographic.html").exists()


class TestProfileCommand:
    def test_runs_profiling_only(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
        }).write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            f'[tables]\ndemographic = "{data_dir / "demographic.parquet"}"\n\n'
            f'[options]\noutput_dir = "{output_dir}"\n'
        )
        result = runner.invoke(app, ["profile", str(config_file)])
        assert result.exit_code == 0
        assert "profiled" in result.output


class TestServeCommand:
    def test_nonexistent_dir_exits_2(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["serve", str(tmp_path / "nope")])
        assert result.exit_code == 2

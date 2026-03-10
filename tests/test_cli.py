from __future__ import annotations

from pathlib import Path

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


class TestRunCommand:
    def test_run_with_valid_config(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/enrollment.parquet"\n'
        )
        result = runner.invoke(app, ["run", str(config_file)])
        assert result.exit_code == 0

    def test_run_with_missing_config(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["run", str(tmp_path / "missing.toml")])
        assert result.exit_code == 2

    def test_run_with_table_filter(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/enrollment.parquet"\n'
        )
        result = runner.invoke(app, ["run", str(config_file), "--table", "enrollment"])
        assert result.exit_code == 0


class TestProfileCommand:
    def test_profile_with_valid_config(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/enrollment.parquet"\n'
        )
        result = runner.invoke(app, ["profile", str(config_file)])
        assert result.exit_code == 0


class TestServeCommand:
    def test_serve_prints_stub_message(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["serve", str(tmp_path)])
        assert result.exit_code == 0
        assert "not yet implemented" in result.output

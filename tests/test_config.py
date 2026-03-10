from __future__ import annotations

from pathlib import Path

import pytest

from scdm_qa.config import ConfigError, QAConfig, load_config


class TestLoadConfig:
    def test_loads_minimal_config(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/enrollment.parquet"\n'
        )
        cfg = load_config(config_file)

        assert "enrollment" in cfg.tables
        assert cfg.tables["enrollment"] == Path("/data/enrollment.parquet")
        assert cfg.chunk_size == 500_000
        assert cfg.output_dir == Path("./qa-reports")

    def test_loads_full_config(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\n'
            'enrollment = "/data/enrollment.parquet"\n'
            'demographic = "/data/demographic.sas7bdat"\n'
            '\n'
            '[options]\n'
            'chunk_size = 100000\n'
            'output_dir = "./output"\n'
            'max_failing_rows = 100\n'
            'error_threshold = 0.10\n'
            'custom_rules_dir = "./rules"\n'
            'log_file = "./logs/qa.log"\n'
            'verbose = true\n'
        )
        cfg = load_config(config_file)

        assert len(cfg.tables) == 2
        assert cfg.chunk_size == 100_000
        assert cfg.output_dir == Path("./output")
        assert cfg.max_failing_rows == 100
        assert cfg.error_threshold == 0.10
        assert cfg.custom_rules_dir == Path("./rules")
        assert cfg.log_file == Path("./logs/qa.log")
        assert cfg.verbose is True

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="config file not found"):
            load_config(tmp_path / "nonexistent.toml")

    def test_raises_on_non_toml_extension(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text("")
        with pytest.raises(ConfigError, match="must be .toml"):
            load_config(config_file)

    def test_raises_on_missing_tables_section(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text('[options]\nchunk_size = 100\n')
        with pytest.raises(ConfigError, match="must contain a \\[tables\\] section"):
            load_config(config_file)

    def test_raises_on_invalid_chunk_size(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[tables]\nenrollment = "/data/e.parquet"\n\n'
            '[options]\nchunk_size = -1\n'
        )
        with pytest.raises(ConfigError, match="chunk_size must be a positive integer"):
            load_config(config_file)


class TestQAConfig:
    def test_is_frozen(self) -> None:
        cfg = QAConfig(tables={"a": Path("/a")})
        with pytest.raises(AttributeError):
            cfg.chunk_size = 999  # type: ignore[misc]

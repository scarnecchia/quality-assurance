from __future__ import annotations

import json
import logging
from pathlib import Path

import structlog

from scdm_qa.logging import configure_logging, get_logger


class TestConfigureLogging:
    def test_configures_console_handler(self) -> None:
        configure_logging()

        root = logging.getLogger()
        assert any(isinstance(h, logging.StreamHandler) for h in root.handlers)

    def test_configures_file_handler_when_log_file_provided(self, tmp_path: Path) -> None:
        log_file = tmp_path / "test.log"
        configure_logging(log_file=log_file)

        root = logging.getLogger()
        assert any(isinstance(h, logging.FileHandler) for h in root.handlers)

    def test_file_handler_writes_json(self, tmp_path: Path) -> None:
        log_file = tmp_path / "test.log"
        configure_logging(log_file=log_file)

        logger = get_logger("test")
        logger.info("test_event", key="value")

        content = log_file.read_text().strip()
        assert content, "log file should not be empty"
        record = json.loads(content.splitlines()[-1])
        assert record["event"] == "test_event"
        assert record["key"] == "value"

    def test_creates_log_directory_if_missing(self, tmp_path: Path) -> None:
        log_file = tmp_path / "subdir" / "nested" / "test.log"
        configure_logging(log_file=log_file)

        assert log_file.parent.exists()


class TestGetLogger:
    def test_returns_bound_logger(self) -> None:
        configure_logging()
        logger = get_logger("test")
        assert hasattr(logger, "info")
        assert hasattr(logger, "debug")
        assert hasattr(logger, "warning")
        assert hasattr(logger, "error")

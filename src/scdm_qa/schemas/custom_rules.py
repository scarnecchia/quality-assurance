from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Callable

import pointblank as pb
import polars as pl
import structlog

log = structlog.get_logger(__name__)

ExtendFn = Callable[[pb.Validate, pl.DataFrame], pb.Validate]


def load_custom_rules(
    table_key: str,
    custom_rules_dir: Path | None,
) -> ExtendFn | None:
    if custom_rules_dir is None:
        return None

    rules_file = custom_rules_dir / f"{table_key}_rules.py"
    if not rules_file.exists():
        return None

    log.info("loading custom rules", table=table_key, file=str(rules_file))

    spec = importlib.util.spec_from_file_location(
        f"scdm_qa_custom.{table_key}_rules",
        rules_file,
    )
    if spec is None or spec.loader is None:
        log.warning("failed to load custom rules module", file=str(rules_file))
        return None

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        log.warning(
            "failed to load custom rules module due to error",
            file=str(rules_file),
            error=str(e),
        )
        return None

    extend_fn = getattr(module, "extend_validation", None)
    if extend_fn is None:
        log.warning(
            "custom rules file missing extend_validation function",
            file=str(rules_file),
        )
        return None

    return extend_fn


def apply_custom_rules(
    validation: pb.Validate,
    data: pl.DataFrame,
    extend_fn: ExtendFn | None,
) -> pb.Validate:
    if extend_fn is None:
        return validation
    try:
        return extend_fn(validation, data)
    except Exception as e:
        log.warning(
            "failed to apply custom rules due to error",
            error=str(e),
        )
        return validation

from __future__ import annotations

from scdm_qa.schemas.models import ColumnDef, ConditionalRule, TableSchema
from scdm_qa.schemas.parser import parse_spec
from scdm_qa.schemas.validation import build_validation

_REGISTRY: dict[str, TableSchema] | None = None


def get_registry() -> dict[str, TableSchema]:
    global _REGISTRY
    if _REGISTRY is None:
        tables = parse_spec()
        _REGISTRY = {t.table_key: t for t in tables}
    return _REGISTRY


def get_schema(table_key: str) -> TableSchema:
    registry = get_registry()
    if table_key not in registry:
        available = sorted(registry.keys())
        raise KeyError(
            f"unknown table key: {table_key!r}. Available: {available}"
        )
    return registry[table_key]


def list_table_keys() -> list[str]:
    return sorted(get_registry().keys())


__all__ = [
    "ColumnDef",
    "ConditionalRule",
    "TableSchema",
    "build_validation",
    "get_registry",
    "get_schema",
    "list_table_keys",
    "parse_spec",
]

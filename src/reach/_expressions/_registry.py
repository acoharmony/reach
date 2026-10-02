"""Central registry for REACH Spark expression builders."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import Any

ExpressionBuilder = Callable[..., Any]

EXPRESSION_DEFINITION_MODULES = (
    "reach._expressions._alignment",
    "reach._expressions._bnmr",
    "reach._expressions._copay_waiver",
    "reach._expressions._hedr",
    "reach._expressions._high_needs",
    "reach._expressions._participant",
    "reach._expressions._quality",
)


class ExpressionRegistry:
    """Central catalog of pure REACH Spark expression builders."""

    _builders: dict[str, ExpressionBuilder] = {}
    _metadata: dict[str, dict[str, Any]] = {}
    _loaded = False

    @classmethod
    def register(cls, name: str, metadata: dict[str, Any] | None = None) -> Callable[[ExpressionBuilder], ExpressionBuilder]:
        """Return a decorator that records an expression builder under ``name``."""

        def decorator(func: ExpressionBuilder) -> ExpressionBuilder:
            cls._builders[name] = func
            cls._metadata[name] = metadata or {}
            return func

        return decorator

    @classmethod
    def load_definitions(cls) -> None:
        """Import definition modules once so decorators populate the registry."""
        if cls._loaded:
            return
        for module in EXPRESSION_DEFINITION_MODULES:
            import_module(module)
        cls._loaded = True

    @classmethod
    def get(cls, name: str) -> ExpressionBuilder:
        """Return a registered expression builder or raise with known names."""
        cls.load_definitions()
        try:
            return cls._builders[name]
        except KeyError as exc:
            available = ", ".join(sorted(cls._builders)) or "<none>"
            raise KeyError(f"Unknown REACH expression {name!r}. Available expressions: {available}.") from exc

    @classmethod
    def list_names(cls, *, concern: str | None = None) -> tuple[str, ...]:
        """Return registered expression names, optionally filtered by concern."""
        cls.load_definitions()
        if concern is None:
            return tuple(sorted(cls._builders))
        return tuple(sorted(name for name, meta in cls._metadata.items() if meta.get("concern") == concern))

    @classmethod
    def metadata(cls, name: str) -> dict[str, Any]:
        """Return a copy of expression metadata."""
        cls.get(name)
        return dict(cls._metadata.get(name, {}))


def register_expression(name: str, **metadata: Any) -> Callable[[ExpressionBuilder], ExpressionBuilder]:
    """Register a Spark expression builder."""
    return ExpressionRegistry.register(name, metadata)

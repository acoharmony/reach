"""Central registry for REACH Spark transforms."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import Any

TransformBuilder = Callable[..., Any]

TRANSFORM_DEFINITION_MODULES = (
    "reach._transforms._alignment",
    "reach._transforms._bnmr",
    "reach._transforms._clinical_oversight",
    "reach._transforms._copay_waiver",
    "reach._transforms._giftcards",
    "reach._transforms._hedr",
    "reach._transforms._high_needs",
    "reach._transforms._high_needs_reconciliation",
    "reach._transforms._quality",
)


class TransformRegistry:
    """Central catalog of Spark transforms; each builder returns one DataFrame."""

    _builders: dict[str, TransformBuilder] = {}
    _metadata: dict[str, dict[str, Any]] = {}
    _loaded = False

    @classmethod
    def register(cls, name: str, metadata: dict[str, Any] | None = None) -> Callable[[TransformBuilder], TransformBuilder]:
        """Return a decorator that records a transform builder under ``name``."""

        def decorator(func: TransformBuilder) -> TransformBuilder:
            cls._builders[name] = func
            cls._metadata[name] = metadata or {}
            return func

        return decorator

    @classmethod
    def load_definitions(cls) -> None:
        """Import transform modules once so decorators populate the registry."""
        if cls._loaded:
            return
        for module in TRANSFORM_DEFINITION_MODULES:
            import_module(module)
        cls._loaded = True

    @classmethod
    def get(cls, name: str) -> TransformBuilder:
        """Return a registered transform builder or raise with known names."""
        cls.load_definitions()
        try:
            return cls._builders[name]
        except KeyError as exc:
            available = ", ".join(sorted(cls._builders)) or "<none>"
            raise KeyError(f"Unknown REACH transform {name!r}. Available transforms: {available}.") from exc

    @classmethod
    def list_names(cls, *, concern: str | None = None) -> tuple[str, ...]:
        """Return registered transform names, optionally filtered by concern."""
        cls.load_definitions()
        if concern is None:
            return tuple(sorted(cls._builders))
        return tuple(sorted(name for name, meta in cls._metadata.items() if meta.get("concern") == concern))

    @classmethod
    def metadata(cls, name: str) -> dict[str, Any]:
        """Return a copy of transform metadata."""
        cls.get(name)
        return dict(cls._metadata.get(name, {}))


def register_transform(name: str, **metadata: Any) -> Callable[[TransformBuilder], TransformBuilder]:
    """Register a Spark transform builder."""
    return TransformRegistry.register(name, metadata)

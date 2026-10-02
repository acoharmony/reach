"""Central registry for REACH analytic pipe definitions."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import Any

from reach._pipes._builder import PipeStage

PipeFactory = Callable[[], tuple[PipeStage, ...]]

PIPE_DEFINITION_MODULES = (
    "reach._pipes._audit",
    "reach._pipes._clinical_oversight",
    "reach._pipes._warehouse",
)


class PipeRegistry:
    """Central registry for named REACH Spark analytic pipelines."""

    _pipes: dict[str, PipeFactory] = {}
    _metadata: dict[str, dict[str, Any]] = {}
    _loaded = False

    @classmethod
    def register(cls, name: str, metadata: dict[str, Any] | None = None) -> Callable[[PipeFactory], PipeFactory]:
        """Return a decorator that records a pipe factory."""

        def decorator(func: PipeFactory) -> PipeFactory:
            cls._pipes[name] = func
            cls._metadata[name] = metadata or {}
            return func

        return decorator

    @classmethod
    def load_definitions(cls) -> None:
        """Import pipe definition modules once."""
        if cls._loaded:
            return
        for module in PIPE_DEFINITION_MODULES:
            import_module(module)
        cls._loaded = True

    @classmethod
    def get(cls, name: str) -> PipeFactory:
        """Return a registered pipe factory or raise with known names."""
        cls.load_definitions()
        try:
            return cls._pipes[name]
        except KeyError as exc:
            available = ", ".join(sorted(cls._pipes)) or "<none>"
            raise KeyError(f"Unknown REACH pipe {name!r}. Available pipes: {available}.") from exc

    @classmethod
    def list_names(cls) -> tuple[str, ...]:
        """Return registered pipe names."""
        cls.load_definitions()
        return tuple(sorted(cls._pipes))

    @classmethod
    def metadata(cls, name: str) -> dict[str, Any]:
        """Return a copy of metadata for a registered pipe."""
        cls.get(name)
        return dict(cls._metadata.get(name, {}))


def register_pipe(name: str, **metadata: Any) -> Callable[[PipeFactory], PipeFactory]:
    """Register a REACH pipe definition."""
    return PipeRegistry.register(name, metadata)

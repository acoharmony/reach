"""Small pipeline stage definitions for REACH Spark warehouse outputs."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class PipeStage:
    """One ordered REACH analytic stage."""

    name: str
    transform_name: str
    output_table: str
    concern: str
    order: int
    depends_on: tuple[str, ...] = ()

    def __repr__(self) -> str:
        deps = f" -> depends on {', '.join(self.depends_on)}" if self.depends_on else ""
        return f"PipeStage({self.order}: {self.name} [{self.concern}] => {self.output_table}{deps})"


def ordered_pipe_stages(stages: Iterable[PipeStage]) -> tuple[PipeStage, ...]:
    """Return stages in canonical order after validating unique names and outputs."""
    ordered = tuple(sorted(stages, key=lambda stage: stage.order))
    names = [stage.name for stage in ordered]
    outputs = [stage.output_table for stage in ordered]
    duplicate_names = sorted({name for name in names if names.count(name) > 1})
    duplicate_outputs = sorted({output for output in outputs if outputs.count(output) > 1})
    if duplicate_names:
        raise ValueError(f"Duplicate REACH pipe stage names: {', '.join(duplicate_names)}.")
    if duplicate_outputs:
        raise ValueError(f"Duplicate REACH pipe output tables: {', '.join(duplicate_outputs)}.")
    return ordered


def _stage_by_identifier(stages: tuple[PipeStage, ...], identifier: str) -> PipeStage:
    by_name = {stage.name: stage for stage in stages}
    by_output = {stage.output_table: stage for stage in stages}
    try:
        return by_name[identifier]
    except KeyError:
        try:
            return by_output[identifier]
        except KeyError as exc:
            available = ", ".join(stage.output_table for stage in stages)
            raise ValueError(f"Unknown REACH pipe stage or output {identifier!r}. Available outputs: {available}.") from exc


def select_pipe_stages(stages: Iterable[PipeStage], *, outputs: Iterable[str] = (), through_output: str = "") -> tuple[PipeStage, ...]:
    """Select an ordered subset by output/stage name, including dependencies."""
    ordered = ordered_pipe_stages(stages)
    requested = tuple(output for output in outputs if output)
    if through_output and requested:
        raise ValueError("Use either explicit output selection or through-output selection, not both.")
    if through_output:
        target = _stage_by_identifier(ordered, through_output)
        return tuple(stage for stage in ordered if stage.order <= target.order)
    if not requested:
        return ordered
    selected: set[str] = set()

    def visit(stage: PipeStage) -> None:
        if stage.name in selected:
            return
        for dependency in stage.depends_on:
            visit(_stage_by_identifier(ordered, dependency))
        selected.add(stage.name)

    for output in requested:
        visit(_stage_by_identifier(ordered, output))
    return tuple(stage for stage in ordered if stage.name in selected)

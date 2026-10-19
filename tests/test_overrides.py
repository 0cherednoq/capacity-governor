"""Команды оператора: `pin`, `unpin`, `pause`, `resume` (G7).

Автоматический контур, который может поставить всем окно 0, обязан иметь выключатель,
иначе во время инцидента оператору остаётся только править таблицу руками.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest

from capacity import ConsumerState, EligibleCapacity, Governor, MaxMinFair, Stabilizer
from capacity.ports import OverrideStore
from tests.fakes import (
    FakeCapacity,
    FakeClock,
    FakeRegistry,
    FakeScope,
    FakeState,
    FakeWindows,
)

NOW = datetime(2026, 10, 19, 18, 0, 0, tzinfo=UTC)


@dataclass(slots=True)
class WritableOverrides:
    """Переопределения с записью. В проекте это `PostgresOverrideStore`."""

    pins: dict[str, int] = field(default_factory=dict)
    is_paused: bool = False
    writes: list[tuple[str, object]] = field(default_factory=list)

    async def pinned(self) -> Mapping[str, int]:
        return dict(self.pins)

    async def paused(self) -> bool:
        return self.is_paused

    async def pin(self, consumer_id: str, limit: int) -> None:
        self.pins[consumer_id] = limit
        self.writes.append(("pin", (consumer_id, limit)))

    async def unpin(self, consumer_id: str) -> None:
        self.pins.pop(consumer_id, None)
        self.writes.append(("unpin", consumer_id))

    async def set_paused(self, paused: bool) -> None:
        self.is_paused = paused
        self.writes.append(("paused", paused))


def advance(governor: Governor, seconds: int) -> None:
    """Сдвинуть часы контура: такт за тактом, без настоящего ожидания."""
    clock = governor.clock
    assert isinstance(clock, FakeClock)
    clock.advance(timedelta(seconds=seconds))


def build(
    *,
    free: int = 10,
    demands: tuple[int, ...] = (100, 100),
    overrides: WritableOverrides | None = None,
) -> tuple[Governor, WritableOverrides, FakeWindows]:
    store = overrides or WritableOverrides()
    windows = FakeWindows({})
    governor = Governor(
        scope=FakeScope(),
        capacity_source=FakeCapacity(
            EligibleCapacity(eligible=free, free=free, work_units=free * 10)
        ),
        registry=FakeRegistry(
            [ConsumerState(f"c{i}", demand=d) for i, d in enumerate(demands)]
        ),
        windows=windows,
        state=FakeState(),
        policy=MaxMinFair(),
        clock=FakeClock(NOW),
        stabilizer=Stabilizer(),
        overrides=store,
        min_tick_interval=timedelta(seconds=5),
    )
    return governor, store, windows


# --- pin ----------------------------------------------------------------------------


async def test_pin_is_written_to_the_store_not_kept_in_memory() -> None:
    """Лидер меняется между тактами, поэтому пин обязан лежать вне процесса (G7)."""
    governor, store, _ = build()

    await governor.pin("c0", 2)

    assert store.pins == {"c0": 2}
    assert store.writes == [("pin", ("c0", 2))]


async def test_pinned_window_is_not_written_but_capacity_is_spent() -> None:
    governor, _, windows = build(free=10)

    await governor.pin("c0", 3)
    await governor.tick()

    assert windows.windows == {"c1": 7}


async def test_unpin_returns_the_consumer_to_the_allocation() -> None:
    """Снятый пин снова участвует в раздаче — его окно пишется сразу.

    Сосед при этом остаётся с прежним, более широким окном: его уменьшение держит
    выдержка стабилизатора (G5). Так и задумано — вниз контур идёт медленно, и
    снятие пина не исключение.
    """
    governor, store, windows = build(free=10)

    await governor.pin("c0", 3)
    await governor.tick()
    await governor.unpin("c0")
    advance(governor, 5)
    await governor.tick()

    assert store.pins == {}
    assert windows.windows == {"c0": 5, "c1": 7}

    advance(governor, 301)
    await governor.tick()

    assert windows.windows == {"c0": 5, "c1": 5}


async def test_pin_to_zero_stops_one_consumer_only() -> None:
    """Оператору нужно уметь остановить одного, не трогая остальных."""
    governor, _, windows = build(free=10)

    await governor.pin("c0", 0)
    await governor.tick()

    assert windows.windows == {"c1": 10}


async def test_negative_pin_is_rejected() -> None:
    governor, store, _ = build()

    with pytest.raises(ValueError, match="limit < 0"):
        await governor.pin("c0", -1)
    assert store.writes == []


# --- pause --------------------------------------------------------------------------


async def test_pause_stops_writing_windows_but_keeps_counting() -> None:
    """«Это контур дёргает окна или нет» — вопрос, на который пауза и отвечает."""
    governor, store, windows = build(free=10, demands=(100,))

    await governor.pause()
    outcome = await governor.tick()

    assert store.is_paused is True
    assert windows.writes == []
    assert outcome.paused is True
    assert outcome.plan is not None
    assert outcome.plan.allocation.grants == {"c0": 10}


async def test_resume_brings_the_windows_back() -> None:
    governor, store, windows = build(free=10, demands=(100,))

    await governor.pause()
    await governor.tick()
    await governor.resume()
    advance(governor, 5)
    await governor.tick()

    assert store.is_paused is False
    assert windows.windows == {"c0": 10}


async def test_pause_leaves_current_windows_untouched() -> None:
    """Пауза оставляет то, что стоит, а не обнуляет: иначе это не пауза, а стоп."""
    governor, _, windows = build(free=10, demands=(100,))
    await governor.tick()
    standing = dict(windows.windows)

    await governor.pause()
    advance(governor, 5)
    # Ёмкость упала до нуля: без паузы контур немедленно закрыл бы окна.
    governor.capacity_source = FakeCapacity(
        EligibleCapacity(eligible=0, free=0, work_units=0)
    )
    await governor.tick()

    assert windows.windows == standing


# --- контур без переопределений -----------------------------------------------------


async def test_commands_require_a_store() -> None:
    """Контур можно собрать без переопределений, но тогда команды честно падают."""
    governor, _, _ = build()
    governor.overrides = None

    for command in (
        governor.pin("c0", 1),
        governor.unpin("c0"),
        governor.pause(),
        governor.resume(),
    ):
        with pytest.raises(RuntimeError, match="OverrideStore"):
            await command


def test_writable_store_satisfies_the_port() -> None:
    assert isinstance(WritableOverrides(), OverrideStore)

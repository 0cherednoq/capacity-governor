"""Поддельные реализации портов: контур проверяется целиком без БД.

Они же — образец того, что проект должен написать поверх Postgres, Tallyho и
Identity Hub: набор методов здесь ровно тот, который требуют порты.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from capacity.types import ConsumerState, EligibleCapacity, Hold, SupplySnapshot


@dataclass(slots=True)
class FakeClock:
    moment: datetime

    def now(self) -> datetime:
        return self.moment

    def advance(self, delta: timedelta) -> None:
        self.moment += delta


@dataclass(slots=True)
class FakeScope:
    """Транзакция такта с лидерством. `leading=False` — лок взял кто-то другой."""

    leading: bool = True
    entered: int = 0
    committed: int = 0

    @asynccontextmanager
    async def lead(self) -> AsyncIterator[bool]:
        self.entered += 1
        yield self.leading
        if self.leading:
            self.committed += 1


@dataclass(slots=True)
class FakeCapacity:
    value: EligibleCapacity

    async def capacity(self) -> EligibleCapacity:
        return self.value


@dataclass(slots=True)
class FailingCapacity:
    """Источник ёмкости, который недоступен: проверка поведения контура на сбое."""

    async def capacity(self) -> EligibleCapacity:
        raise RuntimeError("пул недоступен")


@dataclass(slots=True)
class FakeRegistry:
    states: Sequence[ConsumerState]

    async def consumers(self) -> Sequence[ConsumerState]:
        return self.states


@dataclass(slots=True)
class FakeWindows:
    """Окна Tallyho: абсолютные значения, пишутся одной транзакцией."""

    windows: dict[str, int] = field(default_factory=dict)
    writes: list[Mapping[str, int]] = field(default_factory=list)

    async def current_windows(self) -> Mapping[str, int]:
        return dict(self.windows)

    async def set_windows(self, windows: Mapping[str, int]) -> None:
        self.writes.append(dict(windows))
        self.windows.update(windows)


@dataclass(slots=True)
class FakeState:
    deficits: Mapping[str, int] = field(default_factory=dict)
    holds: Mapping[str, Hold] = field(default_factory=dict)
    saves: int = 0

    async def load(self) -> tuple[Mapping[str, int], Mapping[str, Hold]]:
        return dict(self.deficits), dict(self.holds)

    async def save(self, deficits: Mapping[str, int], holds: Mapping[str, Hold]) -> None:
        self.deficits = dict(deficits)
        self.holds = dict(holds)
        self.saves += 1


@dataclass(slots=True)
class FakeOverrides:
    pins: dict[str, int] = field(default_factory=dict)
    is_paused: bool = False

    async def pinned(self) -> Mapping[str, int]:
        return dict(self.pins)

    async def paused(self) -> bool:
        return self.is_paused

    async def pin(self, consumer_id: str, limit: int) -> None:
        self.pins[consumer_id] = limit

    async def unpin(self, consumer_id: str) -> None:
        self.pins.pop(consumer_id, None)

    async def set_paused(self, paused: bool) -> None:
        self.is_paused = paused


@dataclass(slots=True)
class FakeSupply:
    value: SupplySnapshot

    async def snapshot(self) -> SupplySnapshot:
        return self.value


@dataclass(slots=True)
class FakeReplenisher:
    orders: list[tuple[int, str]] = field(default_factory=list)

    async def order(self, count: int, window_key: str) -> None:
        self.orders.append((count, window_key))


@dataclass(slots=True)
class FakeObserver:
    reports: list[object] = field(default_factory=list)

    async def observe(self, report: object) -> None:
        self.reports.append(report)

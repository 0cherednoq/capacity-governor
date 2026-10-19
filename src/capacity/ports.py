"""Порты (G1). Только `Protocol`: реализации пишет проект, ядро их не знает."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol, runtime_checkable

from capacity.types import (
    Allocation,
    ConsumerState,
    EligibleCapacity,
    Hold,
    SupplySnapshot,
)


@runtime_checkable
class AllocationPolicy(Protocol):
    """Чистая функция раздачи: ни БД, ни времени, ни номера такта (G1)."""

    def allocate(
        self,
        capacity: int,
        consumers: Sequence[ConsumerState],
        deficits: Mapping[str, int],
        holds: Mapping[str, Hold] | None = None,
    ) -> Allocation: ...


@runtime_checkable
class CapacitySource(Protocol):
    """Откуда берётся ёмкость пула. Отдаёт структуру, не число (G1.3)."""

    def capacity(self) -> EligibleCapacity: ...


@runtime_checkable
class SupplySource(Protocol):
    """Снимок запаса для политики пополнения. Бюджетный журнал ведёт проект (G4)."""

    def snapshot(self) -> SupplySnapshot: ...


@runtime_checkable
class ConsumerRegistry(Protocol):
    """Кто сейчас претендует на ёмкость и сколько просит."""

    def consumers(self) -> Sequence[ConsumerState]: ...


@runtime_checkable
class WindowSink(Protocol):
    """Куда применяются окна. Абсолютные значения, все окна такта — одной транзакцией (G6)."""

    def current_windows(self) -> Mapping[str, int]: ...

    def set_windows(self, windows: Mapping[str, int]) -> None: ...


@runtime_checkable
class Replenisher(Protocol):
    """Постановка заказа на пополнение. Идемпотентность — через ключ окна (G4)."""

    def order(self, count: int, window_key: str) -> None: ...


@runtime_checkable
class DeficitStore(Protocol):
    """Состояние политики между тактами: лидер меняется, значит оно вне памяти (G6, G7).

    Дефициты DRR и удержания окон переносятся вместе: по отдельности они
    рассогласуются, и потребитель получит либо чужое удержание, либо чужую очередь.
    """

    def load(self) -> tuple[Mapping[str, int], Mapping[str, Hold]]: ...

    def save(self, deficits: Mapping[str, int], holds: Mapping[str, Hold]) -> None: ...


@runtime_checkable
class OverrideStore(Protocol):
    """Ручное переопределение: `pin`/`pause` живут в БД, не в памяти процесса (G7)."""

    def pinned(self) -> Mapping[str, int]: ...

    def paused(self) -> bool: ...


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime: ...

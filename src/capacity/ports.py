"""Порты (G1). Только `Protocol`: реализации пишет проект, ядро их не знает.

Политика остаётся синхронной и чистой — это функция. Всё, что ходит в БД и к соседним
сервисам, объявлено асинхронным: Tallyho и Identity Hub асинхронны, и заворачивать их
в синхронный фасад значит либо блокировать цикл событий, либо держать отдельный пул
потоков ради вызовов, которые и так ждут сеть.

Транзакция такта в порты не протекает. Контур открывает `TickScope`, а реализация
внутри него сама знает, в какой сессии работают её методы: иначе тип сессии проекта
попал бы в сигнатуры ядра и переносимость фазы A (G0) кончилась бы на первом адаптере.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
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
class TickScope(Protocol):
    """Транзакция такта вместе с лидерством (G6).

    Лидерство берётся транзакционным `pg_try_advisory_xact_lock` на каждый такт:
    сессионный лок держится за соединением и под PgBouncer в режиме transaction pooling
    либо теряется, либо удерживается не тем, кто думает. Транзакционный снимается
    коммитом и безопасен под любым пулером.

    `lead()` отдаёт `True`, если лок взят. `False` — такт пропущен, лидер сейчас
    другой; это не ошибка и не требует ни healthcheck'а, ни состояния лидерства.
    """

    def lead(self) -> AbstractAsyncContextManager[bool]: ...


@runtime_checkable
class CapacitySource(Protocol):
    """Откуда берётся ёмкость пула. Отдаёт структуру, не число (G1.3)."""

    async def capacity(self) -> EligibleCapacity: ...


@runtime_checkable
class SupplySource(Protocol):
    """Снимок запаса для политики пополнения. Бюджетный журнал ведёт проект (G4)."""

    async def snapshot(self) -> SupplySnapshot: ...


@runtime_checkable
class ConsumerRegistry(Protocol):
    """Кто сейчас претендует на ёмкость и сколько просит."""

    async def consumers(self) -> Sequence[ConsumerState]: ...


@runtime_checkable
class WindowSink(Protocol):
    """Куда применяются окна. Абсолютные значения, все окна такта — одной транзакцией (G6)."""

    async def current_windows(self) -> Mapping[str, int]: ...

    async def set_windows(self, windows: Mapping[str, int]) -> None: ...


@runtime_checkable
class Replenisher(Protocol):
    """Постановка заказа на пополнение. Идемпотентность — через ключ окна (G4).

    Вызывается вне транзакции такта: это постановка работы, а не чтение.
    """

    async def order(self, count: int, window_key: str) -> None: ...


@runtime_checkable
class StateStore(Protocol):
    """Состояние политики между тактами: лидер меняется, значит оно вне памяти (G6, G7).

    Дефициты DRR и удержания окон переносятся вместе: по отдельности они
    рассогласуются, и потребитель получит либо чужое удержание, либо чужую очередь.
    """

    async def load(self) -> tuple[Mapping[str, int], Mapping[str, Hold]]: ...

    async def save(
        self, deficits: Mapping[str, int], holds: Mapping[str, Hold]
    ) -> None: ...


@runtime_checkable
class OverrideStore(Protocol):
    """Ручное переопределение: `pin`/`pause` живут в БД, не в памяти процесса (G7).

    Запись здесь такая же обязательная часть, как чтение: автоматический контур, который
    может поставить всем окно 0, обязан иметь выключатель, а оператору во время инцидента
    некогда разбираться, в какую таблицу писать руками.

    Эти методы вызываются **вне** транзакции такта: оператор не ждёт, пока контур
    закончит пересчёт, а его правка подхватывается следующим тактом.
    """

    async def pinned(self) -> Mapping[str, int]: ...

    async def paused(self) -> bool: ...

    async def pin(self, consumer_id: str, limit: int) -> None: ...

    async def unpin(self, consumer_id: str) -> None: ...

    async def set_paused(self, paused: bool) -> None: ...


@runtime_checkable
class Clock(Protocol):
    """Время берётся портом: такт обязан быть воспроизводимым в тестах."""

    def now(self) -> datetime: ...


@runtime_checkable
class TickObserver(Protocol):
    """Куда уходят метрики такта (G6): график загрузки пула для админки."""

    async def observe(self, report: object) -> None: ...


__all__ = [
    "AllocationPolicy",
    "CapacitySource",
    "Clock",
    "ConsumerRegistry",
    "OverrideStore",
    "Replenisher",
    "StateStore",
    "SupplySource",
    "TickObserver",
    "TickScope",
    "WindowSink",
]

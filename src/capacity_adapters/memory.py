"""Реализации портов в памяти: для примеров, тестов и прототипов.

Контур целиком собирается из них без базы и без внешних сервисов — так его поведение
можно посмотреть за секунды, прежде чем подключать Postgres и брокер. Это же делает
осмысленными тесты приложения: политику распределения можно проверить таблицей, не
поднимая инфраструктуру.

Для продакшна они не годятся по одной причине: состояние живёт в процессе. Лидер
контура меняется между тактами, и второй процесс не увидит ни дефицитов, ни пинов —
поэтому в бою берутся адаптеры `capacity_adapters.postgres`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from capacity.types import ConsumerState, EligibleCapacity, Hold, SupplySnapshot


@dataclass(slots=True)
class MemoryTickScope:
    """Транзакция такта без транзакции: лидером считается единственный процесс."""

    leading: bool = True
    ticks: int = 0

    @asynccontextmanager
    async def lead(self) -> AsyncIterator[bool]:
        self.ticks += 1
        yield self.leading


@dataclass(slots=True)
class MemoryCapacity:
    """Ёмкость пула, которой управляет сам пример или тест.

    `work_units` — сколько единиц работы покрывает остаток кредитов; `None` означает,
    что кредитов не считают и ширину работы ограничивает только число ресурсов.
    """

    eligible: int
    free: int | None = None
    leased: int = 0
    work_units: int | None = None
    credits_per_resource: int = 1

    async def capacity(self) -> EligibleCapacity:
        units = self.work_units
        return EligibleCapacity(
            eligible=self.eligible,
            free=self.eligible if self.free is None else self.free,
            leased=self.leased,
            effective=0.0 if units is None else units / self.credits_per_resource,
            work_units=units,
        )


@dataclass(slots=True)
class MemoryRegistry:
    """Кто просит работу. Меняется прямо по ходу прогона: спрос живой."""

    states: list[ConsumerState] = field(default_factory=list)

    async def consumers(self) -> Sequence[ConsumerState]:
        return list(self.states)

    def set_demand(self, consumer_id: str, demand: int) -> None:
        self.states = [
            (state if state.consumer_id != consumer_id else _with_demand(state, demand))
            for state in self.states
        ]


def _with_demand(state: ConsumerState, demand: int) -> ConsumerState:
    return ConsumerState(
        consumer_id=state.consumer_id,
        demand=demand,
        weight=state.weight,
        guaranteed=state.guaranteed,
        ceiling=state.ceiling,
    )


@dataclass(slots=True)
class MemoryWindows:
    """Окна и история их записей.

    `writes` — то, ради чего мёртвая зона и существует: по длине этого списка видно,
    сколько раз контур потревожил бы брокер и базу.
    """

    windows: dict[str, int] = field(default_factory=dict)
    writes: list[dict[str, int]] = field(default_factory=list)

    async def current_windows(self) -> Mapping[str, int]:
        return dict(self.windows)

    async def set_windows(self, windows: Mapping[str, int]) -> None:
        self.writes.append(dict(windows))
        self.windows.update(windows)


@dataclass(slots=True)
class MemoryState:
    """Дефициты и удержания между тактами."""

    deficits: dict[str, int] = field(default_factory=dict)
    holds: dict[str, Hold] = field(default_factory=dict)

    async def load(self) -> tuple[Mapping[str, int], Mapping[str, Hold]]:
        return dict(self.deficits), dict(self.holds)

    async def save(self, deficits: Mapping[str, int], holds: Mapping[str, Hold]) -> None:
        self.deficits = dict(deficits)
        self.holds = dict(holds)


@dataclass(slots=True)
class MemoryOverrides:
    """`pin` и `pause` оператора."""

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
class MemorySupply:
    """Снимок запаса для политики пополнения."""

    snapshot_value: SupplySnapshot

    async def snapshot(self) -> SupplySnapshot:
        return self.snapshot_value


@dataclass(slots=True)
class MemoryReplenisher:
    """Заказы пополнения: что заказали и под каким ключом окна."""

    orders: list[tuple[int, str]] = field(default_factory=list)

    async def order(self, count: int, window_key: str) -> None:
        self.orders.append((count, window_key))

    @property
    def total(self) -> int:
        return sum(count for count, _ in self.orders)


@dataclass(slots=True)
class MemoryClock:
    """Часы, которые двигает прогон: такт занимает столько, сколько вы скажете."""

    moment: datetime = datetime(2026, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        return self.moment

    def advance(self, seconds: float) -> None:
        self.moment += timedelta(seconds=seconds)


@dataclass(slots=True)
class MemoryObserver:
    """Метрики такта. В бою на их месте Prometheus или журнал приложения."""

    reports: list[object] = field(default_factory=list)

    async def observe(self, report: object) -> None:
        self.reports.append(report)

"""Адаптер к Identity Hub: ёмкость пула и снимок запаса (п.6 плана).

Identity Hub здесь не импортируется — внешнее API описано `Protocol` по его публичным
сигнатурам (`identities.count(where=…)`, `src/identity_hub/api/identities.py:369`).
Фильтры годности создаёт проект и передаёт готовыми: что считать годным аккаунтом —
решение домена, а не политики.

`eligible_count` (I1 плана) в Identity Hub ещё нет, но `count()` с фильтром закрывает
ту же потребность: один запрос с агрегатом вместо выборки. Разница в том, что
«годность» выражается фильтром снаружи, то есть проект обязан держать его в одном
месте с тем фильтром, по которому идёт аренда, — иначе контур будет считать не тех,
кого арендует.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from capacity.types import EligibleCapacity, SupplySnapshot


class Identities(Protocol):
    """`th`-подобный фасад Identity Hub: нужен только счёт по фильтру."""

    async def count(self, where: Any = None) -> int: ...


#: Сколько единиц работы покрывает остаток кредитов пула. Модель кредитов — доменная
#: вещь проекта (у одного сервиса это сообщения, у другого проверки), поэтому ядро её
#: не знает и получает готовое число.
WorkUnits = Callable[[], Awaitable[int]]


@dataclass(slots=True)
class IdentityHubCapacity:
    """`CapacitySource` поверх `identities.count()`.

    `eligible_filter` — годные для работы аккаунты (обычно `lifecycle=ready`,
    `standing=good`, `availability=available`), `free_filter` и `leased_filter` —
    необязательные уточнения для наблюдаемости (G1.3). Если их нет, `free` считается
    равным `eligible`: раздача всё равно идёт по `eligible` (окно — план на работу, а
    не захват ресурса сейчас), а в метриках будет видно, что занятость не измеряется.
    """

    identities: Identities
    eligible_filter: Any
    free_filter: Any | None = None
    leased_filter: Any | None = None
    work_units: WorkUnits | None = None
    credits_per_resource: int = 1

    async def capacity(self) -> EligibleCapacity:
        eligible = await self.identities.count(where=self.eligible_filter)
        free = (
            await self.identities.count(where=self.free_filter)
            if self.free_filter is not None
            else eligible
        )
        leased = (
            await self.identities.count(where=self.leased_filter)
            if self.leased_filter is not None
            else 0
        )
        units = await self.work_units() if self.work_units else None
        effective = units / self.credits_per_resource if units is not None else 0.0
        return EligibleCapacity(
            eligible=eligible,
            free=free,
            leased=leased,
            effective=effective,
            work_units=units,
        )


@dataclass(slots=True)
class PoolSupply:
    """`SupplySource`: собирает снимок запаса из ёмкости, спроса и журнала заказов.

    Величины здесь дробные (G1.1): пополнение считает запас, а не параллелизм.
    `needed_for_demand` переводит спрос в ресурсы — сколько аккаунтов нужно, чтобы
    покрыть уже поставленную работу.
    """

    capacity_source: IdentityHubCapacity
    demand_units: Callable[[], Awaitable[int]]
    order_counts: Callable[[], Awaitable[tuple[int, int]]]
    credits_per_resource: int = 1

    async def snapshot(self) -> SupplySnapshot:
        capacity = await self.capacity_source.capacity()
        demand = await self.demand_units()
        in_window, in_progress = await self.order_counts()
        return SupplySnapshot(
            effective_available=capacity.effective,
            needed_for_demand=demand / self.credits_per_resource,
            ordered_in_progress=in_progress,
            ordered_in_window=in_window,
        )

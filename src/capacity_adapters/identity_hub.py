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

from capacity.ports import CapacitySource
from capacity.types import EligibleCapacity, SupplySnapshot


class Identities(Protocol):
    """Фасад Identity Hub: нужен только счёт по фильтру."""

    async def count(self, where: Any = None) -> int: ...


class PoolCapacity(Protocol):
    """Результат `pool.eligible_count()`: ёмкость в разрезе занятости и резерва."""

    @property
    def eligible(self) -> int: ...

    @property
    def free(self) -> int: ...

    @property
    def leased(self) -> int: ...

    @property
    def reserved(self) -> int: ...


class IdentityHubPool(Protocol):
    """Пул Identity Hub: тот же запрос кандидатов, что и у `pool.lease()`."""

    async def eligible_count(
        self,
        *,
        for_operation: str | None = ...,
        among: Any = ...,
        require_quota: bool = ...,
        limit: int = ...,
    ) -> PoolCapacity: ...


#: Сколько единиц работы покрывает остаток кредитов пула. Модель кредитов — доменная
#: вещь проекта (у одного сервиса это сообщения, у другого проверки), поэтому ядро её
#: не знает и получает готовое число.
WorkUnits = Callable[[], Awaitable[int]]


@dataclass(slots=True)
class IdentityHubPoolCapacity:
    """`CapacitySource` поверх `pool.eligible_count()`.

    Считает ровно те identity, которые выдаст `pool.lease(for_operation=…, among=…)`:
    тот же запрос кандидатов, те же квоты, тот же резерв. Поэтому `among` обязан быть
    тем же самым — лучше всего одной функцией, которая строит его и для аренды, и для
    ёмкости. Разойдясь, они дают худший вид ошибки: контур раздаёт окна под аккаунты,
    которых аренда не выдаст.

    `limit` в Identity Hub ограничивает каждый внутренний счётчик: результат, равный
    ему, означает «насыщено», а не точное число. Он должен быть не меньше суммарного
    потолка потребителей, иначе ёмкость окажется занижена.

    `work_units` — сколько единиц работы покрывают кредиты, если расход ресурса их
    требует; `None` означает, что кредитов не считают.
    """

    pool: IdentityHubPool
    among: Any = None
    for_operation: str | None = None
    require_quota: bool = False
    limit: int = 1_000_000
    work_units: WorkUnits | None = None
    credits_per_resource: int = 1

    async def capacity(self) -> EligibleCapacity:
        hub = await self.pool.eligible_count(
            for_operation=self.for_operation,
            among=self.among,
            require_quota=self.require_quota,
            limit=self.limit,
        )
        units = await self.work_units() if self.work_units else None
        return EligibleCapacity(
            eligible=hub.eligible,
            free=hub.free,
            leased=hub.leased,
            reserved=hub.reserved,
            effective=0.0 if units is None else units / self.credits_per_resource,
            work_units=units,
        )


@dataclass(slots=True)
class IdentityFilterCapacity:
    """`CapacitySource` поверх `identities.count(where=…)`.

    Годится, когда годность ресурса целиком выражается полями Identity Hub:
    `lifecycle`, `standing`, `auth`, `availability`, теги и точные значения меток.
    Подзапрос к таблице проекта, сравнение по диапазону, квоты и резерв пула этот
    фильтр выразить не может — для них берите `IdentityHubPoolCapacity`, который
    спрашивает у самого пула.

    `eligible_filter` — годные для работы аккаунты, `free_filter` и `leased_filter` —
    необязательные уточнения для наблюдаемости. Если их нет, `free` считается равным
    `eligible`: раздача всё равно идёт по `eligible`, а в метриках будет видно, что
    занятость не измеряется.
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

    capacity_source: CapacitySource
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

"""Адаптер к Identity Hub: ёмкость пула и снимок запаса (п.6 плана)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from capacity.ports import CapacitySource, SupplySource
from capacity_adapters.identity_hub import IdentityHubCapacity, PoolSupply

ELIGIBLE = {"lifecycle": "ready", "standing": "good", "availability": "available"}
FREE = {**ELIGIBLE, "without_link": "lease"}
LEASED = {"is_target_of": ("lease", None)}


@dataclass(slots=True)
class FakeIdentities:
    """`identities.count(where=…)` — единственное, что нужно от Identity Hub."""

    counts: dict[str, int] = field(default_factory=dict)
    asked: list[Any] = field(default_factory=list)

    async def count(self, where: Any = None) -> int:
        self.asked.append(where)
        return self.counts.get(_key(where), 0)


def _key(where: Any) -> str:
    return repr(sorted(where.items())) if isinstance(where, dict) else repr(where)


def build(**counts: int) -> FakeIdentities:
    mapping = {
        "eligible": _key(ELIGIBLE),
        "free": _key(FREE),
        "leased": _key(LEASED),
    }
    return FakeIdentities({mapping[name]: value for name, value in counts.items()})


async def test_eligible_count_comes_from_the_domain_filter() -> None:
    """Что считать годным аккаунтом — решение домена, фильтр приходит снаружи."""
    identities = build(eligible=17)
    source = IdentityHubCapacity(identities, eligible_filter=ELIGIBLE)

    capacity = await source.capacity()

    assert capacity.eligible == 17
    assert identities.asked == [ELIGIBLE]


async def test_allocatable_follows_eligible_not_free() -> None:
    """Окно — план на работу: занятые аккаунты из раздачи не выпадают (см. S11)."""
    source = IdentityHubCapacity(
        build(eligible=10, free=2, leased=8),
        eligible_filter=ELIGIBLE,
        free_filter=FREE,
        leased_filter=LEASED,
    )

    capacity = await source.capacity()

    assert (capacity.free, capacity.leased) == (2, 8)
    assert capacity.allocatable == 10


async def test_observability_filters_are_optional() -> None:
    """Без уточняющих фильтров занятость не измеряется, и это видно в метриках."""
    identities = build(eligible=5)
    source = IdentityHubCapacity(identities, eligible_filter=ELIGIBLE)

    capacity = await source.capacity()

    assert (capacity.free, capacity.leased) == (5, 0)
    assert len(identities.asked) == 1


async def test_credits_bind_the_tail_of_the_work() -> None:
    """Кредитов осталось на две единицы работы при десяти аккаунтах — окон два."""

    async def work_units() -> int:
        return 2

    source = IdentityHubCapacity(
        build(eligible=10), eligible_filter=ELIGIBLE, work_units=work_units
    )

    capacity = await source.capacity()

    assert capacity.allocatable == 2


async def test_effective_capacity_is_fractional_for_replenishment() -> None:
    """Запас дробный (G1.1): 950 единиц работы по 100 на аккаунт — 9.5 аккаунта."""

    async def work_units() -> int:
        return 950

    source = IdentityHubCapacity(
        build(eligible=10),
        eligible_filter=ELIGIBLE,
        work_units=work_units,
        credits_per_resource=100,
    )

    capacity = await source.capacity()

    assert capacity.effective == 9.5
    assert capacity.allocatable == 10


def test_capacity_adapter_satisfies_the_port() -> None:
    assert isinstance(
        IdentityHubCapacity(build(), eligible_filter=ELIGIBLE), CapacitySource
    )


# --- снимок запаса ------------------------------------------------------------------


async def test_supply_snapshot_collects_stock_demand_and_orders() -> None:
    async def work_units() -> int:
        return 800

    async def demand_units() -> int:
        return 500

    async def order_counts() -> tuple[int, int]:
        return 20, 12

    supply = PoolSupply(
        capacity_source=IdentityHubCapacity(
            build(eligible=8),
            eligible_filter=ELIGIBLE,
            work_units=work_units,
            credits_per_resource=100,
        ),
        demand_units=demand_units,
        order_counts=order_counts,
        credits_per_resource=100,
    )

    snapshot = await supply.snapshot()

    assert snapshot.effective_available == 8.0
    assert snapshot.needed_for_demand == 5.0
    assert (snapshot.ordered_in_window, snapshot.ordered_in_progress) == (20, 12)
    # Инвентарная позиция — запас плюс заказанное (G4).
    assert snapshot.position == 20.0


def test_supply_adapter_satisfies_the_port() -> None:
    async def units() -> int:
        return 0

    async def counts() -> tuple[int, int]:
        return 0, 0

    supply = PoolSupply(
        capacity_source=IdentityHubCapacity(build(), eligible_filter=ELIGIBLE),
        demand_units=units,
        order_counts=counts,
    )

    assert isinstance(supply, SupplySource)

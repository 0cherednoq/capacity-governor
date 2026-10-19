"""Сборка целиком: контур из настоящих адаптеров поверх поддельных сервисов.

Это и проверка того, что порты стыкуются без переходников, и единственный пример
того, как контур собирается в проекте: всё, что ниже, кроме подделок Postgres,
Tallyho и Identity Hub, — рабочий код.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from capacity import Governor, Hold, MaxMinFair, ReorderPoint, Stabilizer, TickStatus
from capacity_adapters.identity_hub import IdentityHubCapacity, PoolSupply
from capacity_adapters.postgres import (
    PostgresOverrideStore,
    PostgresStateStore,
    PostgresTickScope,
)
from capacity_adapters.tallyho import (
    TallyhoConsumers,
    TallyhoOrderLedger,
    TallyhoReplenisher,
    TallyhoWindows,
)
from tests.test_adapters_identity_hub import ELIGIBLE, FakeIdentities, _key
from tests.test_adapters_postgres import FakeResult, FakeSession, FakeSessionScope, Row
from tests.test_adapters_tallyho import FakeProgress, FakeTallyho

NOW = datetime(2026, 10, 19, 15, 0, 0, tzinfo=UTC)
TICK = 5


class FixedClock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment

    def advance(self, seconds: int) -> None:
        self.moment += timedelta(seconds=seconds)


class DictJournal:
    """Журнал окон. В проекте это `PostgresStateStore.load_windows/save_windows`."""

    def __init__(self) -> None:
        self.windows: dict[str, int] = {}

    async def load(self) -> Mapping[str, int]:
        return dict(self.windows)

    async def save(self, windows: Mapping[str, int]) -> None:
        self.windows = dict(windows)


class DictState:
    """Состояние политики. В проекте это `PostgresStateStore`."""

    def __init__(self) -> None:
        self.deficits: Mapping[str, int] = {}
        self.holds: Mapping[str, Hold] = {}

    async def load(self) -> tuple[Mapping[str, int], Mapping[str, Hold]]:
        return dict(self.deficits), dict(self.holds)

    async def save(self, deficits: Mapping[str, int], holds: Mapping[str, Hold]) -> None:
        self.deficits, self.holds = dict(deficits), dict(holds)


class NoOverrides:
    """Переопределений нет. В проекте это `PostgresOverrideStore`."""

    async def pinned(self) -> Mapping[str, int]:
        return {}

    async def paused(self) -> bool:
        return False

    async def pin(self, consumer_id: str, limit: int) -> None:
        raise NotImplementedError

    async def unpin(self, consumer_id: str) -> None:
        raise NotImplementedError

    async def set_paused(self, paused: bool) -> None:
        raise NotImplementedError


async def register_account(window_key: str, slot: int) -> None:
    """Задача регистрации ресурса: её пишет проект."""


class PassthroughScope:
    """Транзакция такта. В проекте это `PostgresTickScope` с advisory-локом."""

    def __init__(self, *, leading: bool = True) -> None:
        self.leading = leading
        self.ticks = 0

    @asynccontextmanager
    async def lead(self) -> AsyncIterator[bool]:
        self.ticks += 1
        yield self.leading


def wire(
    *,
    eligible: int,
    demand: int,
    credits_units: int,
    leading: bool = True,
) -> tuple[Governor, dict[str, object]]:
    batch = uuid4()
    tallyho = FakeTallyho({batch: FakeProgress(found=demand, queued=demand)})
    identities = FakeIdentities({_key(ELIGIBLE): eligible})
    clock = FixedClock(NOW)
    journal = DictJournal()

    async def batches() -> Mapping[str, UUID]:
        return {"domains": batch}

    async def work_units() -> int:
        return credits_units

    async def demand_units() -> int:
        return demand

    capacity_source = IdentityHubCapacity(
        identities,
        eligible_filter=ELIGIBLE,
        work_units=work_units,
        credits_per_resource=100,
    )
    ledger = TallyhoOrderLedger(
        tallyho, kinds=["registration"], window=timedelta(hours=1), now=clock.now
    )
    governor = Governor(
        scope=PassthroughScope(leading=leading),
        capacity_source=capacity_source,
        registry=TallyhoConsumers(tallyho, batches),
        windows=TallyhoWindows(tallyho, lambda cid: batch, journal),
        state=DictState(),
        policy=MaxMinFair(min_hold_ticks=5),
        clock=clock,
        stabilizer=Stabilizer(),
        overrides=NoOverrides(),
        supply=PoolSupply(
            capacity_source=capacity_source,
            demand_units=demand_units,
            order_counts=ledger.counts,
            credits_per_resource=100,
        ),
        reorder=ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=20),
        replenisher=TallyhoReplenisher(
            tallyho, kind="registration", register=register_account
        ),
        min_tick_interval=timedelta(seconds=TICK),
    )
    return governor, {
        "tallyho": tallyho,
        "journal": journal,
        "clock": clock,
        "batch": batch,
    }


async def test_tick_reaches_tallyho_and_identity_hub() -> None:
    """Ёмкость читается у Identity Hub, спрос и окно — у Tallyho."""
    governor, parts = wire(eligible=4, demand=100, credits_units=400)

    outcome = await governor.tick()

    assert outcome.status is TickStatus.RAN
    tallyho = parts["tallyho"]
    assert [limit for _, limit, _, _ in tallyho.applied] == [4]  # type: ignore[attr-defined]
    assert parts["journal"].windows == {"domains": 4}  # type: ignore[attr-defined]


async def test_window_is_not_rewritten_when_nothing_changed() -> None:
    """Второй такт при той же ёмкости не пишет в Tallyho вовсе (G5, G6)."""
    governor, parts = wire(eligible=4, demand=100, credits_units=400)

    await governor.tick()
    parts["clock"].advance(TICK)  # type: ignore[attr-defined]
    await governor.tick()

    assert len(parts["tallyho"].applied) == 1  # type: ignore[attr-defined]


async def test_empty_pool_closes_the_window_and_orders_replenishment() -> None:
    """Ресурса нет: окно 0 немедленно, заказ — после коммита такта (G5, G6)."""
    governor, parts = wire(eligible=0, demand=100, credits_units=0)

    await governor.tick()

    tallyho = parts["tallyho"]
    assert [limit for _, limit, _, _ in tallyho.applied] == [0]  # type: ignore[attr-defined]
    # Заказ — настоящий батч регистраций с ключом бюджетного окна и слотами (§8.3).
    assert tallyho.batches == [  # type: ignore[attr-defined]
        ("registration", "replenish:2026-10-19T15:00:00+00:00", False, None)
    ]
    assert tallyho.added[:3] == ["slot:0", "slot:1", "slot:2"]  # type: ignore[attr-defined]


async def test_nothing_happens_without_leadership() -> None:
    governor, parts = wire(eligible=4, demand=100, credits_units=400, leading=False)

    outcome = await governor.tick()

    assert outcome.status is TickStatus.NOT_LEADER
    assert parts["tallyho"].applied == []  # type: ignore[attr-defined]


async def test_postgres_pieces_fit_the_same_loop() -> None:
    """Те же порты, но состояние и лидерство — из Postgres-адаптеров."""
    session = FakeSession(
        results=[
            FakeResult(rows=[Row(name="deficits", payload={"domains": 1})]),
            FakeResult(),
            FakeResult(),
        ]
    )
    scope = PostgresTickScope(session_scope=FakeSessionScope(session), scope="domains")
    state = PostgresStateStore(scope, scope="domains")
    overrides = PostgresOverrideStore(scope, scope="domains")

    async with scope.lead() as leading:
        assert leading is True
        deficits, holds = await state.load()
        pins = await overrides.pinned()

    assert deficits == {"domains": 1}
    assert (holds, pins) == ({}, {})

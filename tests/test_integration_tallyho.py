"""Сценарий 1 гейта (п.9 плана): проверка доменов на `MaxMinFair`, живьём.

Здесь нет ни одной подделки со стороны Tallyho и Postgres: настоящая база, настоящие
батчи, настоящие окна. Единственное, что осталось смоделированным, — источник ёмкости:
Identity Hub в этом прогоне не участвует, ёмкость задаётся числом, как её отдал бы
`IdentityHubCapacity` (он проверен отдельно).

Что проверяется: контур действительно управляет работой Tallyho — закрытое окно
останавливает проверку доменов, открытое пускает её, а когда ёмкости меньше, чем
батчей, окно ходит между ними по дефицитам и никто не стоит вечно.

Пропускается без базы. Поднять:

    docker run -d --name capacity-governor-test-pg \\
        -e POSTGRES_PASSWORD=capacity -e POSTGRES_USER=capacity \\
        -e POSTGRES_DB=capacity -p 55444:5432 postgres:16-alpine
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from capacity import (
    EligibleCapacity,
    Governor,
    MaxMinFair,
    Stabilizer,
)
from capacity_adapters.postgres import (
    GOVERNOR_OVERRIDE,
    GOVERNOR_STATE,
    METADATA,
    PostgresStateStore,
    PostgresTickScope,
)
from capacity_adapters.tallyho import (
    TallyhoConsumers,
    TallyhoOrderLedger,
    TallyhoReplenisher,
    TallyhoWindows,
)

DSN = os.environ.get(
    "CAPACITY_TEST_DSN",
    "postgresql+asyncpg://capacity:capacity@127.0.0.1:55444/capacity",
)
SCHEMA = "capacity_it"

tallyho = pytest.importorskip("tallyho", reason="Tallyho не установлен")
testing = pytest.importorskip("tallyho.testing", reason="Tallyho не установлен")

checked: list[str] = []


async def check_domain(domain: str) -> None:
    """Единица работы: «проверка домена». Содержание неважно, важен факт расхода."""
    checked.append(domain)
    tallyho.item.ok("checked")


@dataclass(slots=True)
class FixedCapacity:
    """Ёмкость пула. В бою это `IdentityHubCapacity` поверх `identities.count()`."""

    value: int

    async def capacity(self) -> EligibleCapacity:
        return EligibleCapacity(eligible=self.value, free=self.value)


class Clock:
    def __init__(self) -> None:
        self.moment = datetime(2026, 10, 19, 16, 0, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.moment

    def tick(self, seconds: int = 5) -> None:
        self.moment += timedelta(seconds=seconds)


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    live = create_async_engine(DSN)
    try:
        async with live.begin() as connection:
            await connection.run_sync(METADATA.create_all)
    except Exception:
        await live.dispose()
        pytest.skip(f"нет Postgres по {DSN}")
    try:
        yield live
    finally:
        await live.dispose()


@pytest.fixture
async def th(engine: AsyncEngine) -> AsyncIterator[tuple[Any, Any]]:
    # Схема Tallyho пересоздаётся, состояние контура чистится: иначе батчи прошлого
    # прогона остаются в базе с открытыми окнами и `drain()` выполняет чужую работу,
    # а журнал окон помнит выставленное тогда и решает, что менять нечего.
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        await connection.execute(GOVERNOR_STATE.delete())
        await connection.execute(GOVERNOR_OVERRIDE.delete())
    broker = testing.InlineBroker()
    # `relay_grace=0`: запаркованная работа выпускается сразу после расширения окна.
    # В бою пять секунд задержки незаметны, в тесте они означали бы sleep на каждый такт.
    client = tallyho.Tallyho(engine, schema=SCHEMA, relay_grace=timedelta(0))
    client.install(broker.adapter)
    await client.migrate()
    checked.clear()
    try:
        yield client, broker
    finally:
        await client.aclose()


async def make_batch(client: Any, kind: str, domains: list[str]) -> UUID:
    """Батч проверки доменов, созданный с закрытым окном.

    Окно задаётся при создании, а не после: до коммита батча его ещё нет в базе, и
    `set_max_in_flight` на незакоммиченном батче падает с `BatchPurged`. Для контура
    это и есть правильный порядок — работа ставится закрытой, окно открывает он.
    """
    async with client.batch(kind, max_in_flight=0) as batch:
        await batch.map(check_domain, domains)
    return batch.handle.id  # type: ignore[no-any-return]


def build_governor(
    engine: AsyncEngine,
    client: Any,
    batches: Mapping[str, UUID],
    capacity: int,
    clock: Clock,
    scope_name: str,
) -> tuple[Governor, PostgresStateStore]:
    maker = async_sessionmaker(engine, expire_on_commit=False)
    scope = PostgresTickScope(session_scope=maker.begin, scope=scope_name)
    state = PostgresStateStore(scope, scope=scope_name)

    async def batch_map() -> Mapping[str, UUID]:
        return dict(batches)

    governor = Governor(
        scope=scope,
        capacity_source=FixedCapacity(capacity),
        registry=TallyhoConsumers(client, batch_map),
        windows=TallyhoWindows(
            client,
            resolve=lambda consumer_id: batches[consumer_id],
            journal=_JournalOverState(state),
            session_provider=scope.session,
        ),
        state=state,
        policy=MaxMinFair(),
        clock=clock,
        stabilizer=Stabilizer(),
        min_tick_interval=timedelta(seconds=5),
    )
    return governor, state


@dataclass(slots=True)
class _JournalOverState:
    """Журнал окон поверх таблицы контура: у Tallyho текущее окно не прочитать."""

    state: PostgresStateStore

    async def load(self) -> Mapping[str, int]:
        return await self.state.load_windows()

    async def save(self, windows: Mapping[str, int]) -> None:
        await self.state.save_windows(windows)


# --- сценарий -----------------------------------------------------------------------


async def test_closed_window_stops_domain_checking(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Ёмкости нет — работа не идёт, хотя задачи поставлены и брокер работает."""
    client, broker = th
    batch = await make_batch(client, "domains-empty", [f"d{i}.test" for i in range(5)])
    governor, _ = build_governor(
        engine, client, {"run": batch}, capacity=0, clock=Clock(), scope_name="it-empty"
    )

    await governor.tick()
    await broker.drain()

    assert checked == []


async def test_capacity_opens_the_window_and_work_goes_through(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Ёмкость появилась — контур открыл окно, и проверка доменов пошла."""
    client, broker = th
    domains = [f"d{i}.test" for i in range(5)]
    batch = await make_batch(client, "domains-open", domains)
    governor, _ = build_governor(
        engine, client, {"run": batch}, capacity=3, clock=Clock(), scope_name="it-open"
    )

    outcome = await governor.tick()
    await broker.drain()

    assert outcome.plan is not None
    assert outcome.plan.allocation.grants == {"run": 3}
    assert sorted(checked) == sorted(domains)


async def test_demand_is_read_from_live_progress(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Спрос берётся из настоящих счётчиков батча, а не из нашего представления о них."""
    client, broker = th
    batch = await make_batch(client, "domains-demand", [f"d{i}.test" for i in range(4)])
    governor, _ = build_governor(
        engine, client, {"run": batch}, capacity=2, clock=Clock(), scope_name="it-demand"
    )

    first = await governor.tick()
    await broker.drain()
    clock_later = Clock()
    clock_later.tick()
    governor.clock = clock_later
    second = await governor.tick()

    assert first.plan is not None and second.plan is not None
    assert first.plan.report.total_demand == 4
    # Работа сделана — спроса больше нет, и окно под него не выдаётся.
    assert second.plan.report.total_demand == 0
    assert second.plan.allocation.grants == {"run": 0}


async def test_scarce_capacity_rotates_between_batches(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Ёмкость 1 на два батча: окно ходит между ними по дефицитам, никто не забыт."""
    client, _ = th
    first = await make_batch(client, "domains-a", [f"a{i}.test" for i in range(10)])
    second = await make_batch(client, "domains-b", [f"b{i}.test" for i in range(10)])
    clock = Clock()
    governor, _ = build_governor(
        engine,
        client,
        {"a": first, "b": second},
        capacity=1,
        clock=clock,
        scope_name="it-rotate",
    )

    grants = []
    for _ in range(4):
        outcome = await governor.tick()
        assert outcome.plan is not None
        grants.append(outcome.plan.allocation.grants)
        clock.tick()

    assert all(sum(g.values()) == 1 for g in grants)
    assert sum(g["a"] for g in grants) == 2
    assert sum(g["b"] for g in grants) == 2


async def test_state_is_shared_between_loop_restarts(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Контур перезапущен — очередь не сбрасывается: она лежит в базе, а не в процессе."""
    client, _ = th
    first = await make_batch(client, "domains-x", [f"x{i}.test" for i in range(10)])
    second = await make_batch(client, "domains-y", [f"y{i}.test" for i in range(10)])
    batches = {"x": first, "y": second}
    clock = Clock()

    governor, _ = build_governor(
        engine, client, batches, capacity=1, clock=clock, scope_name="it-restart"
    )
    first_grants = (await governor.tick()).plan
    clock.tick()

    # Новый процесс: свой Governor, своя память, та же база.
    restarted, _ = build_governor(
        engine, client, batches, capacity=1, clock=clock, scope_name="it-restart"
    )
    second_grants = (await restarted.tick()).plan

    assert first_grants is not None and second_grants is not None
    served_first = {cid for cid, g in first_grants.allocation.grants.items() if g}
    served_second = {cid for cid, g in second_grants.allocation.grants.items() if g}
    assert served_first != served_second


# --- пополнение ---------------------------------------------------------------------

registered: list[tuple[str, int]] = []


async def register_account(window_key: str, slot: int) -> None:
    """Регистрация одного ресурса. В бою это работа библиотеки авторизации."""
    registered.append((window_key, slot))
    tallyho.item.ok("registered")


async def test_replenishment_order_is_a_real_batch_of_slots(
    th: tuple[Any, Any],
) -> None:
    """Заказ ставится батчем со слотами и исполняется (G4, §8.3)."""
    client, broker = th
    registered.clear()
    replenisher = TallyhoReplenisher(
        client, kind="registration", register=register_account
    )

    await replenisher.order(3, "replenish:2026-10-19T16:00:00+00:00")
    await broker.drain()

    assert sorted(slot for _, slot in registered) == [0, 1, 2]


async def test_repeated_order_in_the_same_window_adds_only_what_is_missing(
    th: tuple[Any, Any],
) -> None:
    """Повторный такт того же окна не дублирует слоты и не теряет новые.

    Это главное свойство схемы §8.3, и проверить его можно только живьём: ключ батча
    по бюджетному окну плюс уникальный индекс `(batch_id, key)` на слотах.
    """
    client, broker = th
    registered.clear()
    window = "replenish:2026-10-19T17:00:00+00:00"
    replenisher = TallyhoReplenisher(
        client, kind="registration", register=register_account
    )

    await replenisher.order(2, window)
    await replenisher.order(2, window)
    await broker.drain()

    assert sorted(slot for _, slot in registered) == [0, 1, 2, 3]
    assert {key for key, _ in registered} == {window}


async def test_order_in_a_new_window_starts_its_own_batch(
    th: tuple[Any, Any],
) -> None:
    """Новое бюджетное окно — новый батч: бюджет считается по тому же окну, что ключ."""
    client, broker = th
    registered.clear()
    replenisher = TallyhoReplenisher(
        client, kind="registration", register=register_account
    )

    await replenisher.order(1, "replenish:2026-10-19T18:00:00+00:00")
    await replenisher.order(1, "replenish:2026-10-19T19:00:00+00:00")
    await broker.drain()

    assert sorted(registered) == [
        ("replenish:2026-10-19T18:00:00+00:00", 0),
        ("replenish:2026-10-19T19:00:00+00:00", 0),
    ]


async def test_ledger_counts_the_orders_it_placed(th: tuple[Any, Any]) -> None:
    """Журнал бюджета читает собственные батчи: заказ виден политике следующим тактом."""
    client, broker = th
    registered.clear()
    clock = Clock()
    replenisher = TallyhoReplenisher(
        client, kind="registration", register=register_account
    )
    ledger = TallyhoOrderLedger(
        client,
        kinds=["registration"],
        window=timedelta(hours=1),
        now=clock.now,
        cache_for=timedelta(0),
    )

    await replenisher.order(4, "replenish:2026-10-19T16:00:00+00:00")
    in_window, in_progress = await ledger.counts()
    await broker.drain()
    after_window, after_progress = await ledger.counts()

    assert (in_window, in_progress) == (4, 4)
    # Работа сделана: бюджет окна израсходован, в работе ничего не осталось.
    assert (after_window, after_progress) == (4, 0)

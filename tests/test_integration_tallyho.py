"""Проверка доменов на `MaxMinFair` против живых Postgres и Tallyho.

Здесь нет ни одной подделки со стороны Tallyho и базы: настоящие батчи, настоящий
`th.demand()`, настоящие окна. Смоделирован только источник ёмкости — Identity Hub в
этом прогоне не участвует и проверяется отдельно.

Что проверяется: контур действительно управляет работой Tallyho — закрытое окно
останавливает проверку доменов, открытое пускает её, спрос и окна приходят одним
снимком, а когда ёмкости меньше, чем батчей, окно ходит между ними по дефицитам.

Пропускается без базы. Поднять:

    docker run -d --name capacity-governor-test-pg \\
        -e POSTGRES_PASSWORD=capacity -e POSTGRES_USER=capacity \\
        -e POSTGRES_DB=capacity -p 55444:5432 postgres:16-alpine
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from capacity import EligibleCapacity, Governor, MaxMinFair, Stabilizer
from capacity_adapters.postgres import (
    GOVERNOR_OVERRIDE,
    GOVERNOR_STATE,
    METADATA,
    PostgresStateStore,
    PostgresTickScope,
)
from capacity_adapters.tallyho import (
    TallyhoDemand,
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

from tallyho import Ok  # noqa: E402 - после importorskip

checked: list[str] = []
registered: list[tuple[str, int]] = []


async def check_domain(domain: str) -> Ok:
    """Единица работы: «проверка домена». Содержание неважно, важен факт расхода."""
    checked.append(domain)
    return Ok("checked")


async def register_account(window_key: str, slot: int) -> Ok:
    """Регистрация одного ресурса. В бою это работа библиотеки авторизации."""
    registered.append((window_key, slot))
    return Ok("registered")


@dataclass(slots=True)
class FixedCapacity:
    """Ёмкость пула. В бою это `IdentityHubPoolCapacity` поверх `eligible_count()`."""

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
    # прогона остаются в базе с открытыми окнами и попадают в `demand()` этого.
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        await connection.execute(GOVERNOR_STATE.delete())
        await connection.execute(GOVERNOR_OVERRIDE.delete())
    broker = testing.InlineBroker()
    # `relay_grace=0`: запаркованная работа выпускается сразу после расширения окна.
    client = tallyho.Tallyho(engine, schema=SCHEMA, relay_grace=timedelta(0))
    client.install(broker.adapter)
    await client.migrate()
    checked.clear()
    registered.clear()
    try:
        yield client, broker
    finally:
        await client.aclose()


async def make_batch(client: Any, kind: str, domains: list[str]) -> UUID:
    """Батч проверки доменов, созданный с закрытым окном.

    Окно задаётся при создании: до коммита батча его ещё нет в базе, и
    `set_max_in_flight` на незакоммиченном батче падает с `BatchPurged`. Для контура
    это и есть правильный порядок — работа ставится закрытой, окно открывает он.
    """
    async with client.batch(kind, max_in_flight=0) as batch:
        await batch.map(check_domain, domains)
    return batch.handle.id  # type: ignore[no-any-return]


def build_governor(
    engine: AsyncEngine, client: Any, kind: str, capacity: int, clock: Clock
) -> Governor:
    """Контур над батчами одного вида: спрос и окна приходят одним `demand()`."""
    maker = async_sessionmaker(engine, expire_on_commit=False)
    scope = PostgresTickScope(session_scope=maker.begin, scope=f"it-{kind}")
    demand = TallyhoDemand(client, kinds=[kind])
    return Governor(
        scope=scope,
        capacity_source=FixedCapacity(capacity),
        registry=demand,
        windows=TallyhoWindows(client, demand, session_provider=scope.session),
        state=PostgresStateStore(scope),
        policy=MaxMinFair(),
        clock=clock,
        stabilizer=Stabilizer(),
        min_tick_interval=timedelta(seconds=5),
    )


# --- сценарий -----------------------------------------------------------------------


async def test_closed_window_stops_domain_checking(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Ёмкости нет — работа не идёт, хотя задачи поставлены и брокер работает."""
    client, broker = th
    kind = f"domains-{uuid4().hex[:8]}"
    await make_batch(client, kind, [f"d{i}.test" for i in range(5)])
    governor = build_governor(engine, client, kind, capacity=0, clock=Clock())

    await governor.tick()
    await broker.drain()

    assert checked == []


async def test_capacity_opens_the_window_and_work_goes_through(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Ёмкость появилась — контур открыл окно, и проверка доменов пошла."""
    client, broker = th
    kind = f"domains-{uuid4().hex[:8]}"
    domains = [f"d{i}.test" for i in range(5)]
    batch = await make_batch(client, kind, domains)
    governor = build_governor(engine, client, kind, capacity=3, clock=Clock())

    outcome = await governor.tick()
    await broker.drain()

    assert outcome.plan is not None
    assert outcome.plan.allocation.grants == {str(batch): 3}
    assert sorted(checked) == sorted(domains)


async def test_demand_and_windows_come_from_one_snapshot(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Спрос и текущее окно читаются одним запросом и описывают один момент."""
    client, broker = th
    kind = f"domains-{uuid4().hex[:8]}"
    batch = await make_batch(client, kind, [f"d{i}.test" for i in range(4)])
    clock = Clock()
    governor = build_governor(engine, client, kind, capacity=2, clock=clock)

    first = await governor.tick()
    await broker.drain()
    clock.tick()
    second = await governor.tick()

    assert first.plan is not None and second.plan is not None
    assert first.plan.allocation.grants == {str(batch): 2}
    assert first.plan.report.total_demand == 4
    # Работа сделана, батч финализирован — он больше не активен и из `demand()`
    # выпадает совсем. Контур о нём не знает и окон ему не выдаёт.
    assert second.plan.report.total_demand == 0
    assert second.plan.allocation.grants == {}


async def test_window_set_by_the_loop_is_read_back_next_tick(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Окно читается у Tallyho, а не из журнала контура: журнала больше нет."""
    client, _ = th
    kind = f"domains-{uuid4().hex[:8]}"
    batch = await make_batch(client, kind, [f"d{i}.test" for i in range(20)])
    clock = Clock()
    governor = build_governor(engine, client, kind, capacity=4, clock=clock)

    await governor.tick()
    # Проект сам сузил окно мимо контура — контур обязан это увидеть.
    await client.handle(batch).set_max_in_flight(1)
    clock.tick()
    second = await governor.tick()

    assert second.plan is not None
    assert second.plan.report.window_deltas == {str(batch): 3}


async def test_scarce_capacity_rotates_between_batches(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Ёмкость 1 на два батча: окно ходит между ними по дефицитам, никто не забыт."""
    client, _ = th
    kind = f"domains-{uuid4().hex[:8]}"
    first = await make_batch(client, kind, [f"a{i}.test" for i in range(10)])
    second = await make_batch(client, kind, [f"b{i}.test" for i in range(10)])
    clock = Clock()
    governor = build_governor(engine, client, kind, capacity=1, clock=clock)

    grants = []
    for _ in range(4):
        outcome = await governor.tick()
        assert outcome.plan is not None
        grants.append(outcome.plan.allocation.grants)
        clock.tick()

    assert all(sum(g.values()) == 1 for g in grants)
    assert sum(g[str(first)] for g in grants) == 2
    assert sum(g[str(second)] for g in grants) == 2


async def test_state_survives_a_loop_restart(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Контур перезапущен — очередь не сбрасывается: она лежит в базе."""
    client, _ = th
    kind = f"domains-{uuid4().hex[:8]}"
    await make_batch(client, kind, [f"x{i}.test" for i in range(10)])
    await make_batch(client, kind, [f"y{i}.test" for i in range(10)])
    clock = Clock()

    governor = build_governor(engine, client, kind, capacity=1, clock=clock)
    first = (await governor.tick()).plan
    clock.tick()

    # Новый процесс: свой Governor, своя память, та же база.
    restarted = build_governor(engine, client, kind, capacity=1, clock=clock)
    second = (await restarted.tick()).plan

    assert first is not None and second is not None
    served_first = {cid for cid, g in first.allocation.grants.items() if g}
    served_second = {cid for cid, g in second.allocation.grants.items() if g}
    assert served_first != served_second


# --- пополнение ---------------------------------------------------------------------


async def test_replenishment_order_is_a_real_batch_of_slots(
    th: tuple[Any, Any],
) -> None:
    """Заказ ставится батчем со слотами и исполняется."""
    client, broker = th
    kind = f"registration-{uuid4().hex[:8]}"
    replenisher = TallyhoReplenisher(client, kind=kind, register=register_account)

    await replenisher.order(3, "replenish:2026-10-19T16:00:00+00:00")
    await broker.drain()

    assert sorted(slot for _, slot in registered) == [0, 1, 2]


async def test_repeated_order_adds_only_what_is_missing(th: tuple[Any, Any]) -> None:
    """Повторный такт того же окна не дублирует слоты и не теряет новые.

    Главное свойство схемы, и проверить его можно только живьём: ключ батча по
    бюджетному окну плюс уникальный индекс `(batch_id, key)` на слотах. Нумерация
    продолжается по `found`, поэтому уже выполненные слоты её не сбивают.
    """
    client, broker = th
    kind = f"registration-{uuid4().hex[:8]}"
    window = "replenish:2026-10-19T17:00:00+00:00"
    replenisher = TallyhoReplenisher(client, kind=kind, register=register_account)

    await replenisher.order(2, window)
    await broker.drain()          # первые два слота уже выполнены
    await replenisher.order(2, window)
    await broker.drain()

    assert sorted(slot for _, slot in registered) == [0, 1, 2, 3]
    assert {key for key, _ in registered} == {window}


async def test_order_in_a_new_window_starts_its_own_batch(
    th: tuple[Any, Any],
) -> None:
    """Новое бюджетное окно — новый батч: бюджет считается по тому же окну, что ключ."""
    client, broker = th
    kind = f"registration-{uuid4().hex[:8]}"
    replenisher = TallyhoReplenisher(client, kind=kind, register=register_account)

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
    kind = f"registration-{uuid4().hex[:8]}"
    clock = Clock()
    replenisher = TallyhoReplenisher(client, kind=kind, register=register_account)
    ledger = TallyhoOrderLedger(
        client,
        kinds=[kind],
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

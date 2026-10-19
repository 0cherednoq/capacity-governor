"""Сквозная приёмка: контур, ограниченный ресурс и работа, которая не горит.

Сценарий A-RM из приёмки Tallyho, но целиком через governor: 20 единиц работы, пул
ресурсов с убывающей ёмкостью 5 → 3 → 0, затем пополнение. Проверяется то, ради чего
всё и делалось:

* **ноль сгоревших попыток** — задача, которой не досталось ресурса, откладывает себя,
  а не падает;
* ровно 20 платных вызовов — ни одного лишнего обращения к внешнему сервису;
* не больше одного параллельного вызова с одного ресурса;
* 20 из 20 доходят после пополнения.

Настоящие Postgres и Tallyho; смоделирован только сам пул ресурсов — он и есть то,
что в бою даёт Identity Hub.

Пропускается без базы и без Tallyho.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

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
from capacity_adapters.tallyho import TallyhoDemand, TallyhoWaker, TallyhoWindows

DSN = os.environ.get(
    "CAPACITY_TEST_DSN",
    "postgresql+asyncpg://capacity:capacity@127.0.0.1:55444/capacity",
)
SCHEMA = "capacity_acceptance"

tallyho = pytest.importorskip("tallyho", reason="Tallyho не установлен")
testing = pytest.importorskip("tallyho.testing", reason="Tallyho не установлен")


@dataclass
class ResourcePool:
    """Пул эксклюзивных ресурсов с конечным запасом. В бою это аккаунты Identity Hub.

    У каждого ресурса свои кредиты: исчерпав их, он перестаёт быть годным. Так ёмкость
    убывает по ходу работы сама, как в жизни, а не по расписанию теста.
    """

    credits: dict[int, int] = field(default_factory=dict)
    leased: set[int] = field(default_factory=set)
    paid_calls: int = 0
    parallel_now: int = 0
    peak_parallel: int = 0
    deferrals: int = 0

    def add(self, count: int, credits_each: int) -> None:
        start = max(self.credits, default=-1) + 1
        for resource in range(start, start + count):
            self.credits[resource] = credits_each

    def ban_all(self) -> None:
        for resource in self.credits:
            self.credits[resource] = 0

    @property
    def alive(self) -> int:
        return sum(1 for left in self.credits.values() if left > 0)

    def lease(self) -> int | None:
        for resource, left in self.credits.items():
            if left > 0 and resource not in self.leased:
                self.leased.add(resource)
                self.credits[resource] = left - 1
                return resource
        return None

    def release(self, resource: int) -> None:
        self.leased.discard(resource)

    async def capacity(self) -> EligibleCapacity:
        return EligibleCapacity(
            eligible=self.alive, free=max(0, self.alive - len(self.leased))
        )


pool = ResourcePool()


async def paid_operation(item_no: int) -> None:
    """Единица работы: платный вызов, который можно делать только с ресурса."""
    resource = pool.lease()
    if resource is None:
        # Ресурса нет — откладываем, не тратя попытку. Это и есть «не сгоревший ретрай».
        pool.deferrals += 1
        # Срок намеренно длинный: пока ресурса нет, работа не должна возвращаться в
        # брокер по таймеру. Разбудит её контур, когда окно снова откроется.
        tallyho.item.defer(reason="no_resource", delay=timedelta(minutes=5))
        return
    pool.parallel_now += 1
    pool.peak_parallel = max(pool.peak_parallel, pool.parallel_now)
    try:
        pool.paid_calls += 1
        tallyho.item.ok("done")
    finally:
        pool.parallel_now -= 1
        pool.release(resource)


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
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        await connection.execute(GOVERNOR_STATE.delete())
        await connection.execute(GOVERNOR_OVERRIDE.delete())
    broker = testing.InlineBroker()
    client = tallyho.Tallyho(engine, schema=SCHEMA, relay_grace=timedelta(0))
    client.install(broker.adapter)
    await client.migrate()
    pool.credits.clear()
    pool.leased.clear()
    pool.paid_calls = 0
    pool.parallel_now = 0
    pool.peak_parallel = 0
    pool.deferrals = 0
    try:
        yield client, broker
    finally:
        await client.aclose()


class Clock:
    def __init__(self) -> None:
        self.moment = datetime(2026, 10, 20, 9, 0, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.moment

    def tick(self, seconds: int = 5) -> None:
        self.moment += timedelta(seconds=seconds)


async def test_a_rm_work_survives_a_vanishing_pool(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """20 единиц работы, ёмкость 5 → 3 → 0, пополнение. Ни одной сгоревшей попытки."""
    client, broker = th
    kind = f"work-{uuid4().hex[:8]}"
    clock = Clock()

    async with client.batch(kind, max_in_flight=0) as batch:
        await batch.map(paid_operation, list(range(20)))
    handle = batch.handle

    maker = async_sessionmaker(engine, expire_on_commit=False)
    scope = PostgresTickScope(session_scope=maker.begin, scope="acceptance")
    # `wakeable=True`: контур умеет будить отложенных, значит их работа входит в
    # спрос. Иначе спрос обнулится, окно не откроется и будить будет нечего.
    demand = TallyhoDemand(client, kinds=[kind], wakeable=True)
    governor = Governor(
        scope=scope,
        capacity_source=pool,
        registry=demand,
        windows=TallyhoWindows(client, demand, session_provider=scope.session),
        state=PostgresStateStore(scope),
        policy=MaxMinFair(),
        clock=clock,
        stabilizer=Stabilizer(),
        # Часы будильника — настоящие: `reschedule` переносит работу на переданный
        # момент, а виртуальные часы такта идут впереди и отправили бы её в будущее.
        waker=TallyhoWaker(client, now=lambda: datetime.now(UTC)),
        min_tick_interval=timedelta(seconds=5),
    )

    # Пять ресурсов, по два вызова каждый: на 20 единиц работы их заведомо мало.
    pool.add(count=5, credits_each=2)
    first = await governor.tick()
    await broker.drain()
    assert first.plan is not None
    assert first.plan.allocation.grants == {str(handle.id): 5}

    # Часть ресурсов исчерпана расходом — ёмкость убыла сама. Окно режется в том же
    # такте, переподписки нет.
    clock.tick()
    second = await governor.tick()
    await broker.drain()
    assert second.plan is not None
    assert second.plan.report.oversubscribed == 0

    # Пул забанен целиком. Окно уходит в 0 немедленно, оставшаяся работа ждёт
    # своего срока отложения, не тратя попыток.
    pool.ban_all()
    clock.tick()
    third = await governor.tick()
    await broker.drain()
    assert third.plan is not None
    assert third.plan.allocation.grants == {str(handle.id): 0}
    assert pool.alive == 0

    # Пополнение: ресурсы завели заново. Контур открывает окно и будит отложенных.
    pool.add(count=5, credits_each=3)
    for _ in range(12):
        clock.tick()
        await governor.tick()
        await broker.drain()
        if (await handle.view()).progress.ok == 20:
            break

    view = await handle.view()
    assert view.progress.ok == 20, "работа должна дойти вся"
    assert view.progress.error == 0, "ни одной сгоревшей попытки"
    assert pool.paid_calls == 20, "ровно 20 платных вызовов, без повторов"
    assert pool.deferrals > 0, "работа при нехватке ресурса откладывалась"
    assert pool.peak_parallel <= 5, "параллельных вызовов не больше, чем ресурсов"


async def test_a_rm_window_never_exceeds_the_pool(
    engine: AsyncEngine, th: tuple[Any, Any]
) -> None:
    """Пока работа идёт, окно ни разу не превышает число живых ресурсов."""
    client, broker = th
    kind = f"work-{uuid4().hex[:8]}"
    clock = Clock()

    async with client.batch(kind, max_in_flight=0) as batch:
        await batch.map(paid_operation, list(range(20)))

    maker = async_sessionmaker(engine, expire_on_commit=False)
    scope = PostgresTickScope(session_scope=maker.begin, scope="acceptance-window")
    demand = TallyhoDemand(client, kinds=[kind], wakeable=True)
    governor = Governor(
        scope=scope,
        capacity_source=pool,
        registry=demand,
        windows=TallyhoWindows(client, demand, session_provider=scope.session),
        state=PostgresStateStore(scope),
        policy=MaxMinFair(),
        clock=clock,
        stabilizer=Stabilizer(),
        waker=TallyhoWaker(client, now=lambda: datetime.now(UTC)),
    )

    pool.add(count=5, credits_each=1)
    overruns = []
    for extra in (0, 0, 3, 0, 0, 2, 0):
        if extra:
            pool.add(count=extra, credits_each=1)
        clock.tick()
        alive = pool.alive
        outcome = await governor.tick()
        assert outcome.plan is not None
        granted = sum(outcome.plan.allocation.grants.values())
        if granted > alive:
            overruns.append((alive, granted))
        await broker.drain()

    assert overruns == []
    assert pool.paid_calls <= 20

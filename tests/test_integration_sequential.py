"""Сценарий 2 гейта (п.9 плана): массовая рассылка на `Sequential`, живьём.

Контура здесь нет, и это не упущение, а вывод G3: окно выделенного пула не считается
политикой раздачи, оно выводится из статуса кампании в транзакции смены статуса.
Поэтому прогон выглядит так, как он выглядел бы в проекте: политика говорит, какое
окно положено, а выставляет его домен — в той же транзакции, что и смену отправителя.

Проверяется главное свойство §7.2: расход идёт строго по текущему ресурсу. Пока
отправитель жив — окно 1 и письма уходят через него; умер — окно 0, работа стоит,
домен переключает отправителя, и расход продолжается по следующему, а не
размазывается по всему пулу.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from capacity import ConsumerState, Sequential

DSN = os.environ.get(
    "CAPACITY_TEST_DSN",
    "postgresql+asyncpg://capacity:capacity@127.0.0.1:55444/capacity",
)
SCHEMA = "capacity_it_seq"

tallyho = pytest.importorskip("tallyho", reason="Tallyho не установлен")
testing = pytest.importorskip("tallyho.testing", reason="Tallyho не установлен")


@dataclass(slots=True)
class Campaign:
    """Домен проекта: кампания знает свой пул отправителей и текущего из них.

    Переключение под блокировкой строки со сравнением «текущий == умерший» — тоже
    домен, в библиотеку оно не попадает (G3). Здесь воспроизведена его суть: сменить
    отправителя можно только на того, кого считали мёртвым.
    """

    campaign_id: str
    senders: list[str]
    current: str | None = None
    dead: set[str] = field(default_factory=set)
    sent_by: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.current = self.senders[0]

    @property
    def alive(self) -> bool:
        return self.current is not None and self.current not in self.dead

    def kill_current(self) -> None:
        if self.current is not None:
            self.dead.add(self.current)

    def switch(self, dead_sender: str) -> None:
        if self.current != dead_sender:
            return  # кто-то уже переключил — повтор ничего не делает
        for candidate in self.senders:
            if candidate not in self.dead:
                self.current = candidate
                return
        self.current = None

    def record(self) -> None:
        assert self.current is not None
        self.sent_by[self.current] = self.sent_by.get(self.current, 0) + 1


campaigns: dict[str, Campaign] = {}


async def send_letter(address: str) -> None:
    """Единица работы рассылки: письмо уходит через текущего отправителя кампании."""
    campaign = campaigns[address.split("@")[1]]
    campaign.record()
    tallyho.item.ok("sent")


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    live = create_async_engine(DSN)
    try:
        async with live.connect() as connection:
            await connection.close()
    except Exception:
        await live.dispose()
        pytest.skip(f"нет Postgres по {DSN}")
    try:
        yield live
    finally:
        await live.dispose()


@pytest.fixture
async def th(engine: AsyncEngine) -> AsyncIterator[tuple[object, object]]:
    # Схема пересоздаётся на каждый тест: иначе батчи прошлого прогона остаются в
    # базе с открытыми окнами, и `drain()` выполняет чужую работу.
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
    broker = testing.InlineBroker()
    client = tallyho.Tallyho(engine, schema=SCHEMA, relay_grace=timedelta(0))
    client.install(broker.adapter)
    await client.migrate()
    campaigns.clear()
    try:
        yield client, broker
    finally:
        await client.aclose()


async def make_campaign(
    client: Any, campaign: Campaign, letters: int
) -> tuple[UUID, Any]:
    campaigns[campaign.campaign_id] = campaign
    addresses = [f"user{i}@{campaign.campaign_id}" for i in range(letters)]
    async with client.batch("mailing", max_in_flight=0) as batch:
        await batch.map(send_letter, addresses)
    return batch.handle.id, batch.handle


async def apply_window(handle: Any, campaign: Campaign, policy: Sequential) -> int:
    """То, что в проекте делает транзакция смены статуса кампании (§9.2)."""
    state = ConsumerState(campaign.campaign_id, demand=10_000)
    alive = {campaign.campaign_id} if campaign.alive else set()
    window = policy.allocate_alive([state], alive).grants[campaign.campaign_id]
    await handle.set_max_in_flight(window)
    return window


# --- сценарий -----------------------------------------------------------------------


async def test_letters_go_through_the_current_sender_only(
    th: tuple[object, object],
) -> None:
    """Расход последовательный: пока отправитель жив, весь трафик идёт через него."""
    client, broker = th
    campaign = Campaign("alpha.test", senders=["s1", "s2", "s3"])
    _, handle = await make_campaign(client, campaign, letters=5)

    assert await apply_window(handle, campaign, Sequential()) == 1
    await broker.drain()  # type: ignore[attr-defined]

    assert campaign.sent_by == {"s1": 5}


async def test_dead_sender_closes_the_window(th: tuple[object, object]) -> None:
    """Ресурс умер — окно 0, работа стоит и ждёт решения домена, а не падает."""
    client, broker = th
    campaign = Campaign("beta.test", senders=["s1", "s2"])
    _, handle = await make_campaign(client, campaign, letters=4)
    campaign.kill_current()

    assert await apply_window(handle, campaign, Sequential()) == 0
    await broker.drain()  # type: ignore[attr-defined]

    assert campaign.sent_by == {}


async def test_switching_continues_the_sequence_on_the_next_sender(
    th: tuple[object, object],
) -> None:
    """Переключение — дело домена; политика лишь снова разрешает работу."""
    client, broker = th
    campaign = Campaign("gamma.test", senders=["s1", "s2"])
    _, handle = await make_campaign(client, campaign, letters=6)
    policy = Sequential()

    campaign.kill_current()
    assert await apply_window(handle, campaign, policy) == 0
    campaign.switch("s1")
    assert await apply_window(handle, campaign, policy) == 1
    await broker.drain()  # type: ignore[attr-defined]

    assert campaign.sent_by == {"s2": 6}
    assert campaign.current == "s2"


async def test_switch_is_idempotent_for_a_stale_report(
    th: tuple[object, object],
) -> None:
    """Повторный отчёт о смерти старого отправителя не уводит кампанию дальше."""
    client, _ = th
    campaign = Campaign("delta.test", senders=["s1", "s2", "s3"])
    await make_campaign(client, campaign, letters=1)

    campaign.kill_current()
    campaign.switch("s1")
    campaign.switch("s1")  # опоздавший отчёт о том же отправителе

    assert campaign.current == "s2"


async def test_exhausted_pool_leaves_the_campaign_waiting(
    th: tuple[object, object],
) -> None:
    """Живых отправителей не осталось: окно 0, кампания ждёт пополнения (§8.4)."""
    client, broker = th
    campaign = Campaign("omega.test", senders=["s1"])
    _, handle = await make_campaign(client, campaign, letters=3)

    campaign.kill_current()
    campaign.switch("s1")

    assert campaign.current is None
    assert await apply_window(handle, campaign, Sequential()) == 0
    await broker.drain()  # type: ignore[attr-defined]
    assert campaign.sent_by == {}

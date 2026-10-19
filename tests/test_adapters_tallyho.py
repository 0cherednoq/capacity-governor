"""Адаптеры к Tallyho: окна, спрос, бюджетный журнал, заказ (п.6 плана).

Tallyho не установлен и не нужен: адаптеры написаны против его публичных сигнатур,
а здесь подставлены подделки с теми же именами и порядком аргументов.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from capacity.ports import ConsumerRegistry, Replenisher, WindowSink
from capacity_adapters.tallyho import (
    TallyhoConsumers,
    TallyhoOrderLedger,
    TallyhoReplenisher,
    TallyhoWindows,
    WindowJournal,
)

NOW = datetime(2026, 10, 19, 14, 0, 0, tzinfo=UTC)


@dataclass(slots=True)
class FakeProgress:
    found: int = 0
    queued: int = 0
    in_flight: int = 0


@dataclass(slots=True)
class FakeView:
    progress: FakeProgress


@dataclass(slots=True)
class FakeInfo:
    id: UUID
    key: str | None = None


@dataclass(slots=True)
class FakePage:
    items: list[FakeInfo]


@dataclass(slots=True)
class FakeHandle:
    batch_id: UUID
    client: FakeTallyho

    async def view(self) -> FakeView:
        self.client.views += 1
        return FakeView(self.client.progress[self.batch_id])

    async def set_max_in_flight(
        self, limit: int, *, session: object = None, reason: str | None = None
    ) -> None:
        self.client.applied.append((self.batch_id, limit, session, reason))


@dataclass(slots=True)
class FakeTallyho:
    progress: dict[UUID, FakeProgress] = field(default_factory=dict)
    listed: list[FakeInfo] = field(default_factory=list)
    applied: list[tuple[UUID, int, object, str | None]] = field(default_factory=list)
    queries: list[tuple[Collection[str] | None, datetime | None]] = field(
        default_factory=list
    )
    views: int = 0

    def handle(self, batch_id: UUID) -> FakeHandle:
        return FakeHandle(batch_id, self)

    async def list_batches(
        self,
        *,
        kinds: Collection[str] | None = None,
        created_after: datetime | None = None,
        limit: int = 100,
    ) -> FakePage:
        self.queries.append((kinds, created_after))
        return FakePage(list(self.listed))


@dataclass(slots=True)
class FakeJournal:
    windows: dict[str, int] = field(default_factory=dict)
    saves: int = 0

    async def load(self) -> Mapping[str, int]:
        return dict(self.windows)

    async def save(self, windows: Mapping[str, int]) -> None:
        self.windows = dict(windows)
        self.saves += 1


# --- окна ---------------------------------------------------------------------------


async def test_windows_are_applied_absolutely_in_the_tick_session() -> None:
    """Окна выставляются абсолютно и в сессии такта — одним коммитом (G6)."""
    batch = uuid4()
    client = FakeTallyho()
    session = object()
    sink = TallyhoWindows(
        client=client,
        resolve=lambda cid: batch,
        journal=FakeJournal(),
        session_provider=lambda: session,
    )

    await sink.set_windows({"c0": 7})

    assert client.applied == [(batch, 7, session, "capacity-governor")]


async def test_zero_window_is_applied_like_any_other() -> None:
    """`limit=0` закрывает окно: новая работа паркуется, текущая доделывается."""
    client = FakeTallyho()
    sink = TallyhoWindows(client, lambda cid: uuid4(), FakeJournal())

    await sink.set_windows({"c0": 0})

    assert [limit for _, limit, _, _ in client.applied] == [0]


async def test_journal_remembers_what_was_applied() -> None:
    """Текущее окно у Tallyho не прочитать, поэтому контур помнит его сам."""
    journal = FakeJournal({"c0": 3, "c1": 9})
    sink = TallyhoWindows(FakeTallyho(), lambda cid: uuid4(), journal)

    await sink.set_windows({"c0": 5})

    assert await sink.current_windows() == {"c0": 5, "c1": 9}


async def test_journal_is_not_touched_when_nothing_changes() -> None:
    """Контур не присылает неизменившихся окон, но и пустой вызов не пишет в журнал."""
    journal = FakeJournal({"c0": 3})
    sink = TallyhoWindows(FakeTallyho(), lambda cid: uuid4(), journal)

    await sink.set_windows({})

    assert journal.saves == 1  # запись одна, значения те же
    assert journal.windows == {"c0": 3}


def test_windows_adapter_satisfies_the_port() -> None:
    assert isinstance(
        TallyhoWindows(FakeTallyho(), lambda cid: uuid4(), FakeJournal()), WindowSink
    )
    assert isinstance(FakeJournal(), WindowJournal)


# --- спрос --------------------------------------------------------------------------


async def test_demand_is_queued_plus_in_flight() -> None:
    """Спрос — остаток работы: и то, что ждёт, и то, что уже у брокера."""
    batch = uuid4()
    client = FakeTallyho({batch: FakeProgress(found=100, queued=30, in_flight=5)})
    registry = TallyhoConsumers(client, lambda: _batches({"c0": batch}))

    states = await registry.consumers()

    assert [(s.consumer_id, s.demand) for s in states] == [("c0", 35)]


async def test_weights_guarantees_and_ceilings_are_passed_through() -> None:
    batch = uuid4()
    client = FakeTallyho({batch: FakeProgress(queued=10)})
    registry = TallyhoConsumers(
        client,
        lambda: _batches({"c0": batch}),
        weights={"c0": 3},
        guarantees={"c0": 2},
        ceilings={"c0": 8},
    )

    state = (await registry.consumers())[0]

    assert (state.weight, state.guaranteed, state.ceiling) == (3, 2, 8)


async def test_finished_batch_reports_no_demand() -> None:
    """Окно под отсутствующую работу не выдаётся — политика увидит нулевой спрос."""
    batch = uuid4()
    client = FakeTallyho({batch: FakeProgress(found=50, queued=0, in_flight=0)})
    registry = TallyhoConsumers(client, lambda: _batches({"c0": batch}))

    assert (await registry.consumers())[0].demand == 0


async def test_one_view_per_consumer_per_tick() -> None:
    """Пока дешёвого `demand()` (T3) нет, цена спроса — по одному `view()` на батч."""
    batches = {f"c{i}": uuid4() for i in range(4)}
    client = FakeTallyho({bid: FakeProgress(queued=1) for bid in batches.values()})
    registry = TallyhoConsumers(client, lambda: _batches(batches))

    await registry.consumers()

    assert client.views == 4


def test_registry_adapter_satisfies_the_port() -> None:
    assert isinstance(
        TallyhoConsumers(FakeTallyho(), lambda: _batches({})), ConsumerRegistry
    )


# --- бюджетный журнал ---------------------------------------------------------------


async def test_ledger_counts_slots_in_window_and_in_progress() -> None:
    """В окне — все слоты (бюджет тратит заказ), в работе — только незавершённые."""
    first, second = uuid4(), uuid4()
    client = FakeTallyho(
        progress={
            first: FakeProgress(found=20, queued=5, in_flight=3),
            second: FakeProgress(found=10, queued=0, in_flight=0),
        },
        listed=[FakeInfo(first), FakeInfo(second)],
    )
    ledger = TallyhoOrderLedger(
        client, kinds=["registration"], window=timedelta(hours=1), now=lambda: NOW
    )

    in_window, in_progress = await ledger.counts()

    assert (in_window, in_progress) == (30, 8)


async def test_ledger_asks_only_for_the_budget_window() -> None:
    client = FakeTallyho()
    ledger = TallyhoOrderLedger(
        client, kinds=["registration"], window=timedelta(hours=1), now=lambda: NOW
    )

    await ledger.counts()

    assert client.queries == [(["registration"], NOW - timedelta(hours=1))]


async def test_ledger_is_cached_for_the_tick() -> None:
    """Политике журнал нужен раз в такт — читать его на каждый вопрос незачем."""
    batch = uuid4()
    client = FakeTallyho(
        progress={batch: FakeProgress(found=1)}, listed=[FakeInfo(batch)]
    )
    moment = NOW
    ledger = TallyhoOrderLedger(
        client,
        kinds=["registration"],
        window=timedelta(hours=1),
        now=lambda: moment,
        cache_for=timedelta(seconds=5),
    )

    await ledger.counts()
    await ledger.counts()

    assert len(client.queries) == 1


async def test_ledger_cache_expires() -> None:
    batch = uuid4()
    client = FakeTallyho(
        progress={batch: FakeProgress(found=1)}, listed=[FakeInfo(batch)]
    )
    moments = iter([NOW, NOW + timedelta(seconds=6)])
    ledger = TallyhoOrderLedger(
        client,
        kinds=["registration"],
        window=timedelta(hours=1),
        now=lambda: next(moments),
        cache_for=timedelta(seconds=5),
    )

    await ledger.counts()
    await ledger.counts()

    assert len(client.queries) == 2


# --- заказ --------------------------------------------------------------------------


async def test_order_passes_the_budget_window_key() -> None:
    """Ключ батча — начало бюджетного окна, а не минута такта (G4)."""
    submitted: list[tuple[str, int]] = []

    async def submit(window_key: str, count: int) -> None:
        submitted.append((window_key, count))

    replenisher = TallyhoReplenisher(submit)
    await replenisher.order(20, "replenish:2026-10-19T14:00:00+00:00")

    assert submitted == [("replenish:2026-10-19T14:00:00+00:00", 20)]


@pytest.mark.parametrize("count", [0, -1])
async def test_empty_order_does_nothing(count: int) -> None:
    submitted: list[tuple[str, int]] = []

    async def submit(window_key: str, number: int) -> None:
        submitted.append((window_key, number))

    await TallyhoReplenisher(submit).order(count, "replenish:x")

    assert submitted == []


def test_replenisher_satisfies_the_port() -> None:
    async def submit(window_key: str, count: int) -> None:
        return None

    assert isinstance(TallyhoReplenisher(submit), Replenisher)


async def _batches(mapping: Mapping[str, UUID]) -> Mapping[str, UUID]:
    return dict(mapping)

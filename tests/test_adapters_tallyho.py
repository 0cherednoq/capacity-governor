"""Адаптеры к Tallyho: спрос, окна, бюджетный журнал, заказ пополнения.

Tallyho не установлен и не нужен: адаптеры написаны против его публичных сигнатур,
а здесь подставлены подделки с теми же именами и порядком аргументов.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from capacity.ports import ConsumerRegistry, Replenisher, WindowSink
from capacity_adapters.tallyho import (
    DemandTruncatedError,
    TallyhoDemand,
    TallyhoOrderLedger,
    TallyhoReplenisher,
    TallyhoWindows,
    TaskCall,
    ready_demand,
)

NOW = datetime(2026, 10, 19, 14, 0, 0, tzinfo=UTC)


@dataclass(slots=True)
class FakeDemand:
    """Строка `th.demand()`."""

    id: UUID = field(default_factory=uuid4)
    kind: str = "domains"
    key: str | None = None
    attributes: Mapping[str, Any] = field(default_factory=dict)
    pending: int = 0
    in_flight: int = 0
    deferred: int = 0
    max_in_flight: int | None = 0
    paused: bool = False
    truncated: bool = False


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
        # Следующий `demand()` обязан вернуть выставленное окно: иначе подделка врёт
        # ровно в том месте, ради которого журнал окон и был убран.
        for row in self.client.rows:
            if row.id == self.batch_id:
                row.max_in_flight = limit


@dataclass(slots=True)
class FakeCall:
    fn: object
    args: tuple[object, ...]
    key: str | None = None

    def opts(self, *, key: str | None = None, **options: object) -> TaskCall:
        return FakeCall(self.fn, self.args, key)


@dataclass(slots=True)
class FakeBuilder:
    client: FakeTallyho

    async def add_calls(self, calls: Iterable[TaskCall]) -> None:
        self.client.added.extend(getattr(call, "key", "") or "" for call in calls)


@dataclass(slots=True)
class FakeBatchScope:
    """`async with client.batch(...)`: классом, чтобы `__aexit__` вернул `bool | None`."""

    client: FakeTallyho

    async def __aenter__(self) -> FakeBuilder:
        return FakeBuilder(self.client)

    async def __aexit__(self, *exc_info: object) -> bool | None:
        return None


@dataclass(slots=True)
class FakeTallyho:
    rows: list[FakeDemand] = field(default_factory=list)
    progress: dict[UUID, FakeProgress] = field(default_factory=dict)
    listed: list[FakeInfo] = field(default_factory=list)
    found_by_key: dict[str, UUID] = field(default_factory=dict)
    applied: list[tuple[UUID, int, object, str | None]] = field(default_factory=list)
    demand_calls: list[tuple[Sequence[str], Mapping[str, object] | None, int]] = field(
        default_factory=list
    )
    queries: list[tuple[Collection[str] | None, datetime | None]] = field(
        default_factory=list
    )
    batches: list[tuple[str, str | None, bool, int | None]] = field(default_factory=list)
    added: list[str] = field(default_factory=list)
    views: int = 0

    def handle(self, batch_id: UUID) -> FakeHandle:
        return FakeHandle(batch_id, self)

    def call(self, fn: object, *args: object) -> FakeCall:
        return FakeCall(fn, args)

    async def demand(
        self,
        kinds: Sequence[str],
        *,
        attributes: Mapping[str, object] | None = None,
        limit: int = 1000,
    ) -> list[FakeDemand]:
        self.demand_calls.append((list(kinds), attributes, limit))
        return list(self.rows)

    async def find(self, kind: str, key: str) -> FakeHandle:
        if key not in self.found_by_key:
            raise LookupError(f"батча {kind}:{key} нет")
        return FakeHandle(self.found_by_key[key], self)

    def batch(
        self,
        kind: str,
        *,
        key: str | None = None,
        seal: bool = True,
        max_in_flight: int | None = None,
    ) -> FakeBatchScope:
        self.batches.append((kind, key, seal, max_in_flight))
        return FakeBatchScope(self)

    async def list_batches(
        self,
        *,
        kinds: Collection[str] | None = None,
        created_after: datetime | None = None,
        limit: int = 100,
    ) -> FakePage:
        self.queries.append((kinds, created_after))
        return FakePage(list(self.listed))


def demand_source(client: FakeTallyho, **kwargs: Any) -> TallyhoDemand:
    return TallyhoDemand(client, kinds=["domains"], **kwargs)


# --- спрос --------------------------------------------------------------------------


async def test_one_demand_call_per_tick_whatever_the_number_of_batches() -> None:
    """Главный выигрыш: число запросов не зависит от числа потребителей."""
    client = FakeTallyho(rows=[FakeDemand(pending=10) for _ in range(20)])
    source = demand_source(client)

    states = await source.consumers()

    assert len(states) == 20
    assert len(client.demand_calls) == 1


async def test_deferred_work_is_not_counted_as_ready_demand() -> None:
    """Отложенный item не уйдёт в брокер раньше срока — окно под него простаивает."""
    client = FakeTallyho(rows=[FakeDemand(pending=30, deferred=12)])
    source = demand_source(client)

    assert (await source.consumers())[0].demand == 18
    # Но для пополнения нужна вся оставшаяся работа, включая отложенную.
    assert source.pending_work() == 30
    assert source.deferred_work() == 12


def test_ready_demand_never_goes_negative() -> None:
    assert ready_demand(FakeDemand(pending=3, deferred=10)) == 0


async def test_paused_batch_is_not_a_consumer() -> None:
    """Пауза — решение человека: батч не работает, и окно ему не нужно."""
    client = FakeTallyho(
        rows=[FakeDemand(pending=10, paused=True), FakeDemand(pending=10)]
    )
    source = demand_source(client)

    assert len(await source.consumers()) == 1


async def test_batch_without_a_window_is_not_ours() -> None:
    """`max_in_flight is None` — окно никто не выставлял: это чужая работа."""
    client = FakeTallyho(
        rows=[FakeDemand(pending=10, max_in_flight=None), FakeDemand(pending=10)]
    )
    source = demand_source(client)

    assert len(await source.consumers()) == 1


async def test_zero_window_differs_from_no_window() -> None:
    """Ноль — это закрытое контуром окно, и такой батч остаётся в раздаче."""
    client = FakeTallyho(rows=[FakeDemand(pending=10, max_in_flight=0)])
    source = demand_source(client)

    states = await source.consumers()

    assert len(states) == 1
    assert source.windows() == {states[0].consumer_id: 0}


async def test_truncated_demand_is_an_error_not_a_silent_half_list() -> None:
    """По неполному списку раздавать нельзя: невидимые потребители получат ноль."""
    client = FakeTallyho(rows=[FakeDemand(pending=1, truncated=True)])
    source = demand_source(client, limit=1)

    with pytest.raises(DemandTruncatedError, match="усечён"):
        await source.consumers()


async def test_weights_guarantees_and_ceilings_come_from_the_project() -> None:
    """Приоритеты знает проект: библиотека берёт их функцией от строки спроса."""
    client = FakeTallyho(rows=[FakeDemand(pending=10, kind="urgent")])
    source = demand_source(
        client,
        weight_of=lambda row: 3 if row.kind == "urgent" else 1,
        guaranteed_of=lambda _: 2,
        ceiling_of=lambda _: 8,
    )

    state = (await source.consumers())[0]

    assert (state.weight, state.guaranteed, state.ceiling) == (3, 2, 8)


async def test_attributes_filter_is_passed_through() -> None:
    client = FakeTallyho()
    source = demand_source(client, attributes={"run": "nightly"})

    await source.consumers()

    assert client.demand_calls == [(["domains"], {"run": "nightly"}, 1000)]


def test_demand_source_satisfies_the_registry_port() -> None:
    assert isinstance(demand_source(FakeTallyho()), ConsumerRegistry)


# --- окна ---------------------------------------------------------------------------


async def test_current_windows_come_from_the_same_snapshot_as_demand() -> None:
    """Спрос и окна описывают один момент: журнала окон больше нет."""
    rows = [
        FakeDemand(pending=10, max_in_flight=4),
        FakeDemand(pending=5, max_in_flight=0),
    ]
    client = FakeTallyho(rows=rows)
    source = demand_source(client)
    sink = TallyhoWindows(client, source)

    states = await source.consumers()
    windows = await sink.current_windows()

    assert windows == {states[0].consumer_id: 4, states[1].consumer_id: 0}
    assert len(client.demand_calls) == 1  # отдельного чтения окон нет


async def test_windows_are_applied_in_the_tick_session() -> None:
    row = FakeDemand(pending=10, max_in_flight=1)
    client = FakeTallyho(rows=[row])
    source = demand_source(client)
    session = object()
    sink = TallyhoWindows(client, source, session_provider=lambda: session)
    await source.consumers()

    await sink.set_windows({str(row.id): 7})

    assert client.applied == [(row.id, 7, session, "capacity-governor")]


async def test_zero_window_is_applied_like_any_other() -> None:
    """`limit=0` закрывает окно: новая работа паркуется, текущая доделывается."""
    row = FakeDemand(pending=10, max_in_flight=5)
    client = FakeTallyho(rows=[row])
    source = demand_source(client)
    sink = TallyhoWindows(client, source)
    await source.consumers()

    await sink.set_windows({str(row.id): 0})

    assert [limit for _, limit, _, _ in client.applied] == [0]


def test_windows_adapter_satisfies_the_port() -> None:
    client = FakeTallyho()
    assert isinstance(TallyhoWindows(client, demand_source(client)), WindowSink)


# --- бюджетный журнал ---------------------------------------------------------------


async def test_ledger_counts_all_slots_in_window_and_unfinished_in_progress() -> None:
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

    assert await ledger.counts() == (30, 8)


async def test_ledger_asks_only_for_the_budget_window() -> None:
    client = FakeTallyho()
    ledger = TallyhoOrderLedger(
        client, kinds=["registration"], window=timedelta(hours=1), now=lambda: NOW
    )

    await ledger.counts()

    assert client.queries == [(["registration"], NOW - timedelta(hours=1))]


async def test_ledger_is_cached_for_the_tick() -> None:
    batch = uuid4()
    client = FakeTallyho(
        progress={batch: FakeProgress(found=1)}, listed=[FakeInfo(batch)]
    )
    ledger = TallyhoOrderLedger(
        client,
        kinds=["registration"],
        window=timedelta(hours=1),
        now=lambda: NOW,
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


async def register_account(window_key: str, slot: int) -> None:
    """Задача регистрации: её содержание пишет проект, библиотека знает только ключ."""


def replenisher(client: FakeTallyho, **kwargs: Any) -> TallyhoReplenisher:
    return TallyhoReplenisher(
        client, kind="registration", register=register_account, **kwargs
    )


async def test_order_creates_a_batch_keyed_by_the_budget_window() -> None:
    client = FakeTallyho()

    await replenisher(client).order(3, "replenish:2026-10-19T14:00:00+00:00")

    assert client.batches == [
        ("registration", "replenish:2026-10-19T14:00:00+00:00", False, None)
    ]
    assert client.added == ["slot:0", "slot:1", "slot:2"]


async def test_repeated_order_continues_numbering_past_finished_slots() -> None:
    """Выполненный слот свой ключ не освобождает — считаем по `found`, не по остатку."""
    existing = uuid4()
    client = FakeTallyho(
        progress={existing: FakeProgress(found=3, queued=0, in_flight=0)},
        found_by_key={"replenish:w1": existing},
    )

    await replenisher(client).order(2, "replenish:w1")

    assert client.added == ["slot:3", "slot:4"]


async def test_batch_input_stays_open_for_the_window() -> None:
    client = FakeTallyho()

    await replenisher(client).order(1, "replenish:w1")

    assert client.batches[0][2] is False


async def test_registration_parallelism_can_be_capped() -> None:
    client = FakeTallyho()

    await replenisher(client, max_in_flight=2).order(5, "replenish:w1")

    assert client.batches[0][3] == 2


@pytest.mark.parametrize("count", [0, -1])
async def test_empty_order_touches_nothing(count: int) -> None:
    client = FakeTallyho()

    await replenisher(client).order(count, "replenish:w1")

    assert client.batches == []
    assert client.added == []


def test_replenisher_satisfies_the_port() -> None:
    assert isinstance(replenisher(FakeTallyho()), Replenisher)

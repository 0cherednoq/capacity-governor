"""Контур целиком на поддельных портах (G6, G7): порядок такта, лидер, дебаунс."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from capacity import (
    ConsumerState,
    EligibleCapacity,
    Governor,
    MaxMinFair,
    ReorderPoint,
    Stabilizer,
    SupplySnapshot,
    TickStatus,
)
from capacity.ports import (
    CapacitySource,
    Clock,
    ConsumerRegistry,
    OverrideStore,
    Replenisher,
    StateStore,
    SupplySource,
    TickScope,
    WindowSink,
)
from tests.fakes import (
    FailingCapacity,
    FakeCapacity,
    FakeClock,
    FakeObserver,
    FakeOverrides,
    FakeRegistry,
    FakeReplenisher,
    FakeScope,
    FakeState,
    FakeSupply,
    FakeWindows,
)

NOW = datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)


def build(
    *,
    free: int = 10,
    demands: tuple[int, ...] = (100, 100),
    leading: bool = True,
    windows: dict[str, int] | None = None,
    overrides: FakeOverrides | None = None,
    supply: SupplySnapshot | None = None,
) -> tuple[Governor, dict[str, object]]:
    parts: dict[str, object] = {
        "scope": FakeScope(leading=leading),
        "capacity": FakeCapacity(
            EligibleCapacity(eligible=free, free=free, effective=float(free))
        ),
        "registry": FakeRegistry(
            [ConsumerState(f"c{i}", demand=d) for i, d in enumerate(demands)]
        ),
        "windows": FakeWindows(windows or {}),
        "state": FakeState(),
        "clock": FakeClock(NOW),
        "overrides": overrides or FakeOverrides(),
        "replenisher": FakeReplenisher(),
        "observer": FakeObserver(),
    }
    governor = Governor(
        scope=parts["scope"],  # type: ignore[arg-type]
        capacity_source=parts["capacity"],  # type: ignore[arg-type]
        registry=parts["registry"],  # type: ignore[arg-type]
        windows=parts["windows"],  # type: ignore[arg-type]
        state=parts["state"],  # type: ignore[arg-type]
        policy=MaxMinFair(),
        clock=parts["clock"],  # type: ignore[arg-type]
        stabilizer=Stabilizer(),
        overrides=parts["overrides"],  # type: ignore[arg-type]
        supply=FakeSupply(supply) if supply else None,
        reorder=ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=20)
        if supply
        else None,
        replenisher=parts["replenisher"],  # type: ignore[arg-type]
        observer=parts["observer"],  # type: ignore[arg-type]
        min_tick_interval=timedelta(seconds=5),
    )
    return governor, parts


# --- порядок такта ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tick_applies_windows_and_saves_state() -> None:
    governor, parts = build(free=10, demands=(100, 100))

    outcome = await governor.tick()

    assert outcome.status is TickStatus.RAN
    assert parts["windows"].windows == {"c0": 5, "c1": 5}  # type: ignore[attr-defined]
    assert parts["state"].saves == 1  # type: ignore[attr-defined]
    assert parts["scope"].committed == 1  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_unchanged_windows_are_not_written() -> None:
    """Не изменившееся окно не пишется вовсе (G6) — ни одной записи в БД."""
    governor, parts = build(free=10, demands=(100, 100), windows={"c0": 5, "c1": 5})

    await governor.tick()

    assert parts["windows"].writes == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_without_leadership_nothing_is_read_or_written() -> None:
    """Лок не взят — такт пропущен; лидерства как состояния нет (G6)."""
    governor, parts = build(leading=False, windows={"c0": 1})

    outcome = await governor.tick()

    assert outcome.status is TickStatus.NOT_LEADER
    assert outcome.plan is None
    assert parts["windows"].writes == []  # type: ignore[attr-defined]
    assert parts["state"].saves == 0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_order_is_placed_after_the_tick_commits() -> None:
    """Заказ пополнения идёт после коммита такта и вне лидерского лока (G6)."""
    governor, parts = build(
        free=0,
        demands=(100,),
        supply=SupplySnapshot(effective_available=0.0, needed_for_demand=5.0),
    )

    outcome = await governor.tick()

    assert parts["replenisher"].orders == [  # type: ignore[attr-defined]
        (20, "replenish:2026-10-19T12:00:00+00:00")
    ]
    assert outcome.plan is not None
    assert parts["scope"].committed == 1  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_no_order_no_call() -> None:
    governor, parts = build(
        free=10, demands=(5,), supply=SupplySnapshot(effective_available=50.0)
    )

    await governor.tick()

    assert parts["replenisher"].orders == []  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_report_goes_to_the_observer() -> None:
    governor, parts = build(free=6, demands=(100, 100))

    await governor.tick()

    assert len(parts["observer"].reports) == 1  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_state_survives_between_ticks() -> None:
    """Дефициты и удержания переносятся через хранилище: лидер меняется (G6)."""
    governor, parts = build(free=1, demands=(10, 10))

    first = await governor.tick()
    carried = dict(parts["state"].deficits)  # type: ignore[attr-defined]
    parts["clock"].advance(timedelta(seconds=5))  # type: ignore[attr-defined]
    second = await governor.tick()

    assert first.plan is not None and second.plan is not None
    # Единственная единица ёмкости уходит сначала одному, потом другому — очередь
    # держится на дефицитах, прочитанных из хранилища, а не на номере такта.
    assert first.plan.allocation.grants == {"c0": 1, "c1": 0}
    assert carried == {"c0": 0, "c1": 1}
    assert second.plan.allocation.grants == {"c0": 0, "c1": 1}
    assert parts["state"].deficits == {"c0": 1, "c1": 1}  # type: ignore[attr-defined]


# --- переопределения оператора ------------------------------------------------------


@pytest.mark.asyncio
async def test_pause_stops_writing_windows() -> None:
    governor, parts = build(
        free=10, demands=(100,), overrides=FakeOverrides(is_paused=True)
    )

    outcome = await governor.tick()

    assert outcome.paused is True
    assert parts["windows"].writes == []  # type: ignore[attr-defined]
    assert outcome.plan is not None
    assert outcome.plan.allocation.grants == {"c0": 10}


@pytest.mark.asyncio
async def test_pinned_consumer_is_not_written_but_spends_capacity() -> None:
    governor, parts = build(
        free=10, demands=(100, 100), overrides=FakeOverrides(pins={"c0": 2})
    )

    await governor.tick()

    assert parts["windows"].windows == {"c1": 8}  # type: ignore[attr-defined]


# --- kick и дебаунс -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_kick_runs_when_no_tick_happened_yet() -> None:
    governor, _ = build()

    assert (await governor.kick()).status is TickStatus.RAN


@pytest.mark.asyncio
async def test_kick_is_debounced() -> None:
    """Пополнение на двадцать аккаунтов приходит двадцатью событиями (G6)."""
    governor, parts = build()
    await governor.tick()

    outcomes = [await governor.kick() for _ in range(20)]

    assert {o.status for o in outcomes} == {TickStatus.DEBOUNCED}
    assert parts["scope"].entered == 1  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_kick_runs_again_after_the_interval() -> None:
    governor, parts = build()
    await governor.tick()
    parts["clock"].advance(timedelta(seconds=5))  # type: ignore[attr-defined]

    assert (await governor.kick()).status is TickStatus.RAN
    assert parts["scope"].entered == 2  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_skipped_tick_does_not_start_the_debounce_window() -> None:
    """Такт, пропущенный по лидерству, ничего не пересчитал — `kick` не глушим."""
    governor, _ = build(leading=False)
    await governor.tick()

    assert (await governor.kick()).status is TickStatus.NOT_LEADER


# --- цикл ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_ticks_until_stopped() -> None:
    governor, parts = build()
    stop = asyncio.Event()

    async def stop_soon() -> None:
        await asyncio.sleep(0.03)
        stop.set()

    await asyncio.gather(
        governor.run(every=timedelta(seconds=0.01), stop=stop), stop_soon()
    )

    assert parts["scope"].entered >= 2  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_run_propagates_errors_without_a_handler() -> None:
    """Молча глотать ошибку такта ядро не вправе."""
    governor, _ = build()

    governor.capacity_source = FailingCapacity()

    with pytest.raises(RuntimeError, match="пул недоступен"):
        await governor.run(every=timedelta(seconds=0.01), stop=asyncio.Event())


@pytest.mark.asyncio
async def test_run_survives_errors_with_a_handler() -> None:
    """Пропуск такта безопасен: окна абсолютны, следующий такт читает всё заново."""
    governor, _ = build()
    stop = asyncio.Event()
    seen: list[BaseException] = []

    async def remember(error: BaseException) -> None:
        seen.append(error)
        if len(seen) >= 2:
            stop.set()

    governor.capacity_source = FailingCapacity()
    await governor.run(every=timedelta(seconds=0.01), stop=stop, on_error=remember)

    assert len(seen) >= 2


# --- соответствие портам ------------------------------------------------------------


def test_fakes_satisfy_the_ports() -> None:
    """Подделки реализуют ровно те протоколы, против которых написан контур."""
    assert isinstance(FakeScope(), TickScope)
    assert isinstance(FakeCapacity(EligibleCapacity(eligible=1, free=1)), CapacitySource)
    assert isinstance(FakeRegistry([]), ConsumerRegistry)
    assert isinstance(FakeWindows(), WindowSink)
    assert isinstance(FakeState(), StateStore)
    assert isinstance(FakeOverrides(), OverrideStore)
    assert isinstance(FakeSupply(SupplySnapshot(effective_available=0.0)), SupplySource)
    assert isinstance(FakeReplenisher(), Replenisher)
    assert isinstance(FakeClock(NOW), Clock)

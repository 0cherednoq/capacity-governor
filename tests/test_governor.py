"""Чистая часть такта: стабилизация, `pin`, `pause`, наблюдаемость (G5–G7)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from capacity import (
    ConsumerState,
    EligibleCapacity,
    MaxMinFair,
    ReorderPoint,
    Stabilizer,
    SupplySnapshot,
    plan_tick,
)

NOW = datetime(2026, 10, 16, 12, 0, 0, tzinfo=UTC)


def cap(free: int) -> EligibleCapacity:
    return EligibleCapacity(eligible=free, free=free, effective=float(free))


def test_tick_writes_only_changed_windows() -> None:
    """Не изменившееся окно не пишется вовсе (G6)."""
    plan = plan_tick(
        now=NOW,
        capacity=cap(20),
        consumers=[ConsumerState("a", 100), ConsumerState("b", 100)],
        deficits={},
        current_windows={"a": 10, "b": 0},
        policy=MaxMinFair(),
        stabilizer=Stabilizer(),
    )

    assert plan.allocation.grants == {"a": 10, "b": 10}
    assert plan.windows == {"b": 10}


def test_pinned_consumer_window_is_untouched_but_capacity_is_spent() -> None:
    """`pin` (G7): окно пина контур не пишет, но его потребление вычитается."""
    plan = plan_tick(
        now=NOW,
        capacity=cap(10),
        consumers=[ConsumerState("a", 100), ConsumerState("b", 100)],
        deficits={},
        current_windows={"a": 3, "b": 0},
        policy=MaxMinFair(),
        stabilizer=Stabilizer(),
        pinned={"a": 3},
    )

    assert plan.allocation.grants == {"a": 3, "b": 7}
    assert "a" not in plan.windows
    assert plan.windows == {"b": 7}


def test_pin_cannot_raise_an_existing_ceiling() -> None:
    plan = plan_tick(
        now=NOW,
        capacity=cap(10),
        consumers=[ConsumerState("a", 100, ceiling=2)],
        deficits={},
        current_windows={"a": 0},
        policy=MaxMinFair(),
        stabilizer=Stabilizer(),
        pinned={"a": 9},
    )

    assert plan.allocation.grants == {"a": 2}


def test_pause_keeps_current_windows() -> None:
    """`pause` (G7): окна не пишутся, но раздача считается — иначе метрики слепы."""
    plan = plan_tick(
        now=NOW,
        capacity=cap(10),
        consumers=[ConsumerState("a", 100)],
        deficits={},
        current_windows={"a": 0},
        policy=MaxMinFair(),
        stabilizer=Stabilizer(),
        paused=True,
    )

    assert plan.windows == {}
    assert plan.allocation.grants == {"a": 10}


def test_pause_does_not_stop_replenishment() -> None:
    """Остановка заказов во время инцидента с окнами только углубила бы нехватку."""
    plan = plan_tick(
        now=NOW,
        capacity=cap(0),
        consumers=[ConsumerState("a", 100)],
        deficits={},
        current_windows={"a": 0},
        policy=MaxMinFair(),
        stabilizer=Stabilizer(),
        supply=SupplySnapshot(effective_available=0.0, needed_for_demand=5.0),
        reorder=ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=20),
        paused=True,
    )

    assert plan.order == 20
    assert plan.order_window_key == "replenish:2026-10-16T12:00:00+00:00"


def test_no_order_means_no_window_key() -> None:
    plan = plan_tick(
        now=NOW,
        capacity=cap(10),
        consumers=[ConsumerState("a", 5)],
        deficits={},
        current_windows={"a": 5},
        policy=MaxMinFair(),
        stabilizer=Stabilizer(),
        supply=SupplySnapshot(effective_available=50.0),
        reorder=ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=20),
    )

    assert (plan.order, plan.order_window_key) == (0, None)


def test_dead_zone_holds_the_window_between_ticks() -> None:
    """Ёмкость подтаяла на 4 % — окно не дёргаем (G5)."""
    stabilizer = Stabilizer(tolerance=0.1)
    consumers = [ConsumerState("a", 1000)]
    first = plan_tick(
        now=NOW,
        capacity=cap(100),
        consumers=consumers,
        deficits={},
        current_windows={},
        policy=MaxMinFair(),
        stabilizer=stabilizer,
    )
    assert first.windows == {"a": 100}

    second = plan_tick(
        now=NOW + timedelta(seconds=5),
        capacity=cap(96),
        consumers=consumers,
        deficits=first.deficits,
        current_windows={"a": 100},
        policy=MaxMinFair(),
        stabilizer=stabilizer,
    )

    assert second.windows == {}
    assert second.report.skipped_by_tolerance == 1


def test_capacity_loss_stops_work_immediately() -> None:
    """Ресурс кончился — ноль применяется немедленно, минуя мёртвую зону (G5)."""
    plan = plan_tick(
        now=NOW,
        capacity=cap(0),
        consumers=[ConsumerState("a", 1000)],
        deficits={},
        current_windows={"a": 50},
        policy=MaxMinFair(),
        stabilizer=Stabilizer(),
    )

    assert plan.windows == {"a": 0}


def test_report_carries_the_tick_observability() -> None:
    """Набор метрик такта из G6."""
    plan = plan_tick(
        now=NOW,
        capacity=EligibleCapacity(
            eligible=10, free=6, leased=3, reserved=1, effective=6.0, work_units=6
        ),
        consumers=[ConsumerState("a", 100), ConsumerState("b", 100)],
        deficits={},
        current_windows={},
        policy=MaxMinFair(),
        stabilizer=Stabilizer(),
        supply=SupplySnapshot(effective_available=6.0, ordered_in_window=5),
        reorder=ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=20),
    )
    report = plan.report

    assert report.capacity.leased == 3
    assert report.total_demand == 200
    assert report.grants == {"a": 3, "b": 3}
    assert report.window_deltas == {"a": 3, "b": 3}
    assert report.weighted_share == {"a": 0.5, "b": 0.5}
    assert report.order_budget_left == 15


def test_deficits_are_returned_for_external_storage() -> None:
    """Дефициты DRR переживают смену лидера только снаружи процесса (G6)."""
    plan = plan_tick(
        now=NOW,
        capacity=cap(1),
        consumers=[ConsumerState("a", 10), ConsumerState("b", 10)],
        deficits={},
        current_windows={"a": 0, "b": 0},
        policy=MaxMinFair(),
        stabilizer=Stabilizer(),
    )

    assert plan.deficits == {"a": 0, "b": 1}

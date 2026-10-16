"""Политика пополнения: таблица §8.2 документа плюс пограничные случаи G4."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from capacity import ReorderPoint, SupplySnapshot

CREDITS_PER_ACCOUNT = 100


def accounts(credit_units: float) -> float:
    """Таблица §8.2 выражает спрос в кредитах, политика считает в ресурсах."""
    return credit_units / CREDITS_PER_ACCOUNT


@pytest.fixture
def policy() -> ReorderPoint:
    """Параметры §8.2: target 50, порог 20 %, бюджет 20/ч."""
    return ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=20, window=timedelta(hours=1))


# --- таблица §8.2 -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("case", "available", "needed_credits", "in_progress", "expected"),
    [
        ("холодный старт, 100 доменов", 0.0, 100, 0, 20),
        ("обычный день", 35.0, 1_000, 0, 0),
        ("запас упал", 8.0, 500, 0, 20),
        ("наплыв 10 000 доменов", 30.0, 10_000, 0, 20),
        ("регистрация уже идёт", 8.0, 500, 20, 0),
    ],
)
def test_doc_examples(
    policy: ReorderPoint,
    case: str,
    available: float,
    needed_credits: int,
    in_progress: int,
    expected: int,
) -> None:
    supply = SupplySnapshot(
        effective_available=available,
        needed_for_demand=accounts(needed_credits),
        ordered_in_progress=in_progress,
    )

    assert policy.to_order(supply) == expected, case


# --- пограничные случаи -------------------------------------------------------------


def test_position_counted_once_for_both_rules() -> None:
    """Правило нехватки не заказывает повторно то, что уже заказала планка (G4)."""
    policy = ReorderPoint(target=50, reorder_ratio=0.5, max_per_window=1000)
    supply = SupplySnapshot(effective_available=8.0, needed_for_demand=30.0, ordered_in_progress=0)

    # by_level = 42, by_shortfall = 22 — берётся максимум, не сумма.
    assert policy.to_order(supply) == 42


def test_shortfall_fires_above_the_reorder_level() -> None:
    """Правило 2 работает, даже если планка не пробита."""
    policy = ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=1000)
    supply = SupplySnapshot(effective_available=30.0, needed_for_demand=100.0)

    assert policy.to_order(supply) == 70


def test_budget_is_what_is_left_in_the_window() -> None:
    policy = ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=20)
    supply = SupplySnapshot(effective_available=0.0, ordered_in_window=15)

    assert policy.to_order(supply) == 5
    assert policy.budget_left(supply) == 5


def test_exhausted_budget_orders_nothing() -> None:
    policy = ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=20)
    supply = SupplySnapshot(effective_available=0.0, ordered_in_window=20)

    assert policy.to_order(supply) == 0
    assert policy.budget_left(supply) == 0


def test_overspent_budget_does_not_go_negative() -> None:
    """Ручное пополнение (§8.4) могло выйти за бюджет — заказ просто нулевой."""
    policy = ReorderPoint(target=50, max_per_window=20)
    supply = SupplySnapshot(effective_available=0.0, ordered_in_window=25)

    assert policy.to_order(supply) == 0


def test_in_progress_orders_suppress_repeat() -> None:
    """Без учёта заказов в работе контур заказывал бы на каждом такте."""
    policy = ReorderPoint(target=50, reorder_ratio=0.5, max_per_window=20)
    first = SupplySnapshot(effective_available=5.0)
    after = SupplySnapshot(effective_available=5.0, ordered_in_progress=20, ordered_in_window=20)

    assert policy.to_order(first) == 20
    assert policy.to_order(after) == 0


def test_fractional_supply_rounds_up() -> None:
    """Ёмкость для пополнения дробная (G1.1), заказ — целый и в сторону нехватки."""
    policy = ReorderPoint(target=50, reorder_ratio=1.0, max_per_window=1000)
    supply = SupplySnapshot(effective_available=49.1)

    assert policy.to_order(supply) == 1


def test_at_target_orders_nothing() -> None:
    policy = ReorderPoint(target=50, reorder_ratio=1.0, max_per_window=1000)

    assert policy.to_order(SupplySnapshot(effective_available=50.0)) == 0


# --- окно бюджета и ключ идемпотентности --------------------------------------------


def test_window_key_is_tied_to_budget_window_start() -> None:
    """Ключ батча привязан к началу бюджетного окна, а не к минуте такта (G4)."""
    policy = ReorderPoint(target=50, window=timedelta(hours=1))
    early = datetime(2026, 10, 16, 14, 3, 7, tzinfo=UTC)
    late = datetime(2026, 10, 16, 14, 58, 59, tzinfo=UTC)
    next_window = datetime(2026, 10, 16, 15, 0, 0, tzinfo=UTC)

    assert policy.window_key(early) == policy.window_key(late)
    assert policy.window_key(early) != policy.window_key(next_window)
    assert policy.window_key(early).startswith("replenish:2026-10-16T14:00:00")


def test_window_start_does_not_depend_on_process_start() -> None:
    """Лидер меняется между тактами — граница окна обязана быть абсолютной."""
    policy = ReorderPoint(target=50, window=timedelta(minutes=15))
    at = datetime(2026, 10, 16, 14, 37, 0, tzinfo=UTC)

    assert policy.window_start(at) == datetime(2026, 10, 16, 14, 30, 0, tzinfo=UTC)


# --- валидация ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"target": 0}, "target"),
        ({"target": 50, "reorder_ratio": 0}, "reorder_ratio"),
        ({"target": 50, "reorder_ratio": 1.5}, "reorder_ratio"),
        ({"target": 50, "max_per_window": -1}, "max_per_window"),
        ({"target": 50, "window": timedelta(0)}, "window"),
    ],
)
def test_rejects_bad_parameters(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        ReorderPoint(**kwargs)  # type: ignore[arg-type]


def test_rejects_negative_snapshot() -> None:
    with pytest.raises(ValueError, match="effective_available"):
        SupplySnapshot(effective_available=-1.0)

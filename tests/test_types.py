"""Типы ядра (G1): целочисленная раздача, дробное пополнение, валидация."""

from __future__ import annotations

import pytest

from capacity import ConsumerState, EligibleCapacity, SupplySnapshot, TickReport


def test_allocatable_is_floor_of_effective_capacity() -> None:
    """Ёмкость для раздачи целая (G1.1): 4.7 эффективных аккаунта дают 4 окна."""
    assert EligibleCapacity(eligible=10, free=10, effective=4.7).allocatable == 4


def test_fraction_below_one_gives_zero_windows() -> None:
    """0.9 эффективного аккаунта — это 0 окон: пограничный случай из G1.1."""
    assert EligibleCapacity(eligible=1, free=1, effective=0.9).allocatable == 0


def test_allocatable_never_exceeds_free() -> None:
    """Кредитов может быть много, а свободных ресурсов мало — раздаём по свободным."""
    assert EligibleCapacity(eligible=10, free=2, leased=8, effective=9.5).allocatable == 2


def test_capacity_source_reports_observability_fields() -> None:
    """Порт отдаёт структуру, а не число (G1.3) — контуру нужны все четыре величины."""
    cap = EligibleCapacity(eligible=10, free=4, leased=5, reserved=1, effective=6.0)

    assert (cap.eligible, cap.free, cap.leased, cap.reserved) == (10, 4, 5, 1)


def test_headroom_is_demand_capped_by_ceiling() -> None:
    assert ConsumerState("a", demand=100).headroom == 100
    assert ConsumerState("a", demand=100, ceiling=7).headroom == 7
    assert ConsumerState("a", demand=3, ceiling=7).headroom == 3


def test_supply_position_is_stock_plus_ordered() -> None:
    supply = SupplySnapshot(effective_available=8.5, ordered_in_progress=20)

    assert supply.position == 28.5


def test_weighted_share_shows_the_skew() -> None:
    """Нормированная доля выдачи (G6): по ней сразу виден перекос."""
    report = TickReport(
        capacity=EligibleCapacity(eligible=10, free=10, effective=10.0),
        total_demand=100,
        grants={"a": 8, "b": 2},
    )

    assert report.weighted_share == {"a": 0.8, "b": 0.2}


def test_weighted_share_on_empty_tick() -> None:
    report = TickReport(
        capacity=EligibleCapacity(eligible=0, free=0),
        total_demand=0,
        grants={"a": 0},
    )

    assert report.weighted_share == {"a": 0.0}


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"demand": -1}, "demand"),
        ({"demand": 1, "weight": 0}, "weight"),
        ({"demand": 1, "guaranteed": -1}, "guaranteed"),
        ({"demand": 1, "ceiling": -1}, "ceiling"),
    ],
)
def test_consumer_validation(kwargs: dict[str, int], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        ConsumerState(consumer_id="a", **kwargs)


def test_capacity_validation() -> None:
    with pytest.raises(ValueError, match="free"):
        EligibleCapacity(eligible=1, free=-1)


def test_states_are_immutable() -> None:
    """Политика чистая — входные структуры менять нельзя."""
    state = ConsumerState("a", demand=1)
    with pytest.raises(AttributeError):
        state.demand = 2  # type: ignore[misc]

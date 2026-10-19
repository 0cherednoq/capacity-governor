"""Типы ядра (G1): целочисленная раздача, дробное пополнение, валидация."""

from __future__ import annotations

import pytest

from capacity import ConsumerState, EligibleCapacity, SupplySnapshot, TickReport


def test_allocatable_is_the_width_of_work_not_the_size_of_the_stock() -> None:
    """Кредиты ограничивают объём работы, а не её ширину.

    Десять аккаунтов, потративших часть кредитов, дают 9.5 «эффективных аккаунта», но
    параллельно работать могут все десять: запас и параллелизм — разные величины.
    """
    capacity = EligibleCapacity(eligible=10, free=10, effective=9.5, work_units=950)

    assert capacity.allocatable == 10


def test_remaining_credits_bind_only_the_tail() -> None:
    """Кредитов осталось на две единицы работы при трёх ресурсах — окон два."""
    assert EligibleCapacity(eligible=3, free=3, effective=0.02, work_units=2).allocatable == 2


def test_no_credits_left_means_no_windows() -> None:
    """Пограничный случай G1.1: работа встаёт, и `min_grant_floor` решает, давать ли шанс."""
    assert EligibleCapacity(eligible=1, free=1, effective=0.9, work_units=0).allocatable == 0


def test_allocatable_counts_busy_resources_in() -> None:
    """Окно — план на работу: занятые по уже выданным окнам ресурсы из счёта не выпадают.

    Иначе окна схлопываются ровно при полной загрузке пула, работа встаёт, ресурсы
    освобождаются, окна растут — контур автоколеблется.
    """
    capacity = EligibleCapacity(eligible=10, free=2, leased=8, effective=9.5, work_units=950)

    assert capacity.allocatable == 10


def test_allocatable_never_exceeds_eligible() -> None:
    """Кредитов много, ресурсов мало — раздаём по ресурсам."""
    assert EligibleCapacity(eligible=3, free=3, work_units=1900).allocatable == 3


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

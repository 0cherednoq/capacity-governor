"""Таблицы приёмки G2 плана: обе — и перенесённая из §7.1 документа, и добавленная."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest

from capacity import ConsumerState, MaxMinFair

MANY = 10_000


def c(cid: str, demand: int = MANY, **kw: int | None) -> ConsumerState:
    return ConsumerState(consumer_id=cid, demand=demand, **kw)  # type: ignore[arg-type]


def run(
    policy: MaxMinFair,
    capacity: int,
    consumers: Sequence[ConsumerState],
    ticks: int = 1,
    deficits: Mapping[str, int] | None = None,
) -> list[Mapping[str, int]]:
    """Прогнать `ticks` тактов, вернуть выдачу каждого такта."""
    state: Mapping[str, int] = deficits or {}
    out: list[Mapping[str, int]] = []
    for _ in range(ticks):
        allocation = policy.allocate(capacity, consumers, state)
        out.append(allocation.grants)
        state = allocation.deficits
    return out


# --- таблица §7.1 документа ---------------------------------------------------------


def test_big_demand_beside_small_one() -> None:
    """Ёмкость 30, спрос 1000 / 5: малый спрос удовлетворён целиком, остаток — большому."""
    assert run(MaxMinFair(), 30, [c("A", 1000), c("B", 5)]) == [{"A": 25, "B": 5}]


@pytest.mark.parametrize(
    ("capacity", "expected"),
    [
        (30, {"A": 10, "B": 10, "C": 10}),
        (6, {"A": 2, "B": 2, "C": 2}),
        (0, {"A": 0, "B": 0, "C": 0}),
    ],
)
def test_equal_demand_splits_evenly(capacity: int, expected: Mapping[str, int]) -> None:
    assert run(MaxMinFair(), capacity, [c("A"), c("B"), c("C")]) == [expected]


def test_capacity_two_on_three_consumers_rotates() -> None:
    """Ёмкость 2 на трёх: за три такта каждый получает ровно дважды.

    Документ приводит конкретную последовательность (1/1/0, 0/1/1, 1/0/1) — она
    свойство отброшенной реализации со сдвигом по номеру такта. DRR даёт свой порядок
    обхода; проверяем инвариант, ради которого ротация и нужна, плюс фиксируем
    фактическую последовательность, чтобы изменение алгоритма было видно.
    """
    grants = run(MaxMinFair(), 2, [c("A"), c("B"), c("C")], ticks=3)

    assert [sum(g.values()) for g in grants] == [2, 2, 2]
    served = {cid: sum(g[cid] for g in grants) for cid in "ABC"}
    assert served == {"A": 2, "B": 2, "C": 2}
    assert grants == [
        {"A": 1, "B": 1, "C": 0},
        {"A": 1, "B": 0, "C": 1},
        {"A": 0, "B": 1, "C": 1},
    ]


# --- таблица, которой нет в документе -----------------------------------------------


def test_weights_split_proportionally() -> None:
    assert run(MaxMinFair(), 3, [c("A", weight=2), c("B", weight=1)]) == [{"A": 2, "B": 1}]


def test_guarantee_does_not_create_capacity() -> None:
    """Гарантия A=5 при ёмкости 4: A получает 4, B — 0."""
    assert run(MaxMinFair(), 4, [c("A", guaranteed=5), c("B")]) == [{"A": 4, "B": 0}]


def test_ceiling_caps_grant_and_leaves_capacity_unused() -> None:
    assert run(MaxMinFair(), 100, [c("A", ceiling=3)]) == [{"A": 3}]


def test_zero_demand_gets_no_window() -> None:
    """Окно под отсутствующую работу не выдаём, сколько бы ёмкости ни было."""
    assert run(MaxMinFair(), 50, [c("A", 0), c("B", 0), c("C", 0)]) == [
        {"A": 0, "B": 0, "C": 0}
    ]


def test_hundred_consumers_capacity_one_each_served_exactly_once() -> None:
    """100 потребителей, ёмкость 1: за 100 тактов каждый получил ровно один раз."""
    consumers = [c(f"c{i:03d}") for i in range(100)]
    grants = run(MaxMinFair(), 1, consumers, ticks=100)

    assert all(sum(g.values()) == 1 for g in grants)
    served = {x.consumer_id: sum(g[x.consumer_id] for g in grants) for x in consumers}
    assert set(served.values()) == {1}


def test_idle_consumer_returns_with_capped_credit() -> None:
    """Потребитель присутствовал без спроса 1000 тактов: его кредит ограничен."""
    policy = MaxMinFair(deficit_cap_ticks=4)
    idle = c("A", 0)
    busy = c("B")
    state: Mapping[str, int] = {}
    for _ in range(1000):
        state = policy.allocate(1, [idle, busy], state).deficits

    assert state["A"] == 4  # deficit_cap_ticks * weight, а не 1000

    woken = c("A")
    grants = run(policy, 1, [woken, busy], ticks=10, deficits=state)
    # Сверх своей доли (5 из 10 тактов) A берёт не больше кредита в 4 такта.
    assert sum(g["A"] for g in grants) <= 5 + 4


def test_absent_consumer_loses_its_deficit() -> None:
    """Потребителя нет в такте — ключ удаляется (шаг 5 G2)."""
    policy = MaxMinFair()
    after = policy.allocate(1, [c("A"), c("B")], {"A": 0, "B": 0, "GONE": 99})

    assert "GONE" not in after.deficits


def test_pinned_consumer_enters_as_ceiling() -> None:
    """`pin` (G7) выражается через `ceiling`: потребление по-прежнему вычитается."""
    grants = run(MaxMinFair(), 10, [c("PINNED", ceiling=2), c("B")])

    assert grants == [{"PINNED": 2, "B": 8}]


def test_min_grant_floor_gives_one_consumer_a_chance() -> None:
    """Ёмкость 0 от floor эффективной: по умолчанию встаём, с флоором — доедаем остаток."""
    assert run(MaxMinFair(), 0, [c("A"), c("B")]) == [{"A": 0, "B": 0}]
    assert run(MaxMinFair(min_grant_floor=1), 0, [c("A"), c("B")]) == [{"A": 1, "B": 0}]


def test_allocation_is_pure() -> None:
    """Одинаковый вход — одинаковый выход, вход не мутируется."""
    policy = MaxMinFair()
    consumers = [c("A"), c("B", weight=3)]
    deficits = {"A": 2, "B": 0}
    first = policy.allocate(5, consumers, deficits)
    second = policy.allocate(5, consumers, deficits)

    assert first.grants == second.grants
    assert deficits == {"A": 2, "B": 0}


def test_grants_never_exceed_capacity() -> None:
    policy = MaxMinFair()
    consumers = [c("A", 7, weight=3), c("B", 2), c("C", 100, guaranteed=1, ceiling=4)]
    for capacity in range(0, 25):
        allocation = policy.allocate(capacity, consumers, {})
        assert allocation.total_granted <= capacity
        for x in consumers:
            assert allocation.grants[x.consumer_id] <= x.headroom


def test_rejects_duplicate_consumers() -> None:
    with pytest.raises(ValueError, match="одинаковым consumer_id"):
        MaxMinFair().allocate(1, [c("A"), c("A")], {})


def test_rejects_negative_capacity() -> None:
    with pytest.raises(ValueError, match="capacity"):
        MaxMinFair().allocate(-1, [c("A")], {})

"""`Sequential` — выделенный пул (G3)."""

from __future__ import annotations

from capacity import ConsumerState, Sequential


def test_alive_resource_gets_window_one() -> None:
    policy = Sequential()
    allocation = policy.allocate_alive([ConsumerState("camp-1", demand=500)], {"camp-1"})

    assert allocation.grants == {"camp-1": 1}


def test_dead_resource_gets_zero() -> None:
    """Текущий отправитель умер — окно 0, пока проект не переключил `current_sender_id`."""
    policy = Sequential()
    allocation = policy.allocate_alive([ConsumerState("camp-1", demand=500)], set())

    assert allocation.grants == {"camp-1": 0}


def test_no_demand_no_window() -> None:
    policy = Sequential()
    allocation = policy.allocate_alive([ConsumerState("camp-1", demand=0)], {"camp-1"})

    assert allocation.grants == {"camp-1": 0}


def test_consumers_do_not_compete() -> None:
    """Выделенный пул: кампании друг у друга ёмкость не отнимают и дефицитов нет."""
    policy = Sequential()
    allocation = policy.allocate_alive(
        [ConsumerState("a", demand=10), ConsumerState("b", demand=10), ConsumerState("c", 10)],
        {"a", "c"},
    )

    assert allocation.grants == {"a": 1, "b": 0, "c": 1}
    assert allocation.deficits == {}


def test_wider_window_is_capped_by_demand() -> None:
    policy = Sequential(window=5)
    allocation = policy.allocate_alive([ConsumerState("a", demand=2)], {"a"})

    assert allocation.grants == {"a": 2}


def test_policy_protocol_signature_is_usable() -> None:
    """Общий интерфейс `allocate` работает: ёмкость — число живых потребителей."""
    policy = Sequential()
    consumers = [ConsumerState("a", demand=5), ConsumerState("b", demand=5)]

    assert policy.allocate(1, consumers, {}).grants == {"a": 1, "b": 0}

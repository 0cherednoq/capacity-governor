"""`StaticCeiling` — статичный потолок без контура (§7.3 документа)."""

from __future__ import annotations

import pytest

from capacity import ConsumerState, StaticCeiling


def test_every_consumer_with_demand_gets_the_window() -> None:
    policy = StaticCeiling(window=1)

    allocation = policy.allocate(
        0, [ConsumerState("a", demand=100), ConsumerState("b", demand=5)], {}
    )

    assert allocation.grants == {"a": 1, "b": 1}


def test_capacity_is_ignored_on_purpose() -> None:
    """Статичный потолок не зависит от свободного ресурса — в этом отказ от контура."""
    policy = StaticCeiling(window=2)
    consumers = [ConsumerState("a", demand=100)]

    assert policy.allocate(0, consumers, {}).grants == {"a": 2}
    assert policy.allocate(1000, consumers, {}).grants == {"a": 2}


def test_no_demand_no_window() -> None:
    policy = StaticCeiling()

    assert policy.allocate(0, [ConsumerState("a", demand=0)], {}).grants == {"a": 0}


def test_consumer_ceiling_still_binds() -> None:
    policy = StaticCeiling(window=5)

    allocation = policy.allocate(0, [ConsumerState("a", demand=100, ceiling=2)], {})

    assert allocation.grants == {"a": 2}


def test_no_queue_is_kept() -> None:
    """Делить нечего, значит ни дефицитов, ни удержаний — состояние не нужно."""
    policy = StaticCeiling()

    allocation = policy.allocate(0, [ConsumerState("a", demand=1)], {"a": 5})

    assert allocation.deficits == {}
    assert allocation.holds == {}


def test_window_one_gives_implicit_round_robin() -> None:
    """Окно 1 у каждого плюс общий потолок брокера — чередование без контура (§7.3)."""
    policy = StaticCeiling(window=1)
    consumers = [ConsumerState(f"c{i}", demand=50) for i in range(5)]

    allocation = policy.allocate(0, consumers, {})

    assert allocation.total_granted == 5
    assert set(allocation.grants.values()) == {1}


def test_zero_window_stops_everyone() -> None:
    policy = StaticCeiling(window=0)

    assert policy.allocate(0, [ConsumerState("a", demand=9)], {}).grants == {"a": 0}


def test_negative_window_is_rejected() -> None:
    with pytest.raises(ValueError, match="window < 0"):
        StaticCeiling(window=-1)

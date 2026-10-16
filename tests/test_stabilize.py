"""Мёртвая зона и стабилизация (G5)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from capacity import Stabilizer

T0 = datetime(2026, 10, 16, 12, 0, 0, tzinfo=UTC)
TICK = timedelta(seconds=5)


@pytest.fixture
def stab() -> Stabilizer:
    return Stabilizer(tolerance=0.1, stabilize_down=timedelta(seconds=300))


def test_change_inside_tolerance_is_not_written(stab: Stabilizer) -> None:
    """100 → 105 это 5 % — окно не трогаем."""
    result = stab.apply(T0, {"a": 105}, {"a": 100})

    assert result.windows == {}
    assert result.skipped_by_tolerance == 1


def test_growth_outside_tolerance_applies_at_once(stab: Stabilizer) -> None:
    """Вверх реагируем сразу: работа стоит, это дорого."""
    result = stab.apply(T0, {"a": 150}, {"a": 100})

    assert result.windows == {"a": 150}


def test_shrink_is_held_until_the_window_closes(stab: Stabilizer) -> None:
    """Вниз — только когда весь период стабилизации рекомендации были низкими."""
    now = T0
    stab.apply(now, {"a": 50}, {"a": 100})

    held = stab.apply(now + TICK, {"a": 50}, {"a": 100})
    assert held.windows == {}
    assert held.held_down == 1

    late = stab.apply(now + timedelta(seconds=301), {"a": 50}, {"a": 100})
    assert late.windows == {"a": 50}


def test_a_single_spike_cancels_the_shrink(stab: Stabilizer) -> None:
    """Ёмкость мигнула вверх — уменьшение откладывается: берётся максимум за окно."""
    now = T0
    for i in range(10):
        stab.apply(now + i * TICK, {"a": 50}, {"a": 100})
    stab.apply(now + timedelta(seconds=100), {"a": 100}, {"a": 100})

    result = stab.apply(now + timedelta(seconds=301), {"a": 50}, {"a": 100})

    assert result.windows == {}


def test_zero_stops_immediately(stab: Stabilizer) -> None:
    """Ресурс кончился — исключение из мёртвой зоны и из окна стабилизации."""
    result = stab.apply(T0, {"a": 0}, {"a": 100})

    assert result.windows == {"a": 0}


def test_start_from_zero_applies_immediately(stab: Stabilizer) -> None:
    """Ресурс появился — запускаем немедленно, даже если прирост мал."""
    result = stab.apply(T0, {"a": 1}, {"a": 0})

    assert result.windows == {"a": 1}


def test_new_consumer_is_written_immediately(stab: Stabilizer) -> None:
    result = stab.apply(T0, {"a": 7}, {})

    assert result.windows == {"a": 7}


def test_vanished_consumer_is_not_written(stab: Stabilizer) -> None:
    """Потребителя больше нет — окна ему не пишем и историю не копим."""
    stab.apply(T0, {"a": 10, "b": 10}, {"a": 10, "b": 10})
    result = stab.apply(T0 + TICK, {"a": 10}, {"a": 10, "b": 10})

    assert result.windows == {}
    assert all("b" not in r.windows for r in stab.history[-1:])


def test_equal_recommendation_is_not_written(stab: Stabilizer) -> None:
    result = stab.apply(T0, {"a": 10}, {"a": 10})

    assert result.windows == {}
    assert result.skipped_by_tolerance == 0


def test_tolerance_has_an_absolute_floor_of_one(stab: Stabilizer) -> None:
    """Допуск — `max(1, 0.1 × cur)`: на больших окнах процент, на малых единица.

    При окне 3 десять процентов это 0.3, поэтому порог берётся абсолютный: изменение
    на единицу применяется, а на нуле мёртвая зона смысла не имеет.
    """
    assert stab.apply(T0, {"a": 109}, {"a": 100}).windows == {}
    assert stab.apply(T0, {"a": 111}, {"a": 100}).windows == {"a": 111}
    assert stab.apply(T0, {"a": 4}, {"a": 3}).windows == {"a": 4}


def test_fresh_leader_is_conservative() -> None:
    """Новый лидер с пустой историей откладывает уменьшение, а не применяет лишнее."""
    first = Stabilizer(stabilize_down=timedelta(seconds=300))
    for i in range(100):
        first.apply(T0 + i * TICK, {"a": 20}, {"a": 100})
    assert first.apply(T0 + timedelta(seconds=600), {"a": 20}, {"a": 100}).windows == {"a": 20}

    first.reset()
    assert first.apply(T0 + timedelta(seconds=605), {"a": 20}, {"a": 100}).windows == {}

    fresh = Stabilizer(stabilize_down=timedelta(seconds=300))
    assert fresh.apply(T0 + timedelta(seconds=600), {"a": 20}, {"a": 100}).windows == {}


def test_history_is_trimmed_to_the_window(stab: Stabilizer) -> None:
    for i in range(200):
        stab.apply(T0 + i * TICK, {"a": 10}, {"a": 10})

    assert len(stab.history) <= 300 // 5 + 1


def test_reset_clears_history(stab: Stabilizer) -> None:
    stab.apply(T0, {"a": 10}, {"a": 10})
    stab.reset()

    assert stab.history == []


@pytest.mark.parametrize("tolerance", [-0.1, 1.0, 2.0])
def test_rejects_bad_tolerance(tolerance: float) -> None:
    with pytest.raises(ValueError, match="tolerance"):
        Stabilizer(tolerance=tolerance)


def test_rejects_negative_stabilize_down() -> None:
    with pytest.raises(ValueError, match="stabilize_down"):
        Stabilizer(stabilize_down=timedelta(seconds=-1))

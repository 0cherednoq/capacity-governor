"""Приёмка прогонов G8. Политики чистые, seed фиксирован — прогоны детерминированы."""

from __future__ import annotations

from sim.model import windows_per_hour
from sim.scenarios import (
    capacity_below_consumers,
    capacity_below_consumers_no_hold,
    cold_start,
    flapping_capacity,
    half_pool_banned,
    no_dead_zone_baseline,
    ordinary_day,
    surge,
)


def test_cold_start_pulls_work_through() -> None:
    """Прогон 1: из нуля аккаунтов работа идёт, бюджет её не душит навсегда."""
    sim = cold_start()
    metrics = sim.run()

    assert metrics.orders > 0
    assert metrics.registrations_done > 0
    assert metrics.completed == 100
    # Бюджет 20/ч: первый такт заказывает ровно бюджет, не весь `target`.
    assert metrics.orders <= 20 * (sim.config.horizon / 3600)


def test_ordinary_day_orders_nothing_and_windows_stay_calm() -> None:
    """Прогон 2: заказов 0, окна не дрожат. Прямая проверка G5 и I1."""
    sim = ordinary_day()
    metrics = sim.run()

    assert metrics.orders == 0
    assert metrics.completed == 1000
    assert windows_per_hour(metrics, sim.config) < 10


def test_surge_stretches_but_does_not_fail() -> None:
    """Прогон 3: наплыв ×10 растягивается по бюджету и доходит до конца."""
    sim = surge()
    metrics = sim.run()

    assert metrics.completed == 10_000
    assert metrics.orders > 0
    assert metrics.orders <= sim.reorder.max_per_window * 12


def test_capacity_below_consumers_starves_nobody() -> None:
    """Прогон 4: ёмкость 3 на 20 потребителей — доходят все, никто не стоит вечно."""
    sim = capacity_below_consumers()
    metrics = sim.run()

    assert metrics.starved == []
    assert metrics.completed == 20 * 10
    assert all(c.remaining == 0 for c in sim.consumers.values())


def test_window_shorter_than_the_work_unit_breaks_the_loop() -> None:
    """Контрольный замер к прогону 4: без удержания окно не успевают использовать.

    Окно переезжает каждые 5 с, а проверка домена длится 20 с, поэтому потребитель
    получает окно, ставит работу и теряет окно раньше, чем работа доходит: вместо
    выполнения она уходит в `defer`. Удержание здесь не оптимизация записей, а условие
    работоспособности контура, и экономия записей — следствие.
    """
    held = capacity_below_consumers().run()
    bare = capacity_below_consumers_no_hold().run()

    assert held.completed == 200
    assert bare.completed < 20
    assert bare.deferred > 1000
    assert held.deferred == 0
    assert held.window_writes * 50 < bare.window_writes


def test_capacity_below_consumers_rotates_evenly() -> None:
    """Ротация DRR: доли тактов с выдачей у потребителей не расходятся вдвое.

    Удержание делает квант крупнее, поэтому доли расходятся заметнее, чем при выдаче
    по одному такту. Справедливость, которую видит пользователь, — это хвост закрытия
    батчей: он не должен превышать нескольких кругов раздачи, где круг — время, за
    которое окно обойдёт всех потребителей.
    """
    sim = capacity_below_consumers()
    metrics = sim.run()
    served = [metrics.served_ticks[cid] for cid in sim.consumers]
    finished = list(metrics.finished_at.values())
    hold_seconds = sim.config.min_hold_ticks * sim.config.tick
    full_round = len(sim.consumers) / metrics.peak_effective * hold_seconds

    assert min(served) > 0
    assert max(served) <= 2 * min(served)
    assert max(finished) - min(finished) <= 3 * full_round


def test_mass_ban_recovers_through_replenishment() -> None:
    """Прогон 5: окна вниз, `defer` держит работу, пополнение поднимает пул."""
    sim = half_pool_banned()
    metrics = sim.run()

    assert metrics.orders > 0
    assert metrics.registrations_done > 0
    assert metrics.deferred > 0
    assert metrics.completed == 3000


def test_dead_zone_cuts_window_writes_by_an_order_of_magnitude() -> None:
    """Прогон 6: число записей окон — метрика качества мёртвой зоны.

    Сравнение с контрольным прогоном без мёртвой зоны и выдержки: на одной и той же
    колеблющейся ёмкости записей должно быть в разы меньше при той же сделанной работе.
    """
    guarded = flapping_capacity()
    guarded_metrics = guarded.run()
    bare = no_dead_zone_baseline()
    bare_metrics = bare.run()

    assert guarded_metrics.completed == bare_metrics.completed
    assert guarded_metrics.window_writes * 5 < bare_metrics.window_writes
    assert windows_per_hour(guarded_metrics, guarded.config) < 25


def test_runs_are_deterministic() -> None:
    """Один и тот же seed — один и тот же прогон, иначе CI бесполезен."""
    first = flapping_capacity().run()
    second = flapping_capacity().run()

    assert (first.window_writes, first.completed, first.orders) == (
        second.window_writes,
        second.completed,
        second.orders,
    )

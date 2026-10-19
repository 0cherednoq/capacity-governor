"""Приёмка прогонов G8. Политики чистые, seed фиксирован — прогоны детерминированы."""

from __future__ import annotations

from sim.model import windows_per_hour
from sim.scenarios import (
    capacity_below_consumers,
    capacity_below_consumers_no_hold,
    capacity_drops,
    cold_start,
    flapping_capacity,
    half_pool_banned,
    no_dead_zone_baseline,
    ordinary_day,
    second_consumer_arrives,
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
    """Прогон 5: окна вниз, пополнение поднимает пул, работа доходит.

    Отложений здесь больше нет, и это следствие инварианта: окна режутся в том же
    такте, в котором пропала ёмкость, поэтому работа под несуществующий ресурс не
    ставится вовсе и откладывать нечего.
    """
    sim = half_pool_banned()
    metrics = sim.run()

    assert metrics.orders > 0
    assert metrics.registrations_done > 0
    assert metrics.completed == 3000
    assert metrics.deferred == 0


def test_dead_zone_cuts_window_writes_by_an_order_of_magnitude() -> None:
    """Прогон 6: число записей окон — метрика качества мёртвой зоны.

    Сравнение с контрольным прогоном без мёртвой зоны и выдержки: на одной и той же
    колеблющейся ёмкости записей меньше при той же сделанной работе.

    Выигрыш скромнее, чем был до инварианта (114 против 169, а не 9 против 114): теперь
    уменьшение окна нельзя отложить, если оно создаёт переподписку, и на колеблющейся
    ёмкости такие уменьшения — большинство. Это осознанный размен: экономия записей
    уступила тому, чтобы не выпускать работу под исчезнувший ресурс.
    """
    guarded = flapping_capacity()
    guarded_metrics = guarded.run()
    bare = no_dead_zone_baseline()
    bare_metrics = bare.run()

    assert guarded_metrics.completed == bare_metrics.completed
    assert guarded_metrics.window_writes < bare_metrics.window_writes
    assert windows_per_hour(guarded_metrics, guarded.config) < 40


def test_runs_are_deterministic() -> None:
    """Один и тот же seed — один и тот же прогон, иначе CI бесполезен."""
    first = flapping_capacity().run()
    second = flapping_capacity().run()

    assert (first.window_writes, first.completed, first.orders) == (
        second.window_writes,
        second.completed,
        second.orders,
    )


# --- инвариант ёмкости --------------------------------------------------------------


def test_no_run_oversubscribes() -> None:
    """Критерий приёмки: во всех прогонах сумма окон не превышает ёмкость."""
    runs = {
        "холодный старт": cold_start(),
        "обычные сутки": ordinary_day(),
        "наплыв": surge(),
        "ёмкость < потребителей": capacity_below_consumers(),
        "массовый бан": half_pool_banned(),
        "колеблющаяся ёмкость": flapping_capacity(),
    }
    over = {name: sim.run().oversubscription for name, sim in runs.items()}

    assert over == dict.fromkeys(runs, 0)


def test_arriving_consumer_does_not_oversubscribe() -> None:
    """Прогон 7: второй потребитель пришёл — первый ужался в том же такте."""
    sim = second_consumer_arrives()
    metrics = sim.run()

    assert metrics.oversubscription == 0
    assert metrics.completed == 4000


def test_falling_capacity_does_not_oversubscribe() -> None:
    """Прогон 8: ёмкость упала втрое — окна срезаны сразу, работа не сгорела."""
    sim = capacity_drops()
    metrics = sim.run()

    assert metrics.oversubscription == 0
    assert metrics.deferred == 0

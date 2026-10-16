"""Шесть прогонов из G8 плана. Каждый отвечает на свой вопрос о параметрах."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import timedelta

from sim.model import Consumer, SimConfig, SimMetrics, Simulation


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    question: str
    build: Callable[[], Simulation]

    def run(self) -> tuple[Simulation, SimMetrics]:
        sim = self.build()
        return sim, sim.run()


BASE = SimConfig()


def cold_start() -> Simulation:
    """Прогон 1: 0 аккаунтов, 100 доменов.

    Вопрос: вытягивает ли правило «нехватка под спрос» работу из нуля и не душит ли её
    бюджет навсегда.
    """
    config = replace(BASE, horizon=6 * 3600)
    return Simulation(config, [Consumer("batch-1", remaining=100)], accounts=0)


def ordinary_day() -> Simulation:
    """Прогон 2: обычные сутки.

    Вопрос: заказов 0 и окна не дрожат. Прямая проверка G5 и I1.
    """
    config = replace(BASE, horizon=12 * 3600)
    consumers = [
        Consumer("batch-a", remaining=400),
        Consumer("batch-b", remaining=300),
        Consumer("batch-c", remaining=300),
    ]
    return Simulation(config, consumers, accounts=35)


def surge() -> Simulation:
    """Прогон 3: наплыв ×10.

    Вопрос: работа растягивается на часы по бюджету, но не проваливается.
    """
    config = replace(BASE, horizon=12 * 3600)
    return Simulation(config, [Consumer("surge", remaining=10_000)], accounts=30)


def capacity_below_consumers() -> Simulation:
    """Прогон 4: ёмкость меньше числа потребителей.

    Вопрос: никто не голодает — проверка DRR из G2 на живой модели.
    """
    config = replace(BASE, horizon=2 * 3600)
    consumers = [Consumer(f"batch-{i:02d}", remaining=50) for i in range(20)]
    return Simulation(config, consumers, accounts=3)


def half_pool_banned() -> Simulation:
    """Прогон 5: массовый бан пула в один момент.

    Вопрос: окна уходят вниз, `defer` держит работу, пополнение поднимает пул, работа
    продолжается. Сценарий §13 документа в масштабе.

    Бан именно половины пула (как в формулировке G8) при `target = 50` и пороге 20 %
    проходит вообще без пополнения: 20 живых аккаунтов выше планки 10 и покрывают спрос
    в 16 эффективных аккаунтов. Это сам по себе полезный результат, но он проверяет не
    то, ради чего прогон задуман. Поэтому здесь банится 80 % — так выполняются оба
    условия заказа, и видно всю цепочку: окна вниз, `defer`, пополнение, продолжение.
    """
    config = replace(BASE, horizon=8 * 3600)
    consumers = [Consumer("batch-a", remaining=1500), Consumer("batch-b", remaining=1500)]
    return Simulation(
        config,
        consumers,
        accounts=40,
        events=[(1800.0, lambda sim: sim.ban(0.8))],
    )


def flapping_capacity() -> Simulation:
    """Прогон 6: колеблющаяся ёмкость (аккаунты входят и выходят из COOLING).

    Вопрос: число записей окон за прогон — метрика качества мёртвой зоны.
    """
    config = replace(BASE, horizon=4 * 3600)
    consumers = [Consumer("batch-a", remaining=2000), Consumer("batch-b", remaining=2000)]
    events: list[tuple[float, Callable[[Simulation], None]]] = [
        (float(60 * minute), lambda sim: sim.flap(count=4, cooling=90.0))
        for minute in range(1, 240, 2)
    ]
    return Simulation(config, consumers, accounts=30, events=events)


def no_dead_zone_baseline() -> Simulation:
    """Контрольный прогон к шестому: та же нагрузка без мёртвой зоны и выдержки.

    Нужен, чтобы «записей окон мало» было утверждением с масштабом, а не на глаз.
    """
    config = replace(
        BASE,
        horizon=4 * 3600,
        tolerance=0.0,
        stabilize_down=timedelta(0),
    )
    consumers = [Consumer("batch-a", remaining=2000), Consumer("batch-b", remaining=2000)]
    events: list[tuple[float, Callable[[Simulation], None]]] = [
        (float(60 * minute), lambda sim: sim.flap(count=4, cooling=90.0))
        for minute in range(1, 240, 2)
    ]
    return Simulation(config, consumers, accounts=30, events=events)


SCENARIOS: tuple[Scenario, ...] = (
    Scenario("1. холодный старт", "правило нехватки вытягивает работу из нуля", cold_start),
    Scenario("2. обычные сутки", "заказов 0, окна не дрожат", ordinary_day),
    Scenario("3. наплыв ×10", "растягивается по бюджету, не проваливается", surge),
    Scenario("4. ёмкость < потребителей", "никто не голодает (DRR)", capacity_below_consumers),
    Scenario("5. массовый бан пула", "defer держит, пополнение поднимает", half_pool_banned),
    Scenario("6. колеблющаяся ёмкость", "записей окон мало (мёртвая зона)", flapping_capacity),
    Scenario("6б. без мёртвой зоны", "контрольный замер к прогону 6", no_dead_zone_baseline),
)

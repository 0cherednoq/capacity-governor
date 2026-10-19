"""Шесть прогонов из G8 плана. Каждый отвечает на свой вопрос о параметрах."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import timedelta

from sim.model import Consumer, SimConfig, SimMetrics, Simulation, min_hold_for


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    question: str
    build: Callable[[], Simulation]

    def run(self) -> tuple[Simulation, SimMetrics]:
        sim = self.build()
        return sim, sim.run()


_DEFAULTS = SimConfig()
# Удержание окна по правилу из `min_hold_for`: при такте 5 с и проверке 20 с это 5
# тактов. Там, где ёмкости хватает всем, оно ни на что не влияет; там, где ёмкость
# меньше числа потребителей, без него контур не работает вовсе (см. журнал, S11).
BASE = replace(_DEFAULTS, min_hold_ticks=min_hold_for(_DEFAULTS))


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


def _scarce_capacity(min_hold_ticks: int) -> Simulation:
    """Три аккаунта на двадцать потребителей, пополнение выключено.

    Пополнение выключено намеренно (`max_per_window = 0`): правило нехватки под спрос
    заказывает двадцать аккаунтов на первом же такте, через четверть часа ёмкости
    хватает всем, и прогон перестаёт проверять то, ради чего он есть. Работы взято
    ровно столько, сколько покрывают кредиты трёх аккаунтов.
    """
    config = replace(
        BASE, horizon=4 * 3600, max_per_window=0, min_hold_ticks=min_hold_ticks
    )
    consumers = [Consumer(f"batch-{i:02d}", remaining=10) for i in range(20)]
    return Simulation(config, consumers, accounts=3)


def capacity_below_consumers() -> Simulation:
    """Прогон 4: ёмкость меньше числа потребителей.

    Вопрос: никто не голодает — проверка DRR из G2 на живой модели.

    Удержание здесь не украшение, а условие работоспособности: единица работы длится
    20 с при такте 5 с, и окно, которое переезжает каждый такт, потребитель просто не
    успевает использовать — работа уходит в `defer`, а не выполняется. Величина берётся
    общим правилом `min_hold_for`, подобранным прогоном `capacity_below_consumers_sweep`.
    """
    return _scarce_capacity(min_hold_ticks=BASE.min_hold_ticks)


def capacity_below_consumers_no_hold() -> Simulation:
    """Контрольный прогон к четвёртому: та же нехватка без удержания окна."""
    return _scarce_capacity(min_hold_ticks=1)


def capacity_below_consumers_sweep() -> list[tuple[int, Simulation]]:
    """Развёртка по величине удержания: по ней и выбрано значение для прогона 4."""
    return [(hold, _scarce_capacity(hold)) for hold in (1, 2, 3, 4, 5, 6, 8)]


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


def second_consumer_arrives() -> Simulation:
    """Прогон 7: к работающему потребителю приходит второй.

    Вопрос инварианта: окно первого обязано ужаться немедленно. Пока уменьшение
    откладывалось «ради экономии записей», сумма окон держалась в полтора раза выше
    ёмкости несколько минут — это работа, выпущенная под несуществующий ресурс.
    """
    config = replace(BASE, horizon=2 * 3600)
    consumers = [Consumer("batch-a", remaining=2000)]
    events: list[tuple[float, Callable[[Simulation], None]]] = [
        (300.0, lambda sim: sim.add_consumer(Consumer("batch-b", remaining=2000)))
    ]
    return Simulation(config, consumers, accounts=30, events=events)


def capacity_drops() -> Simulation:
    """Прогон 8: ёмкость падает втрое на ходу.

    Тот же вопрос с другой стороны: ресурса стало меньше, а окна выставлены под
    прежний объём. Инвариант обязан срезать их в том же такте.
    """
    config = replace(BASE, horizon=2 * 3600, max_per_window=0)
    consumers = [Consumer("batch-a", remaining=1500), Consumer("batch-b", remaining=1500)]
    events: list[tuple[float, Callable[[Simulation], None]]] = [
        (600.0, lambda sim: sim.ban(2 / 3))
    ]
    return Simulation(config, consumers, accounts=30, events=events)


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
    Scenario(
        "4б. то же без удержания",
        "контрольный замер к прогону 4",
        capacity_below_consumers_no_hold,
    ),
    Scenario("5. массовый бан пула", "defer держит, пополнение поднимает", half_pool_banned),
    Scenario(
        "7. приходит второй потребитель",
        "окно первого ужимается сразу, переподписки нет",
        second_consumer_arrives,
    ),
    Scenario(
        "8. ёмкость падает втрое",
        "окна режутся в том же такте",
        capacity_drops,
    ),
    Scenario("6. колеблющаяся ёмкость", "записей окон мало (мёртвая зона)", flapping_capacity),
    Scenario("6б. без мёртвой зоны", "контрольный замер к прогону 6", no_dead_zone_baseline),
)

"""Типы ядра: только стандартная библиотека, никаких импортов проекта (фаза A, G0)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ConsumerState:
    """Потребитель ёмкости в текущем такте.

    `ceiling`, а не `max` — встроенное имя затенять нельзя (G1.2).
    """

    consumer_id: str
    demand: int
    weight: int = 1
    guaranteed: int = 0
    ceiling: int | None = None

    def __post_init__(self) -> None:
        if self.demand < 0:
            raise ValueError(f"demand < 0 у {self.consumer_id!r}")
        if self.weight < 1:
            raise ValueError(f"weight < 1 у {self.consumer_id!r}")
        if self.guaranteed < 0:
            raise ValueError(f"guaranteed < 0 у {self.consumer_id!r}")
        if self.ceiling is not None and self.ceiling < 0:
            raise ValueError(f"ceiling < 0 у {self.consumer_id!r}")

    @property
    def headroom(self) -> int:
        """Сколько потребитель способен принять: спрос, урезанный потолком (G2, шаг 1)."""
        if self.ceiling is None:
            return self.demand
        return min(self.demand, self.ceiling)


@dataclass(frozen=True, slots=True)
class Hold:
    """Удержание выдачи: сколько ещё тактов за потребителем закреплён его объём.

    Нужно, чтобы окно, которое только что выдали, не отобрали раньше, чем потребитель
    успеет им воспользоваться: единица работы длится дольше такта контура.
    """

    amount: int
    ticks_left: int

    def __post_init__(self) -> None:
        if self.amount < 0:
            raise ValueError("amount < 0")
        if self.ticks_left < 0:
            raise ValueError("ticks_left < 0")


@dataclass(frozen=True, slots=True)
class Allocation:
    """Результат такта: выдача и состояние, переносимое в следующий такт."""

    grants: Mapping[str, int]
    deficits: Mapping[str, int]
    holds: Mapping[str, Hold] = field(default_factory=dict)

    @property
    def total_granted(self) -> int:
        return sum(self.grants.values())


@dataclass(frozen=True, slots=True)
class EligibleCapacity:
    """Что отдаёт порт ёмкости: структура для наблюдаемости, а не одно число (G1.3).

    `allocatable` — целое: это окна, floor уже применён (G1.1).
    """

    eligible: int
    free: int
    leased: int = 0
    reserved: int = 0
    effective: float = 0.0
    work_units: int | None = None

    def __post_init__(self) -> None:
        for name in ("eligible", "free", "leased", "reserved"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} < 0")
        if self.effective < 0:
            raise ValueError("effective < 0")
        if self.work_units is not None and self.work_units < 0:
            raise ValueError("work_units < 0")

    @property
    def allocatable(self) -> int:
        """Ёмкость для раздачи: сколько работы может идти параллельно прямо сейчас.

        Две вещи, которые план сводит в одну величину, здесь разделены, и обе правки
        подтверждены прогоном 4 симуляции.

        **Считается по `eligible`, а не по `free`.** Окно — это план на работу, а не
        захват ресурса сейчас: ресурсы, занятые работой из уже выданных окон, лежат в
        `leased` и из `free` вычтены. Раздача по `free` схлопывает окна ровно в тот
        момент, когда пул загружен полностью: работа встаёт, ресурсы освобождаются,
        окна снова растут — контур автоколеблется. В прогоне 4 ёмкость была нулевой в
        202 тактах из 720 при трёх живых аккаунтах.

        **Ограничивает `work_units`, а не `floor(effective)`.** «Эффективные аккаунты»
        (сумма кредитов, делённая на кредиты нового аккаунта) — мера запаса, она нужна
        политике пополнения и живёт в `SupplySnapshot.effective_available`. Параллелизм
        она измерять не может: три аккаунта, потратившие по одному кредиту из ста, дают
        2.97 эффективного аккаунта и `floor` в 2 окна, хотя работать могут все три.
        Кредиты ограничивают объём работы, а не её ширину, поэтому ограничение —
        `work_units`: сколько единиц работы покрывает остаток кредитов. Оно связывает
        только хвост, когда кредитов осталось меньше, чем ресурсов.

        **Резерв вычитается.** `reserved` — часть пула, которую его владелец держит
        под другие нужды: раздать её значит выдать окна под ресурсы, которых контуру
        не дадут. `free` и `leased` остаются величинами наблюдаемости (G1.3) — по ним
        строится график загрузки пула.
        """
        available = max(0, self.eligible - self.reserved)
        if self.work_units is None:
            return available
        return min(available, self.work_units)


@dataclass(frozen=True, slots=True)
class SupplySnapshot:
    """Вход политики пополнения. Ёмкость здесь дробная (G1.1)."""

    effective_available: float
    needed_for_demand: float = 0.0
    ordered_in_progress: int = 0
    ordered_in_window: int = 0

    def __post_init__(self) -> None:
        if self.effective_available < 0:
            raise ValueError("effective_available < 0")
        if self.needed_for_demand < 0:
            raise ValueError("needed_for_demand < 0")
        if self.ordered_in_progress < 0:
            raise ValueError("ordered_in_progress < 0")
        if self.ordered_in_window < 0:
            raise ValueError("ordered_in_window < 0")

    @property
    def position(self) -> float:
        """Инвентарная позиция: запас плюс заказанное (G4) — считается один раз."""
        return self.effective_available + self.ordered_in_progress


@dataclass(frozen=True, slots=True)
class TickReport:
    """Наблюдаемость такта (G6): то, что уходит в метрики и график админки."""

    capacity: EligibleCapacity
    total_demand: int
    grants: Mapping[str, int]
    window_deltas: Mapping[str, int] = field(default_factory=dict)
    skipped_by_tolerance: int = 0
    oversubscribed: int = 0
    """Сумма окон сверх ёмкости. В норме ноль: больше ресурса, чем есть, не раздаётся."""
    ordered: int = 0
    order_budget_left: int = 0

    @property
    def weighted_share(self) -> Mapping[str, float]:
        """Нормированная доля выдачи — по ней сразу виден перекос (G6, Kueue)."""
        total = sum(self.grants.values())
        if total == 0:
            return dict.fromkeys(self.grants, 0.0)
        return {cid: g / total for cid, g in self.grants.items()}

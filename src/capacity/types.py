"""Типы ядра: только стандартная библиотека, никаких импортов проекта (фаза A, G0)."""

from __future__ import annotations

import math
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
class Allocation:
    """Результат такта: выдача и дефициты, переносимые в следующий такт."""

    grants: Mapping[str, int]
    deficits: Mapping[str, int]

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

    def __post_init__(self) -> None:
        for name in ("eligible", "free", "leased", "reserved"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} < 0")
        if self.effective < 0:
            raise ValueError("effective < 0")

    @property
    def allocatable(self) -> int:
        """Ёмкость для раздачи: floor от эффективной, но не больше свободной."""
        return min(self.free, math.floor(self.effective)) if self.effective else self.free


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
    ordered: int = 0
    order_budget_left: int = 0

    @property
    def weighted_share(self) -> Mapping[str, float]:
        """Нормированная доля выдачи — по ней сразу виден перекос (G6, Kueue)."""
        total = sum(self.grants.values())
        if total == 0:
            return dict.fromkeys(self.grants, 0.0)
        return {cid: g / total for cid, g in self.grants.items()}

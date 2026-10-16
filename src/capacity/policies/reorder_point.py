"""Политика пополнения: (s, S) + нехватка под спрос + бюджет окна (G4).

Три правила: планка `(s, S)` (упало ниже `reorder_ratio * target` — дозаказать до
`target`), нехватка под спрос (запаса меньше, чем требует текущий спрос — дозаказать
разницу) и бюджет окна (не больше `max_per_window` за окно).

`position` — инвентарная позиция, запас плюс уже заказанное. Считается один раз и
входит в оба правила: иначе второе правило закажет повторно то, что уже заказано
первым.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from capacity.types import SupplySnapshot


@dataclass(frozen=True, slots=True)
class ReorderPoint:
    """Сколько ресурса заказать сейчас.

    `window` — окно бюджета. Его начало даёт `window_key()`: ключ батча обязан быть
    привязан к началу **бюджетного** окна, иначе `max_per_window` считается по одному
    окну, а дедупликация идёт по другому (G4).
    """

    target: float
    reorder_ratio: float = 0.5
    max_per_window: int = 10
    window: timedelta = timedelta(hours=1)

    def __post_init__(self) -> None:
        if self.target <= 0:
            raise ValueError("target <= 0")
        if not 0 < self.reorder_ratio <= 1:
            raise ValueError("reorder_ratio вне (0, 1]")
        if self.max_per_window < 0:
            raise ValueError("max_per_window < 0")
        if self.window <= timedelta(0):
            raise ValueError("window <= 0")

    def to_order(self, supply: SupplySnapshot) -> int:
        position = supply.position
        by_level = (
            math.ceil(self.target - position) if position < self.reorder_ratio * self.target else 0
        )
        by_shortfall = max(0, math.ceil(supply.needed_for_demand - position))
        want = max(by_level, by_shortfall)
        budget_left = self.max_per_window - supply.ordered_in_window
        return max(0, min(want, budget_left))

    def budget_left(self, supply: SupplySnapshot) -> int:
        return max(0, self.max_per_window - supply.ordered_in_window)

    def window_start(self, now: datetime) -> datetime:
        """Начало бюджетного окна, в котором лежит `now`.

        Окна выкладываются от эпохи того же часового пояса, чтобы граница не зависела
        от момента запуска процесса: лидер меняется между тактами.
        """
        epoch = datetime(1970, 1, 1, tzinfo=now.tzinfo)
        elapsed = (now - epoch) // self.window
        return epoch + elapsed * self.window

    def window_key(self, now: datetime) -> str:
        """Ключ батча пополнения: `replenish:{window_start}` (G4, идемпотентность)."""
        return f"replenish:{self.window_start(now).isoformat()}"

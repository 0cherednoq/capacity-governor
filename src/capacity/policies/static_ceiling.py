"""Статичный потолок без контура (§7.3 документа).

Для ресурсов, чья ёмкость не меняется: потолок задан настройкой брокера или пула, и
пересчитывать его нечего. Контур такому ресурсу не нужен вовсе — окна выставляются
один раз.

Политика здесь существует не для контура, а чтобы у проекта был один способ считать
окно для всех видов ресурсов: подменив `MaxMinFair` на `StaticCeiling`, он выключает
динамику, не меняя код применения окон.

Отдельное свойство, которое документ называет прямо: при окне 1 у каждого потребителя
и общем потолке получается неявный round robin — у каждого в очереди не больше одной
задачи, и брокер сам чередует их.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from capacity.types import Allocation, ConsumerState, Hold


@dataclass(frozen=True, slots=True)
class StaticCeiling:
    """Одинаковое окно каждому, кто просит работу.

    `window` — сколько выдать потребителю со спросом. Потолок потребителя (`ceiling`)
    и его спрос по-прежнему уважаются: окно под отсутствующую работу не выдаётся.

    `capacity` в `allocate` игнорируется: статичный потолок не зависит от того, сколько
    ресурса свободно сейчас, — в этом и смысл отказа от контура.
    """

    window: int = 1

    def __post_init__(self) -> None:
        if self.window < 0:
            raise ValueError("window < 0")

    def allocate(
        self,
        capacity: int,
        consumers: Sequence[ConsumerState],
        deficits: Mapping[str, int],
        holds: Mapping[str, Hold] | None = None,
    ) -> Allocation:
        grants = {c.consumer_id: min(self.window, c.headroom) for c in consumers}
        # Ни очереди, ни удержаний: ёмкость не дефицитна, делить её не нужно.
        return Allocation(grants=grants, deficits={})

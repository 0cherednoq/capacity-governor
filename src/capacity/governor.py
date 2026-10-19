"""Чистая часть такта контура (G6, G7).

Контур целиком — лидер через `pg_try_advisory_xact_lock`, транзакция такта, запись окон
и постановка заказа — лежит за портами и пишется в проекте (шаги S7/S8 журнала).
Здесь живёт то, что от инфраструктуры не зависит и потому проверяется таблицами:
как из ёмкости, спроса, текущих окон и переопределений оператора получается
решение такта.

Порядок внутри такта взят из G6: сначала раздача и окна, заказ пополнения — после них
(это уже не чтение, а постановка работы, и в контуре он идёт своей транзакцией).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime

from capacity.policies.reorder_point import ReorderPoint
from capacity.ports import AllocationPolicy
from capacity.stabilize import Stabilizer
from capacity.types import (
    Allocation,
    ConsumerState,
    EligibleCapacity,
    Hold,
    SupplySnapshot,
    TickReport,
)


@dataclass(frozen=True, slots=True)
class TickPlan:
    """Что контур должен сделать в этом такте.

    `windows` — только изменившиеся окна: не изменившееся не пишется вовсе.
    `deficits` и `holds` сохраняются вне памяти процесса: лидер меняется между тактами.
    """

    windows: Mapping[str, int]
    deficits: Mapping[str, int]
    holds: Mapping[str, Hold]
    allocation: Allocation
    order: int
    order_window_key: str | None
    report: TickReport


def plan_tick(
    *,
    now: datetime,
    capacity: EligibleCapacity,
    consumers: Sequence[ConsumerState],
    deficits: Mapping[str, int],
    current_windows: Mapping[str, int],
    policy: AllocationPolicy,
    stabilizer: Stabilizer,
    holds: Mapping[str, Hold] | None = None,
    supply: SupplySnapshot | None = None,
    reorder: ReorderPoint | None = None,
    pinned: Mapping[str, int] | None = None,
    paused: bool = False,
) -> TickPlan:
    """Посчитать решение такта, ничего не записывая.

    `pinned` — ручное переопределение (G7): окно такого потребителя контур не трогает,
    но в политику он входит с `ceiling = limit`, чтобы его потребление по-прежнему
    вычиталось из ёмкости.

    `paused` — контур перестаёт писать окна вовсе, оставляя текущие значения. Раздача
    при этом считается: иначе по метрикам нельзя отличить «контур на паузе» от
    «политика ничего не выдаёт». Пополнение пауза не затрагивает: это отдельный контур,
    и остановка заказов во время инцидента с окнами только углубила бы нехватку.
    """
    pins = dict(pinned or {})
    effective = [
        replace(c, ceiling=_narrow(c.ceiling, pins[c.consumer_id]))
        if c.consumer_id in pins
        else c
        for c in consumers
    ]

    allocation = policy.allocate(capacity.allocatable, effective, deficits, holds)

    recommended = {
        cid: grant for cid, grant in allocation.grants.items() if cid not in pins
    }
    # Пины ёмкость расходуют, хотя их окна контур не пишет: их доля вычитается из
    # бюджета, иначе отложенные уменьшения разойдутся с тем, что есть на самом деле.
    spent_by_pins = sum(allocation.grants.get(cid, 0) for cid in pins)
    stabilized = stabilizer.apply(
        now,
        recommended,
        current_windows,
        capacity=capacity.allocatable,
        spent_elsewhere=spent_by_pins,
    )
    windows: Mapping[str, int] = {} if paused else stabilized.windows

    order = 0
    window_key: str | None = None
    budget_left = 0
    if reorder is not None and supply is not None:
        order = reorder.to_order(supply)
        budget_left = reorder.budget_left(supply)
        window_key = reorder.window_key(now) if order > 0 else None

    report = TickReport(
        capacity=capacity,
        total_demand=sum(c.demand for c in consumers),
        grants=allocation.grants,
        window_deltas={
            cid: limit - current_windows.get(cid, 0) for cid, limit in windows.items()
        },
        skipped_by_tolerance=stabilized.skipped_by_tolerance + stabilized.held_down,
        oversubscribed=stabilized.oversubscribed,
        ordered=order,
        order_budget_left=budget_left,
    )
    return TickPlan(
        windows=windows,
        deficits=allocation.deficits,
        holds=allocation.holds,
        allocation=allocation,
        order=order,
        order_window_key=window_key,
        report=report,
    )


def _narrow(ceiling: int | None, pin: int) -> int:
    """Пин не может поднять потолок выше того, что потребитель уже имел."""
    return pin if ceiling is None else min(ceiling, pin)

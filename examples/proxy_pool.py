"""Общий пул прокси между скраперами: веса и справедливость.

Ресурс: 12 рабочих прокси. Потребители: три задания скрапинга, которые просят больше,
чем есть. Приоритет задан весами — срочное задание получает вдвое больше остальных,
но не вытесняет их полностью.

Что показывает прогон: ёмкость делится по весам, а когда срочное задание заканчивает
работу, его доля уходит остальным без вмешательства.

    python -m examples.proxy_pool
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from capacity import ConsumerState, Governor, MaxMinFair, Stabilizer
from capacity_adapters.memory import (
    MemoryCapacity,
    MemoryClock,
    MemoryOverrides,
    MemoryRegistry,
    MemoryState,
    MemoryTickScope,
    MemoryWindows,
)

TICK = timedelta(seconds=5)


def build() -> tuple[Governor, MemoryCapacity, MemoryRegistry, MemoryWindows, MemoryClock]:
    capacity = MemoryCapacity(eligible=12)
    registry = MemoryRegistry(
        [
            # `weight` — во сколько раз быстрее заполняется доля потребителя.
            ConsumerState("urgent-scrape", demand=500, weight=2),
            ConsumerState("daily-scrape", demand=500, weight=1),
            ConsumerState("backfill", demand=500, weight=1),
        ]
    )
    windows = MemoryWindows()
    clock = MemoryClock()
    governor = Governor(
        scope=MemoryTickScope(),
        capacity_source=capacity,
        registry=registry,
        windows=windows,
        state=MemoryState(),
        policy=MaxMinFair(),
        clock=clock,
        stabilizer=Stabilizer(),
        overrides=MemoryOverrides(),
        min_tick_interval=TICK,
    )
    return governor, capacity, registry, windows, clock


async def main() -> None:
    governor, capacity, registry, windows, clock = build()

    first = await governor.tick()
    assert first.plan is not None
    print("по весам 2:1:1 из 12 прокси →", dict(first.plan.allocation.grants))

    # Срочное задание закончилось: спроса нет, его доля освобождается.
    registry.set_demand("urgent-scrape", 0)
    clock.advance(TICK.total_seconds())
    second = await governor.tick()
    assert second.plan is not None
    print("срочное закончило, доля ушла остальным →", dict(second.plan.allocation.grants))

    # Половина прокси забанена: окна уезжают вниз в ту же пропорцию.
    capacity.eligible = 6
    clock.advance(TICK.total_seconds() + 301)
    third = await governor.tick()
    assert third.plan is not None
    print("осталось 6 прокси →", dict(third.plan.allocation.grants))
    print("записей окон за три такта:", len(windows.writes))


if __name__ == "__main__":
    asyncio.run(main())

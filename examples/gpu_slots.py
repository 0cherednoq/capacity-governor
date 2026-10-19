"""Слоты GPU между командами: гарантированный минимум и потолок.

Ресурс: 8 слотов инференса. Потребители: продовый сервис, которому нельзя упасть ниже
двух слотов; исследовательская очередь без гарантий; batch-переобучение, которому
больше трёх слотов отдавать незачем — оно всё равно упрётся в диск.

Что показывает прогон: гарантия выдаётся первой, потолок не позволяет забрать больше
нужного, а когда ресурса меньше гарантии — гарантия урезается, потому что политика
ёмкость не создаёт.

    python -m examples.gpu_slots
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from capacity import ConsumerState, Governor, MaxMinFair, Stabilizer
from capacity_adapters.memory import (
    MemoryCapacity,
    MemoryClock,
    MemoryRegistry,
    MemoryState,
    MemoryTickScope,
    MemoryWindows,
)

TICK = timedelta(seconds=5)


def build() -> tuple[Governor, MemoryCapacity, MemoryClock]:
    capacity = MemoryCapacity(eligible=8)
    registry = MemoryRegistry(
        [
            # `guaranteed` — минимум, который выдаётся раньше всякой дележки.
            ConsumerState("prod-inference", demand=100, guaranteed=2),
            ConsumerState("research-queue", demand=100),
            # `ceiling` — потолок: больше трёх слотов этому потребителю не нужно.
            ConsumerState("batch-retrain", demand=100, ceiling=3),
        ]
    )
    clock = MemoryClock()
    governor = Governor(
        scope=MemoryTickScope(),
        capacity_source=capacity,
        registry=registry,
        windows=MemoryWindows(),
        state=MemoryState(),
        policy=MaxMinFair(),
        clock=clock,
        stabilizer=Stabilizer(),
        min_tick_interval=TICK,
    )
    return governor, capacity, clock


async def main() -> None:
    governor, capacity, clock = build()

    full = await governor.tick()
    assert full.plan is not None
    print("8 слотов, гарантия 2 и потолок 3 →", dict(full.plan.allocation.grants))

    # Половина карт в обслуживании.
    capacity.eligible = 4
    clock.advance(TICK.total_seconds() + 301)
    scarce = await governor.tick()
    assert scarce.plan is not None
    print("осталось 4 слота →", dict(scarce.plan.allocation.grants))

    # Остался один слот: гарантия в два слота физически невыполнима.
    capacity.eligible = 1
    clock.advance(TICK.total_seconds() + 301)
    last = await governor.tick()
    assert last.plan is not None
    print("остался 1 слот, гарантия не создаёт ресурс →", dict(last.plan.allocation.grants))


if __name__ == "__main__":
    asyncio.run(main())

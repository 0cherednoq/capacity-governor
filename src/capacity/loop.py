"""Контур: такт, лидер, применение, `kick` с дебаунсом (G6).

Здесь собирается всё, что есть: порты, чистый `plan_tick` и порядок действий внутри
такта. В проекте остаётся написать реализации портов — транзакцию с
`pg_try_advisory_xact_lock`, чтение ёмкости у Identity Hub, спрос и окна у Tallyho.

Порядок внутри такта взят из G6 и важен целиком:

1. лидерство и транзакция — на каждый такт, не как состояние;
2. чтение ёмкости, спроса, текущих окон и переопределений оператора;
3. раздача и стабилизация — чистая функция, без обращений наружу;
4. запись окон, абсолютных и одной транзакцией; не изменившееся не пишется;
5. сохранение дефицитов и удержаний — тем же коммитом, что и окна;
6. заказ пополнения — **после** коммита такта и в своей транзакции: это постановка
   работы, а не чтение, и она не должна держать лок лидера.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from capacity.governor import TickPlan, plan_tick
from capacity.policies.reorder_point import ReorderPoint
from capacity.ports import (
    AllocationPolicy,
    CapacitySource,
    Clock,
    ConsumerRegistry,
    OverrideStore,
    Replenisher,
    StateStore,
    SupplySource,
    TickObserver,
    TickScope,
    WindowSink,
)
from capacity.stabilize import Stabilizer


class TickStatus(Enum):
    """Чем кончился такт."""

    RAN = "ran"
    NOT_LEADER = "not_leader"
    DEBOUNCED = "debounced"


@dataclass(frozen=True, slots=True)
class TickOutcome:
    status: TickStatus
    plan: TickPlan | None = None
    paused: bool = False

    @property
    def ran(self) -> bool:
        return self.status is TickStatus.RAN


@dataclass(slots=True)
class Governor:
    """Контур распределения ёмкости.

    Лидерства как состояния нет: лок берётся на каждый такт внутри его транзакции.
    Не получили — такт пропущен, следующий через `every`. Значит нет ни расщепления
    лидера, ни stale-лидера, ни healthcheck'а соединения; цена — такт не может быть
    длиннее транзакции, и он не должен: это три чтения и один UPDATE.
    """

    scope: TickScope
    capacity_source: CapacitySource
    registry: ConsumerRegistry
    windows: WindowSink
    state: StateStore
    policy: AllocationPolicy
    clock: Clock
    stabilizer: Stabilizer = field(default_factory=Stabilizer)
    overrides: OverrideStore | None = None
    supply: SupplySource | None = None
    reorder: ReorderPoint | None = None
    replenisher: Replenisher | None = None
    observer: TickObserver | None = None
    min_tick_interval: timedelta = timedelta(seconds=1)
    last_tick_at: datetime | None = field(default=None, init=False)

    async def tick(self) -> TickOutcome:
        """Один такт целиком. Повтор такта ничего не ломает: окна абсолютны."""
        plan: TickPlan | None = None
        paused = False

        async with self.scope.lead() as leading:
            if not leading:
                return TickOutcome(status=TickStatus.NOT_LEADER)

            now = self.clock.now()
            # Чтения идут последовательно: они в одной транзакции, а одна сессия
            # параллельных запросов не допускает — `gather` здесь был бы ошибкой.
            capacity = await self.capacity_source.capacity()
            consumers = await self.registry.consumers()
            current_windows = await self.windows.current_windows()
            deficits, holds = await self.state.load()
            pinned = await self.overrides.pinned() if self.overrides else {}
            paused = await self.overrides.paused() if self.overrides else False
            supply = await self.supply.snapshot() if self.supply else None

            plan = plan_tick(
                now=now,
                capacity=capacity,
                consumers=consumers,
                deficits=deficits,
                holds=holds,
                current_windows=current_windows,
                policy=self.policy,
                stabilizer=self.stabilizer,
                supply=supply,
                reorder=self.reorder,
                pinned=pinned,
                paused=paused,
            )

            if plan.windows:
                await self.windows.set_windows(plan.windows)
            await self.state.save(plan.deficits, plan.holds)
            self.last_tick_at = now

        # Дальше — уже вне транзакции такта и вне лидерского лока.
        if plan is None:  # pragma: no cover - ветка без лидерства вернулась выше
            return TickOutcome(status=TickStatus.NOT_LEADER)
        if plan.order > 0 and plan.order_window_key and self.replenisher is not None:
            await self.replenisher.order(plan.order, plan.order_window_key)
        if self.observer is not None:
            await self.observer.observe(plan.report)
        return TickOutcome(status=TickStatus.RAN, plan=plan, paused=paused)

    async def kick(self) -> TickOutcome:
        """Пересчитать сейчас — по событию ёмкости или проекта.

        Дебаунс обязателен: пополнение на двадцать аккаунтов приходит двадцатью
        событиями, и без него это двадцать пересчётов подряд.
        """
        last = self.last_tick_at
        if last is not None and self.clock.now() - last < self.min_tick_interval:
            return TickOutcome(status=TickStatus.DEBOUNCED)
        return await self.tick()

    async def run(
        self,
        every: timedelta,
        stop: asyncio.Event | None = None,
        on_error: Callable[[BaseException], Awaitable[None]] | None = None,
    ) -> None:
        """Гонять такт по расписанию, пока не попросят остановиться.

        Без `on_error` ошибка такта всплывает и останавливает контур — молча глотать
        её ядро не вправе. С `on_error` цикл продолжается: пропуск такта безопасен,
        потому что следующий читает состояние заново, а окна абсолютны, и система не
        остаётся в полусостоянии.
        """
        stop = stop or asyncio.Event()
        seconds = every.total_seconds()
        while not stop.is_set():
            try:
                await self.tick()
            except Exception as error:  # решает вызывающий, не ядро
                if on_error is None:
                    raise
                await on_error(error)
            try:
                await asyncio.wait_for(stop.wait(), timeout=seconds)
            except TimeoutError:
                continue

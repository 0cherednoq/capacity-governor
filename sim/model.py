"""Модель пула и контура на SimPy (G8).

Симуляция нужна, чтобы параметры (`target`, `reorder_ratio`, `max_per_window`,
`tolerance`, `stabilize_down`) выбирались прогоном, а не на глаз. Модель повторяет
устройство из плана: аккаунты — `FilterStore`, кредиты аккаунта — `Container`,
браузеры — `Resource`.

Политики чистые, расписание SimPy детерминировано, случайность берётся из `Random` с
переданным seed — значит прогон воспроизводится полностью.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Callable, Generator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from random import Random
from typing import Any

import simpy

from capacity import (
    ConsumerState,
    EligibleCapacity,
    Hold,
    MaxMinFair,
    ReorderPoint,
    Stabilizer,
    SupplySnapshot,
    plan_tick,
)

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


class AccountState(Enum):
    ACTIVE = "active"
    COOLING = "cooling"
    BANNED = "banned"


@dataclass(slots=True)
class Account:
    account_id: str
    credits: simpy.Container
    state: AccountState = AccountState.ACTIVE

    @property
    def usable(self) -> bool:
        return self.state is AccountState.ACTIVE and self.credits.level > 0


@dataclass(slots=True)
class Consumer:
    """Батч работы: спрос — это остаток, окно выставляет контур."""

    consumer_id: str
    remaining: int
    weight: int = 1
    guaranteed: int = 0
    window: int = 0
    in_flight: int = 0
    done: int = 0
    deferred: int = 0

    def state(self) -> ConsumerState:
        return ConsumerState(
            consumer_id=self.consumer_id,
            demand=self.remaining,
            weight=self.weight,
            guaranteed=self.guaranteed,
        )


@dataclass(slots=True)
class SimConfig:
    """Параметры прогона. Время в секундах модели."""

    horizon: float = 8 * 3600
    tick: float = 5.0
    dispatch_interval: float = 1.0
    check_duration: float = 20.0
    credits_per_account: int = 100
    credit_cost: int = 1
    browser_slots: int = 32
    registration_duration: float = 15 * 60
    registration_failure_rate: float = 0.1
    defer_delay: float = 30.0
    target: float = 50.0
    reorder_ratio: float = 0.2
    max_per_window: int = 20
    budget_window: timedelta = timedelta(hours=1)
    tolerance: float = 0.1
    stabilize_down: timedelta = timedelta(seconds=300)
    min_hold_ticks: int = 1
    seed: int = 20261016


@dataclass(slots=True)
class SimMetrics:
    """Итоги прогона. `window_writes` — метрика качества мёртвой зоны (G8, прогон 6)."""

    window_writes: int = 0
    windows_skipped: int = 0
    orders: int = 0
    order_ticks: int = 0
    registrations_done: int = 0
    registrations_failed: int = 0
    completed: int = 0
    deferred: int = 0
    ticks: int = 0
    served_ticks: Counter[str] = field(default_factory=Counter)
    starved_ticks: Counter[str] = field(default_factory=Counter)
    finished_at: dict[str, float] = field(default_factory=dict)
    peak_effective: float = 0.0

    @property
    def starved(self) -> list[str]:
        """Потребители, которым ёмкость была, а окна не досталось ни разу."""
        return sorted(cid for cid, served in self.served_ticks.items() if served == 0)


class Simulation:
    """Пул, потребители и контур распределения в одном прогоне."""

    def __init__(
        self,
        config: SimConfig,
        consumers: Sequence[Consumer],
        accounts: int,
        events: Sequence[tuple[float, Callable[[Simulation], None]]] = (),
    ) -> None:
        self.config = config
        self.rng = Random(config.seed)
        self.env = simpy.Environment()
        self.metrics = SimMetrics()
        self.consumers = {c.consumer_id: c for c in consumers}
        self.browsers = simpy.Resource(self.env, capacity=config.browser_slots)
        self.pool: simpy.FilterStore = simpy.FilterStore(self.env, capacity=10_000)
        self.all_accounts: list[Account] = []
        self.leased = 0
        self.orders_in_progress = 0
        self.orders_by_window: Counter[str] = Counter()
        self.deficits: Mapping[str, int] = {}
        self.holds: Mapping[str, Hold] = {}
        self.policy = MaxMinFair(min_hold_ticks=config.min_hold_ticks)
        self.stabilizer = Stabilizer(
            tolerance=config.tolerance, stabilize_down=config.stabilize_down
        )
        self.reorder = ReorderPoint(
            target=config.target,
            reorder_ratio=config.reorder_ratio,
            max_per_window=config.max_per_window,
            window=config.budget_window,
        )
        self._next_account = 0
        for _ in range(accounts):
            self._add_account()
        self.env.process(self._governor())
        for consumer in self.consumers.values():
            self.env.process(self._dispatcher(consumer))
        for at, action in events:
            self.env.process(self._schedule(at, action))

    # --- пул ------------------------------------------------------------------------

    def _add_account(self) -> Account:
        self._next_account += 1
        account = Account(
            account_id=f"acc-{self._next_account:04d}",
            credits=simpy.Container(
                self.env,
                capacity=self.config.credits_per_account,
                init=self.config.credits_per_account,
            ),
        )
        self.all_accounts.append(account)
        self.pool.put(account)
        return account

    def ban(self, fraction: float) -> None:
        """Забанить долю живого пула — сценарий 5 прогонов G8."""
        alive = [a for a in self.all_accounts if a.state is AccountState.ACTIVE]
        for account in alive[: int(len(alive) * fraction)]:
            account.state = AccountState.BANNED

    def flap(self, count: int, cooling: float) -> None:
        """Увести аккаунты в COOLING и вернуть — прогон 6 (колеблющаяся ёмкость)."""
        alive = [a for a in self.all_accounts if a.state is AccountState.ACTIVE]
        for account in alive[:count]:
            account.state = AccountState.COOLING
            self.env.process(self._uncool(account, cooling))

    def _uncool(self, account: Account, after: float) -> Generator[simpy.Event, Any, None]:
        yield self.env.timeout(after)
        if account.state is AccountState.COOLING:
            account.state = AccountState.ACTIVE

    def capacity(self) -> EligibleCapacity:
        # Годен тот, кто активен и ещё может оплатить единицу работы.
        eligible = sum(
            1
            for a in self.all_accounts
            if a.state is AccountState.ACTIVE and a.credits.level >= self.config.credit_cost
        )
        free = sum(1 for a in self.pool.items if a.usable)
        credit_sum = sum(
            a.credits.level for a in self.all_accounts if a.state is AccountState.ACTIVE
        )
        effective = credit_sum / self.config.credits_per_account
        self.metrics.peak_effective = max(self.metrics.peak_effective, effective)
        return EligibleCapacity(
            eligible=eligible,
            free=free,
            leased=self.leased,
            reserved=0,
            effective=effective,
            work_units=int(credit_sum // self.config.credit_cost),
        )

    def supply(self, now: datetime) -> SupplySnapshot:
        capacity = self.capacity()
        demand_credits = sum(c.remaining for c in self.consumers.values())
        key = self.reorder.window_key(now)
        return SupplySnapshot(
            effective_available=capacity.effective,
            needed_for_demand=demand_credits * self.config.credit_cost
            / self.config.credits_per_account,
            ordered_in_progress=self.orders_in_progress,
            ordered_in_window=self.orders_by_window[key],
        )

    # --- контур ---------------------------------------------------------------------

    def now(self) -> datetime:
        return EPOCH + timedelta(seconds=self.env.now)

    def _governor(self) -> Generator[simpy.Event, Any, None]:
        while True:
            now = self.now()
            capacity = self.capacity()
            states = [c.state() for c in self.consumers.values() if c.remaining > 0]
            plan = plan_tick(
                now=now,
                capacity=capacity,
                consumers=states,
                deficits=self.deficits,
                current_windows={
                    c.consumer_id: c.window
                    for c in self.consumers.values()
                    if c.remaining > 0
                },
                policy=self.policy,
                stabilizer=self.stabilizer,
                holds=self.holds,
                supply=self.supply(now),
                reorder=self.reorder,
            )
            self.deficits = plan.deficits
            self.holds = plan.holds
            for cid, limit in plan.windows.items():
                self.consumers[cid].window = limit
            self.metrics.window_writes += len(plan.windows)
            self.metrics.windows_skipped += plan.report.skipped_by_tolerance
            self.metrics.ticks += 1
            for cid, grant in plan.allocation.grants.items():
                if grant > 0:
                    self.metrics.served_ticks[cid] += 1
                elif capacity.allocatable > 0:
                    self.metrics.starved_ticks[cid] += 1
                else:
                    self.metrics.served_ticks.setdefault(cid, 0)

            if plan.order > 0:
                self.metrics.orders += plan.order
                self.metrics.order_ticks += 1
                self.orders_by_window[plan.order_window_key or ""] += plan.order
                for _ in range(plan.order):
                    self.orders_in_progress += 1
                    self.env.process(self._register())

            yield self.env.timeout(self.config.tick)

    def _register(self) -> Generator[simpy.Event, Any, None]:
        """Заказ пополнения: батч Tallyho, который либо даёт аккаунт, либо падает."""
        jitter = self.rng.uniform(0.6, 1.4)
        yield self.env.timeout(self.config.registration_duration * jitter)
        self.orders_in_progress -= 1
        if self.rng.random() < self.config.registration_failure_rate:
            self.metrics.registrations_failed += 1
            return
        self._add_account()
        self.metrics.registrations_done += 1

    # --- потребители ----------------------------------------------------------------

    def _dispatcher(self, consumer: Consumer) -> Generator[simpy.Event, Any, None]:
        """Ставит работу в пределах окна: окно меняется на ходу, ноль разрешён."""
        while consumer.remaining > 0:
            # В полёте не больше, чем осталось работы: иначе батч «перевыполнится».
            while (
                consumer.in_flight < consumer.window
                and consumer.remaining - consumer.in_flight > 0
            ):
                consumer.in_flight += 1
                self.env.process(self._item(consumer))
            yield self.env.timeout(self.config.dispatch_interval)

    def _item(self, consumer: Consumer) -> Generator[simpy.Event, Any, None]:
        account: Account | None = None
        try:
            request = self.pool.get(lambda a: a.usable)
            result = yield request | self.env.timeout(self.config.defer_delay)
            if request not in result:
                # Ресурса нет — `item.defer`: работа ждёт, попытка не тратится.
                request.cancel()
                consumer.deferred += 1
                self.metrics.deferred += 1
                return
            account = result[request]
            self.leased += 1
            with self.browsers.request() as browser:
                yield browser
                yield account.credits.get(self.config.credit_cost)
                yield self.env.timeout(self.config.check_duration)
            consumer.remaining -= 1
            consumer.done += 1
            self.metrics.completed += 1
            if consumer.remaining == 0:
                self.metrics.finished_at[consumer.consumer_id] = self.env.now
        finally:
            consumer.in_flight -= 1
            if account is not None:
                self.leased -= 1
                self.pool.put(account)

    # --- прогон ---------------------------------------------------------------------

    def _schedule(
        self, at: float, action: Callable[[Simulation], None]
    ) -> Generator[simpy.Event, Any, None]:
        yield self.env.timeout(at)
        action(self)

    def run(self) -> SimMetrics:
        self.env.run(until=self.config.horizon)
        self.metrics.deferred = sum(c.deferred for c in self.consumers.values())
        return self.metrics

    def report(self) -> str:
        m = self.metrics
        hours = self.config.horizon / 3600
        lines = [
            f"тактов: {m.ticks} за {hours:.0f} ч",
            f"записей окон: {m.window_writes} (пропущено мёртвой зоной: {m.windows_skipped})",
            f"заказов: {m.orders} в {m.order_ticks} тактах, "
            f"готово {m.registrations_done}, упало {m.registrations_failed}",
            f"работы сделано: {m.completed}, отложено (defer): {m.deferred}",
            f"пик эффективной ёмкости: {m.peak_effective:.1f}",
            f"голодало потребителей: {len(m.starved)}",
        ]
        remaining = {c.consumer_id: c.remaining for c in self.consumers.values()}
        done = {cid: r for cid, r in remaining.items() if r == 0}
        lines.append(f"батчей закрыто: {len(done)} из {len(remaining)}")
        return "\n".join(lines)


def windows_per_hour(metrics: SimMetrics, config: SimConfig) -> float:
    """Записей окон в час — то, что мёртвая зона обязана держать низким."""
    return metrics.window_writes / math.ceil(config.horizon / 3600)

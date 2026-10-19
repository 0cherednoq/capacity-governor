"""Адаптеры к Tallyho: окна, спрос, бюджетный журнал, заказ пополнения (п.6 плана).

Tallyho здесь **не импортируется**. Внешнее API описано минимальными `Protocol` по
его публичным сигнатурам, и реальный клиент удовлетворяет им структурно. Так адаптеры
типизируются и тестируются без установки Tallyho, а ядро остаётся переносимым (G0).

Соответствие проверено по рабочему дереву Tallyho на 19.10.2026:

* `BatchHandle.set_max_in_flight(limit, *, session=None, reason=None)` —
  `src/tallyho/api/batch.py:425`, значение абсолютное и идемпотентное, `limit=0`
  закрывает окно;
* `BatchHandle.view() -> BatchView` — `api/batch.py:333`, спрос берётся из
  `view().progress`;
* `Tallyho.list_batches(kinds=…, states=…, created_after=…)` — `api/client.py:438`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from capacity.types import ConsumerState

# --- внешнее API Tallyho, описанное по его сигнатурам -------------------------------


class Progress(Protocol):
    """Счётчики батча: `found` — всего уникальных items, `queued` — ждут выполнения,
    `in_flight` — у брокера.

    Члены объявлены только на чтение: адаптер ничего в них не пишет, а изменяемый
    атрибут в `Protocol` инвариантен и не принял бы ни реальный `Progress` Tallyho,
    ни подделку в тестах.
    """

    @property
    def found(self) -> int: ...

    @property
    def queued(self) -> int: ...

    @property
    def in_flight(self) -> int: ...


class BatchView(Protocol):
    @property
    def progress(self) -> Progress: ...


class BatchHandle(Protocol):
    async def view(self) -> BatchView: ...

    async def set_max_in_flight(
        self, limit: int, *, session: Any = None, reason: str | None = None
    ) -> None: ...


class BatchInfo(Protocol):
    @property
    def id(self) -> UUID: ...

    @property
    def key(self) -> str | None: ...


class BatchPage(Protocol):
    @property
    def items(self) -> Sequence[BatchInfo]: ...


class TallyhoClient(Protocol):
    def handle(self, batch_id: UUID) -> BatchHandle: ...

    async def list_batches(
        self,
        *,
        kinds: Collection[str] | None = ...,
        created_after: datetime | None = ...,
        limit: int = ...,
    ) -> BatchPage: ...


# --- журнал применённых окон --------------------------------------------------------


@runtime_checkable
class WindowJournal(Protocol):
    """Чем контур помнит, какое окно он выставил.

    Прочитать текущее окно у Tallyho нельзя: `set_max_in_flight` и
    `clear_max_in_flight` есть, а в `BatchView` окна нет. Для мёртвой зоны (G5) нужно
    сравнивать новое значение с текущим, поэтому контур ведёт журнал сам — и это
    корректно: окна абсолютны, и пишет их только он. Вмешательство оператора идёт
    через `pin`/`pause` (G7), то есть мимо этого журнала и явно.
    """

    async def load(self) -> Mapping[str, int]: ...

    async def save(self, windows: Mapping[str, int]) -> None: ...


# --- адаптеры -----------------------------------------------------------------------

#: Как `consumer_id` политики превращается в батч Tallyho и обратно.
BatchResolver = Callable[[str], UUID]


@dataclass(slots=True)
class TallyhoWindows:
    """`WindowSink` поверх `set_max_in_flight` (п.6, G6).

    Все окна такта выставляются в транзакции такта: `session` передаётся в Tallyho,
    изменение входит в коммит вызывающего, а relay получает пинок после него. Окна
    абсолютны, поэтому повтор такта ничего не ломает.
    """

    client: TallyhoClient
    resolve: BatchResolver
    journal: WindowJournal
    session_provider: Callable[[], Any] | None = None
    reason: str = "capacity-governor"

    async def current_windows(self) -> Mapping[str, int]:
        return await self.journal.load()

    async def set_windows(self, windows: Mapping[str, int]) -> None:
        session = self.session_provider() if self.session_provider else None
        for consumer_id, limit in windows.items():
            handle = self.client.handle(self.resolve(consumer_id))
            await handle.set_max_in_flight(limit, session=session, reason=self.reason)
        applied = dict(await self.journal.load())
        applied.update(windows)
        await self.journal.save(applied)


@dataclass(slots=True)
class TallyhoConsumers:
    """`ConsumerRegistry` поверх `view().progress`.

    Спрос — это остаток работы: `queued + in_flight`. Дешёвого `demand()` (T3) в
    Tallyho ещё нет, поэтому берётся `view()`, то есть чтение поддерева батча на такт.
    Для единиц батчей это единицы запросов; когда потребителей станет много, T3
    превратится из удобства в необходимость, и заменится ровно это место.
    """

    client: TallyhoClient
    batches: Callable[[], Awaitable[Mapping[str, UUID]]]
    weights: Mapping[str, int] = field(default_factory=dict)
    guarantees: Mapping[str, int] = field(default_factory=dict)
    ceilings: Mapping[str, int] = field(default_factory=dict)

    async def consumers(self) -> Sequence[ConsumerState]:
        states: list[ConsumerState] = []
        for consumer_id, batch_id in (await self.batches()).items():
            progress = (await self.client.handle(batch_id).view()).progress
            states.append(
                ConsumerState(
                    consumer_id=consumer_id,
                    demand=progress.queued + progress.in_flight,
                    weight=self.weights.get(consumer_id, 1),
                    guaranteed=self.guarantees.get(consumer_id, 0),
                    ceiling=self.ceilings.get(consumer_id),
                )
            )
        return states


@dataclass(slots=True)
class TallyhoOrderLedger:
    """Бюджетный журнал пополнения: сами батчи регистраций и есть журнал (G4).

    `ordered_in_window` считается по батчам, созданным с начала бюджетного окна, а
    `ordered_in_progress` — по их незавершённым слотам. Батчей за окно единицы, цена
    чтения — единицы запросов, и результат кэшируется на длительность такта: политике
    он нужен один раз за такт.
    """

    client: TallyhoClient
    kinds: Collection[str]
    window: timedelta
    now: Callable[[], datetime]
    cache_for: timedelta = timedelta(seconds=5)
    _cached: tuple[datetime, int, int] | None = field(default=None, init=False)

    async def counts(self) -> tuple[int, int]:
        """Вернуть `(ordered_in_window, ordered_in_progress)`."""
        moment = self.now()
        if self._cached is not None:
            cached_at, in_window, in_progress = self._cached
            if moment - cached_at < self.cache_for:
                return in_window, in_progress

        page = await self.client.list_batches(
            kinds=self.kinds, created_after=moment - self.window, limit=100
        )
        in_window = 0
        in_progress = 0
        for info in page.items:
            progress = (await self.client.handle(info.id).view()).progress
            # Бюджет тратит сам факт заказа, поэтому в окне считаются все слоты,
            # а в работе — только незавершённые: упавшая регистрация освобождает
            # место в расчёте нехватки, но бюджет окна уже израсходован.
            in_window += progress.found
            in_progress += progress.queued + progress.in_flight
        self._cached = (moment, in_window, in_progress)
        return in_window, in_progress


@dataclass(slots=True)
class TallyhoReplenisher:
    """`Replenisher`: заказ — это батч регистраций (G4, §8.3 документа).

    Идемпотентность держится на двух ключах: ключ батча — `replenish:{window_start}`,
    то есть начало **бюджетного** окна, а не минута такта (иначе бюджет считается по
    одному окну, а дедупликация идёт по другому), и ключ каждого слота — `slot:{i}`,
    который ловится уникальным индексом `(batch_id, key)`. Повторный такт в том же
    окне допишет только недостающие слоты.
    """

    submit: Callable[[str, int], Awaitable[None]]

    async def order(self, count: int, window_key: str) -> None:
        if count <= 0:
            return
        await self.submit(window_key, count)

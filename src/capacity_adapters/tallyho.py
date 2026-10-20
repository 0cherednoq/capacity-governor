"""Адаптеры к Tallyho: спрос, окна, бюджетный журнал, заказ пополнения.

Tallyho здесь **не импортируется**. Внешнее API описано минимальными `Protocol` по
его публичным сигнатурам, и реальный клиент удовлетворяет им структурно. Так адаптеры
типизируются и тестируются без установки Tallyho, а ядро остаётся переносимым.

Всё, что контуру нужно знать о потребителях, приходит одним вызовом `th.demand()`:
спрос, текущее окно и признак паузы. Число запросов не зависит от числа батчей, а
спрос и окна гарантированно описывают один и тот же момент — раньше окна приходилось
помнить самому, и этот журнал расходился с Tallyho при любой правке мимо контура.
"""

from __future__ import annotations

from collections.abc import (
    Awaitable,
    Callable,
    Collection,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from capacity.types import ConsumerState


class DemandTruncatedError(RuntimeError):
    """`th.demand()` упёрся в `limit`: часть батчей не попала в выдачу.

    Раздавать по неполному списку нельзя — потребители, которых контур не увидел,
    получат ноль, а их доля уйдёт остальным. Лучше пропустить такт и поднять `limit`.
    """

    def __init__(self, returned: int, limit: int) -> None:
        super().__init__(
            f"th.demand() вернул {returned} батчей при limit={limit}: "
            "список усечён, раздавать по нему нельзя"
        )
        self.returned = returned
        self.limit = limit


# --- внешнее API Tallyho, описанное по его сигнатурам -------------------------------


class BatchDemand(Protocol):
    """Строка `th.demand()`: спрос и окно одного батча.

    Члены объявлены на чтение: адаптер в них не пишет, а изменяемый атрибут в
    `Protocol` инвариантен и не принял бы ни реальный тип Tallyho, ни подделку.
    """

    @property
    def id(self) -> UUID: ...

    @property
    def kind(self) -> str: ...

    @property
    def key(self) -> str | None: ...

    @property
    def attributes(self) -> Mapping[str, Any]: ...

    @property
    def pending(self) -> int: ...

    @property
    def in_flight(self) -> int: ...

    @property
    def deferred(self) -> int: ...

    @property
    def max_in_flight(self) -> int | None: ...

    @property
    def paused(self) -> bool: ...

    @property
    def truncated(self) -> bool: ...


class Progress(Protocol):
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

    async def release_deferred(
        self, reasons: Collection[str] | None = ..., *, session: Any = ...
    ) -> int: ...

    async def set_max_in_flight(
        self, limit: int, *, session: Any = None, reason: str | None = None
    ) -> None: ...


class TaskCall(Protocol):
    def opts(self, *, key: str | None = ..., **options: object) -> TaskCall: ...


class BatchBuilder(Protocol):
    async def add_calls(self, calls: Iterable[TaskCall]) -> None: ...


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

    def call(self, fn: Callable[..., Any], *args: Any) -> TaskCall: ...

    def batch(
        self,
        kind: str,
        *,
        key: str | None = ...,
        seal: bool = ...,
        max_in_flight: int | None = ...,
    ) -> AbstractAsyncContextManager[BatchBuilder]: ...

    async def demand(
        self,
        kinds: Sequence[str],
        *,
        attributes: Mapping[str, object] | None = ...,
        limit: int = ...,
    ) -> Sequence[BatchDemand]: ...

    async def find(self, kind: str, key: str) -> BatchHandle: ...

    async def list_batches(
        self,
        *,
        kinds: Collection[str] | None = ...,
        created_after: datetime | None = ...,
        limit: int = ...,
    ) -> BatchPage: ...


# --- спрос и окна одним снимком -----------------------------------------------------


def ready_demand(row: BatchDemand, *, wakeable: bool = False) -> int:
    """Сколько работы потребитель может запустить прямо сейчас.

    Отложенные items (`item.defer`) из спроса вычитаются: они не уйдут в брокер раньше
    своего срока, и окно под них простояло бы впустую. В пополнении спрос считается
    иначе — там нужна вся оставшаяся работа, включая отложенную.

    `wakeable` переворачивает это рассуждение. Если контуру дан `DeferredWaker`, он
    выпускает отложенную работу сам, как только открывает окно, — и тогда вычитать её
    нельзя: спрос станет нулевым, окно не откроется, будить будет нечего и работа
    останется лежать до конца срока. Этот замкнутый круг и разрывает флаг.
    """
    return row.pending if wakeable else max(0, row.pending - row.deferred)


@dataclass(slots=True)
class TallyhoDemand:
    """Снимок спроса и окон за такт: один `th.demand()` при любом числе батчей.

    Снимок делает `consumers()`, а `windows()` отдаёт окна из него же. Контур читает
    спрос раньше окон, поэтому они всегда описывают один момент. Батч с
    `max_in_flight is None` контуру не принадлежит: окна ему не выставляли, и он в
    раздачу не входит — иначе governor молча забрал бы под управление чужую работу.
    """

    client: TallyhoClient
    kinds: Sequence[str]
    attributes: Mapping[str, object] | None = None
    limit: int = 1000
    wakeable: bool = False
    """Контуру дан `DeferredWaker`: отложенная работа входит в спрос, см. `ready_demand`."""
    weight_of: Callable[[BatchDemand], int] = lambda _: 1
    guaranteed_of: Callable[[BatchDemand], int] = lambda _: 0
    ceiling_of: Callable[[BatchDemand], int | None] = lambda _: None
    snapshot: dict[str, BatchDemand] = field(default_factory=dict)

    async def consumers(self) -> Sequence[ConsumerState]:
        rows = await self.client.demand(
            self.kinds, attributes=self.attributes, limit=self.limit
        )
        if any(row.truncated for row in rows):
            raise DemandTruncatedError(len(rows), self.limit)

        self.snapshot = {
            str(row.id): row
            for row in rows
            # Пауза — решение человека, и окно ей не нужно: батч всё равно не работает.
            if not row.paused and row.max_in_flight is not None
        }
        return [
            ConsumerState(
                consumer_id=consumer_id,
                demand=ready_demand(row, wakeable=self.wakeable),
                weight=self.weight_of(row),
                guaranteed=self.guaranteed_of(row),
                ceiling=self.ceiling_of(row),
            )
            for consumer_id, row in self.snapshot.items()
        ]

    def windows(self) -> Mapping[str, int]:
        """Текущие окна из того же снимка, что и спрос."""
        return {
            consumer_id: row.max_in_flight
            for consumer_id, row in self.snapshot.items()
            if row.max_in_flight is not None
        }

    def pending_work(self) -> int:
        """Вся оставшаяся работа, включая отложенную: столько нужно ресурса.

        Это вход правила нехватки в `ReorderPoint`: пополнять надо под всю работу, а
        не только под ту, что может начаться сейчас.
        """
        return sum(row.pending for row in self.snapshot.values())

    def deferred_work(self) -> int:
        """Сколько работы ждёт своего срока: метрика нехватки ресурса."""
        return sum(row.deferred for row in self.snapshot.values())


@dataclass(slots=True)
class TallyhoWindows:
    """`WindowSink` поверх `set_max_in_flight`.

    Текущие окна берутся из снимка `TallyhoDemand` — того же, что дал спрос. Своего
    журнала у контура больше нет: он рос без границ, расходился с Tallyho после
    пересоздания схемы и не видел окон, выставленных проектом мимо контура.

    Все окна такта выставляются в его транзакции: `session` передаётся в Tallyho,
    изменение входит в коммит вызывающего, а relay получает пинок после него.
    """

    client: TallyhoClient
    demand: TallyhoDemand
    session_provider: Callable[[], Any] | None = None
    reason: str = "capacity-governor"

    async def current_windows(self) -> Mapping[str, int]:
        return self.demand.windows()

    async def set_windows(self, windows: Mapping[str, int]) -> None:
        session = self.session_provider() if self.session_provider else None
        for consumer_id, limit in windows.items():
            handle = self.client.handle(UUID(consumer_id))
            await handle.set_max_in_flight(limit, session=session, reason=self.reason)


@dataclass(slots=True)
class TallyhoWaker:
    """`DeferredWaker` поверх `handle.release_deferred()`.

    Задача, не получившая ресурс, откладывает себя на выбранный ею срок — выбранный
    вслепую, потому что когда ресурс вернётся, она не знает. Контур знает: он сам
    открывает окно, и в этом же такте будит отложенных.

    `reasons` стоит задать своими причинами отложения (например, `["no_accounts"]`):
    тогда работа, отложенная по другим поводам — скажем, ретрай через час, — продолжит
    ждать свой срок. `None` будит всех отложенных батча.

    Будильник ничего не ломает: пауза, отложенный старт и окно соблюдаются, выпуск
    идёт в пределах свободного места, счётчики отложений не сбрасываются (потолки
    `max_defers` пробуждением не обойти), а сам вызов идемпотентен.
    """

    client: TallyhoClient
    reasons: Collection[str] | None = None
    session_provider: Callable[[], Any] | None = None

    async def wake(self, consumer_id: str) -> None:
        session = self.session_provider() if self.session_provider else None
        await self.client.handle(UUID(consumer_id)).release_deferred(
            self.reasons, session=session
        )


@dataclass(slots=True)
class TallyhoOrderLedger:
    """Бюджетный журнал пополнения: сами батчи регистраций и есть журнал.

    `ordered_in_window` — все слоты, заказанные с начала бюджетного окна, включая уже
    выполненные: бюджет тратит сам факт заказа. Их число даёт только `found`, которого
    в `demand()` нет, поэтому батчи окна читаются через `view()`. Это не возврат к
    старому: батчей за окно единицы, и читаются они раз в такт, а не на каждый батч
    работы, как было со спросом.

    Результат кэшируется на длительность такта: политике он нужен один раз за такт.
    """

    client: TallyhoClient
    kinds: Sequence[str]
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
            kinds=list(self.kinds), created_after=moment - self.window, limit=100
        )
        # Бюджет тратит сам факт заказа, поэтому в окне считаются все слоты, а в
        # работе — только незавершённые: упавшая регистрация освобождает место в
        # расчёте нехватки, но бюджет окна уже израсходован.
        in_window = 0
        in_progress = 0
        for info in page.items:
            progress = (await self.client.handle(info.id).view()).progress
            in_window += progress.found
            in_progress += progress.queued + progress.in_flight
        self._cached = (moment, in_window, in_progress)
        return in_window, in_progress


@dataclass(slots=True)
class TallyhoReplenisher:
    """`Replenisher`: заказ — это батч регистраций.

    Идемпотентность держится на двух ключах, и оба здесь не случайны:

    * ключ батча — тот `window_key`, что дала политика, то есть
      `replenish:{window_start}` по началу **бюджетного** окна, а не по минуте такта.
      Иначе бюджет считался бы по одному окну, а дедупликация шла бы по другому;
    * ключ каждого слота — `slot:{i}`, который ловится уникальным индексом
      `(batch_id, key) WHERE key IS NOT NULL`.

    Поэтому повторный такт в том же окне допишет ровно недостающие слоты: батч
    находится по ключу, уже существующие слоты отсеиваются индексом. Батч создаётся с
    `seal=False` — вход остаётся открытым до конца окна, ведь следующий такт может
    добавить ещё.

    `register` — задача регистрации одного ресурса, её пишет проект: что именно значит
    «завести аккаунт», библиотека не знает.
    """

    client: TallyhoClient
    kind: str
    register: Callable[..., Awaitable[object]]
    max_in_flight: int | None = None

    async def order(self, count: int, window_key: str) -> None:
        if count <= 0:
            return
        existing = await self._slots_in(window_key)
        calls = [
            self.client.call(self.register, window_key, index).opts(key=f"slot:{index}")
            for index in range(existing, existing + count)
        ]
        async with self.client.batch(
            self.kind,
            key=window_key,
            seal=False,
            max_in_flight=self.max_in_flight,
        ) as batch:
            await batch.add_calls(calls)

    async def _slots_in(self, window_key: str) -> int:
        """Сколько слотов уже заказано в этом окне.

        Нумерация продолжается с этого места, иначе повторный заказ попал бы на уже
        занятые ключи `slot:{i}` и был бы отсеян индексом целиком — бюджет списался
        бы, а работа не появилась.

        Считается по `found`, а не по незавершённым: слот, который уже выполнился,
        свой ключ не освобождает. Поэтому здесь `view()`, а не `demand()`.
        """
        try:
            handle = await self.client.find(self.kind, window_key)
        except Exception:
            return 0  # батча этого окна ещё нет — заказ первый
        return (await handle.view()).progress.found

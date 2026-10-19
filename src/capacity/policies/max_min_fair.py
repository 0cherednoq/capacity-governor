"""Max-min fair раздача общего пула на deficit round robin (G2).

Прогрессивное заполнение (progressive filling) — канонический способ считать max-min
fair распределение; весовой вариант = скорости заполнения пропорциональны весам,
гарантированный минимум = заполнение начинается с него, потолок = виртуальное
ограничение спроса.

Очерёдность внутри такта задаётся не номером такта (сдвиг по индексу ломается, когда
потребители появляются и исчезают между тактами), а счётчиком недополученного:
`deficit` пополняется на `weight` каждый такт и тратится при выдаче, остаток
переносится. Потребитель, которому не досталось, в следующий такт стоит первым
по построению.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from capacity.types import Allocation, ConsumerState, Hold


@dataclass(frozen=True, slots=True)
class MaxMinFair:
    """Раздача общей ёмкости: веса, гарантии, потолки, перенос дефицитов.

    `deficit_cap` ограничивает кредит потребителя, который присутствует, но ничего не
    просит (`headroom == 0`): без ограничения потребитель, простоявший сутки с нулевым
    спросом, вернулся бы с огромным кредитом и вытеснил остальных. Выражается в тактах:
    предел равен `deficit_cap_ticks * weight`.

    Кредит активного претендента (есть спрос, но ёмкости не хватило) **не** ограничен:
    этот кредит и есть его место в очереди, и срезать его значит вернуть голодание,
    от которого DRR и спасает. Потребитель, которого в такте нет вовсе, свой дефицит
    теряет — ключ удаляется (шаг 5 G2).

    `min_grant_floor` — сколько выдать при нулевой ёмкости раздачи, когда эффективной
    ёмкости между 0 и 1 (floor дал 0, а кредиты ещё есть). По умолчанию 0: лучше
    встать, чем жечь попытки. Если задан, его получает один потребитель — первый
    по очереди дефицитов, чтобы остаток доедался последовательно, а не размазывался.

    `min_hold_ticks` — сколько тактов выданный объём закреплён за потребителем. По
    умолчанию 1, то есть раздача пересчитывается каждый такт. Смысл значения больше
    единицы виден там, где ёмкость меньше числа потребителей: окна ходят между 0 и 1
    каждый такт, и каждый такой переход — исключение из мёртвой зоны, то есть запись
    в БД. Удержание отдаёт потребителю его единицу на несколько тактов подряд, пока
    остальные ждут в очереди по дефицитам — справедливость сохраняется, а частота
    записи падает кратно. Разумная величина — время единицы работы, делённое на такт:
    отбирать окно раньше, чем работа успевает пройти, значит просто жечь записи.
    """

    deficit_cap_ticks: int = 4
    min_grant_floor: int = 0
    min_hold_ticks: int = 1

    def __post_init__(self) -> None:
        if self.deficit_cap_ticks < 1:
            raise ValueError("deficit_cap_ticks < 1")
        if self.min_grant_floor < 0:
            raise ValueError("min_grant_floor < 0")
        if self.min_hold_ticks < 1:
            raise ValueError("min_hold_ticks < 1")

    def allocate(
        self,
        capacity: int,
        consumers: Sequence[ConsumerState],
        deficits: Mapping[str, int],
        holds: Mapping[str, Hold] | None = None,
    ) -> Allocation:
        if capacity < 0:
            raise ValueError("capacity < 0")
        ids = [c.consumer_id for c in consumers]
        if len(set(ids)) != len(ids):
            raise ValueError("потребители с одинаковым consumer_id")

        grants = {c.consumer_id: 0 for c in consumers}
        if not consumers:
            return Allocation(grants={}, deficits={})

        # Порядок обхода: кто больше недополучил — тот первым. Тай-брейк по id, чтобы
        # политика осталась чистой функцией (одинаковый вход → одинаковый выход).
        order = sorted(consumers, key=lambda c: (-deficits.get(c.consumer_id, 0), c.consumer_id))

        remaining = capacity
        if remaining == 0 and self.min_grant_floor > 0:
            for c in order:
                if c.headroom > 0:
                    grants[c.consumer_id] = min(self.min_grant_floor, c.headroom)
                    break
            return Allocation(
                grants=grants,
                deficits=self._carry(order, grants, grants, deficits),
                holds=self._next_holds(order, grants, holds or {}),
            )

        remaining = self._honour_holds(order, grants, holds or {}, remaining)
        remaining = self._grant_guarantees(order, grants, remaining)
        guaranteed_part = dict(grants)
        self._fill(order, grants, remaining)
        return Allocation(
            grants=grants,
            deficits=self._carry(order, grants, guaranteed_part, deficits),
            holds=self._next_holds(order, grants, holds or {}),
        )

    @staticmethod
    def _honour_holds(
        order: Sequence[ConsumerState],
        grants: dict[str, int],
        holds: Mapping[str, Hold],
        remaining: int,
    ) -> int:
        """Шаг 0: закреплённое выдаётся раньше всего остального.

        Ёмкость могла упасть с прошлого такта, поэтому удержание всё равно урезается
        по оставшейся ёмкости: обещание держать окно не создаёт ресурса.
        """
        for c in order:
            if remaining == 0:
                break
            hold = holds.get(c.consumer_id)
            if hold is None or hold.ticks_left == 0:
                continue
            give = min(hold.amount, c.headroom, remaining)
            if give > 0:
                grants[c.consumer_id] += give
                remaining -= give
        return remaining

    def _next_holds(
        self,
        order: Sequence[ConsumerState],
        grants: Mapping[str, int],
        holds: Mapping[str, Hold],
    ) -> dict[str, Hold]:
        """Шаг 6: продлить начатые удержания и начать новые по сегодняшней выдаче."""
        if self.min_hold_ticks == 1:
            return {}
        carried: dict[str, Hold] = {}
        for c in order:
            cid = c.consumer_id
            granted = grants.get(cid, 0)
            if granted == 0:
                continue
            running = holds.get(cid)
            if running is not None and running.ticks_left > 1:
                carried[cid] = Hold(amount=min(running.amount, granted),
                                    ticks_left=running.ticks_left - 1)
            elif running is None:
                carried[cid] = Hold(amount=granted, ticks_left=self.min_hold_ticks - 1)
        return carried

    @staticmethod
    def _grant_guarantees(
        order: Sequence[ConsumerState],
        grants: dict[str, int],
        remaining: int,
    ) -> int:
        """Шаг 2: гарантия выдаётся первой, но ёмкости не создаёт."""
        for c in order:
            if remaining == 0:
                break
            give = min(c.guaranteed, c.headroom, remaining)
            if give > 0:
                grants[c.consumer_id] += give
                remaining -= give
        return remaining

    @staticmethod
    def _fill(order: Sequence[ConsumerState], grants: dict[str, int], remaining: int) -> None:
        """Шаг 3: прогрессивное заполнение со скоростями, пропорциональными весам."""
        while remaining > 0:
            active = [c for c in order if grants[c.consumer_id] < c.headroom]
            if not active:
                return
            total_weight = sum(c.weight for c in active)
            if remaining < total_weight:
                # Единиц меньше, чем весов: раздать по одной в порядке очереди.
                for c in active[:remaining]:
                    grants[c.consumer_id] += 1
                return
            level = remaining // total_weight
            for c in active:
                give = min(level * c.weight, c.headroom - grants[c.consumer_id])
                grants[c.consumer_id] += give
                remaining -= give

    def _carry(
        self,
        order: Sequence[ConsumerState],
        grants: Mapping[str, int],
        guaranteed_part: Mapping[str, int],
        deficits: Mapping[str, int],
    ) -> dict[str, int]:
        """Шаг 4: перенос дефицитов. Гарантированная часть выдачи кредит не тратит."""
        carried: dict[str, int] = {}
        for c in order:
            cid = c.consumer_id
            spent = grants[cid] - guaranteed_part.get(cid, 0)
            accrued = deficits.get(cid, 0) + c.weight
            if c.headroom == 0:
                # Простаивающий копит кредит ограниченно (см. docstring класса).
                accrued = min(accrued, self.deficit_cap_ticks * c.weight)
            carried[cid] = max(0, accrued - spent)
        return carried

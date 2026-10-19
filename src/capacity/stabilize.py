"""Мёртвая зона и стабилизация окон (G5).

Ёмкость между тактами меняется непрерывно — баланс тает с каждой проверкой домена.
Если выставлять окна абсолютно каждые 5 секунд, они будут дёргаться: каждое
уменьшение окна — парковка записей outbox, каждое увеличение — распарковка, то есть
запись в БД на пустом месте.

Решение взято у HPA в Kubernetes и асимметрично намеренно:

* мёртвая зона: окно не трогаем, если `|new - cur| < max(1, tolerance * cur)`;
* вверх реагируем сразу — работа стоит, это дорого;
* вниз — по окну стабилизации: уменьшаем только после того, как весь период
  `stabilize_down` наблюдался непрерывно, и берём самую консервативную
  (максимальную) рекомендацию за этот период: ёмкость могла мигнуть.

Исключения, применяемые всегда: `new == 0` (ресурс кончился — останавливаем
немедленно), `cur == 0 and new > 0` (ресурс появился — запускаем немедленно),
потребитель появился или исчез.

История рекомендаций живёт в памяти процесса, и это безопасно по двум причинам. Лидер
с пустой историей консервативен: пока окно наблюдения не заполнено, уменьшение
удерживается. Лидер, вернувшийся после перерыва длиннее `stabilize_down`, начинает
наблюдение заново — иначе он применил бы уменьшение сразу, считая, что наблюдал всё
это время, хотя его история давно обрезана.

Отложенное уменьшение — это переподписка: окна больше, чем ресурса. Поэтому `apply`
принимает ёмкость и срезает отложенное ровно настолько, чтобы сумма окон в неё
уложилась. Экономия записей не должна оплачиваться работой, выпущенной под ресурс,
которого нет.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True)
class Recommendation:
    """Что политика посоветовала в момент `at`."""

    at: datetime
    windows: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class StabilizeResult:
    """Что писать в этом такте.

    `windows` содержит только изменившиеся окна: не изменившееся не пишется вовсе (G6).
    """

    windows: Mapping[str, int]
    skipped_by_tolerance: int = 0
    held_down: int = 0
    oversubscribed: int = 0
    """Сколько единиц окна не удалось уложить в ёмкость: в норме ноль."""

    @property
    def deltas(self) -> Mapping[str, int]:
        return self.windows


@dataclass(slots=True)
class Stabilizer:
    """Слой над политикой: решает, писать ли окно и какое именно."""

    tolerance: float = 0.1
    stabilize_down: timedelta = timedelta(seconds=30)
    history: list[Recommendation] = field(default_factory=list)
    observing_since: datetime | None = None
    last_seen_at: datetime | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.tolerance < 1:
            raise ValueError("tolerance вне [0, 1)")
        if self.stabilize_down < timedelta(0):
            raise ValueError("stabilize_down < 0")

    def reset(self) -> None:
        self.history.clear()
        self.observing_since = None
        self.last_seen_at = None

    def apply(
        self,
        now: datetime,
        recommended: Mapping[str, int],
        current: Mapping[str, int],
        capacity: int | None = None,
        spent_elsewhere: int = 0,
    ) -> StabilizeResult:
        """Решить, какие окна писать.

        `capacity` — сколько ресурса есть на самом деле, `spent_elsewhere` — сколько
        его расходуют потребители вне раздачи (пины). Вместе они держат инвариант:
        сумма окон не превышает доступного. Без них отложенное уменьшение — это
        переподписка, то есть работа, выпущенная под ресурс, которого нет.
        """
        self._remember(now, recommended)
        to_write: dict[str, int] = {}
        skipped = 0
        held: list[str] = []

        for cid, new in recommended.items():
            if cid not in current:
                # Потребитель появился: исключение из мёртвой зоны.
                to_write[cid] = new
                continue
            cur = current[cid]
            if new == cur:
                continue
            if new == 0 or cur == 0:
                # Ресурс кончился либо появился — применяем немедленно.
                to_write[cid] = new
                continue
            if new > cur:
                if self._inside_tolerance(new, cur):
                    skipped += 1
                    continue
                to_write[cid] = new
                continue

            candidate = self._most_conservative(cid, now)
            if candidate is None or candidate >= cur:
                held.append(cid)
                continue
            if self._inside_tolerance(candidate, cur):
                skipped += 1
                held.append(cid)
                continue
            to_write[cid] = candidate

        budget = None if capacity is None else capacity - spent_elsewhere
        oversubscribed = self._enforce_budget(recommended, current, to_write, held, budget)
        return StabilizeResult(
            windows=to_write,
            skipped_by_tolerance=skipped,
            held_down=len(held),
            oversubscribed=oversubscribed,
        )

    @staticmethod
    def _enforce_budget(
        recommended: Mapping[str, int],
        current: Mapping[str, int],
        to_write: dict[str, int],
        held: list[str],
        budget: int | None,
    ) -> int:
        """Срезать отложенные уменьшения, пока сумма окон не уложится в ёмкость.

        Откладывать уменьшение можно только пока инвариант цел. Срезаем с того, кто
        переподписан сильнее всех, — он же и выиграл больше всех от отсрочки.

        `budget` — ёмкость за вычетом того, что расходуют потребители вне раздачи.
        Возвращает остаток переподписки: ноль, если инвариант удалось восстановить.
        Ненулевым он становится, когда даже рекомендации политики не умещаются в
        бюджет, — такое бывает при пинах, которые оператор выставил сверх ёмкости.
        """
        if budget is None:
            return 0
        projected = {cid: to_write.get(cid, current.get(cid, 0)) for cid in recommended}
        over = sum(projected.values()) - budget
        if over <= 0:
            return 0
        for cid in sorted(held, key=lambda c: projected[c] - recommended[c], reverse=True):
            if over <= 0:
                break
            cut = min(projected[cid] - recommended[cid], over)
            if cut <= 0:
                continue
            to_write[cid] = projected[cid] - cut
            over -= cut
        return max(0, over)

    def _inside_tolerance(self, new: int, cur: int) -> bool:
        return abs(new - cur) < max(1.0, self.tolerance * cur)

    def _most_conservative(self, cid: str, now: datetime) -> int | None:
        """Максимум рекомендаций по потребителю за окно стабилизации вниз.

        `None` — окно наблюдения ещё не заполнено: уменьшать нельзя, потому что
        рекомендаций за весь период просто нет и судить не о чем.
        """
        if self.observing_since is None or now - self.observing_since < self.stabilize_down:
            return None
        horizon = now - self.stabilize_down
        values = [r.windows[cid] for r in self.history if r.at >= horizon and cid in r.windows]
        return max(values) if values else None

    def _remember(self, now: datetime, recommended: Mapping[str, int]) -> None:
        gap = None if self.last_seen_at is None else now - self.last_seen_at
        if self.observing_since is None or (gap is not None and gap > self.stabilize_down):
            # Процесс не вёл контур дольше окна стабилизации: его история уже обрезана,
            # и наблюдение начинается заново. Иначе вернувшийся лидер применил бы
            # уменьшение сразу, считая, что наблюдал всё это время.
            self.observing_since = now
            self.history.clear()
        self.last_seen_at = now
        self.history.append(Recommendation(at=now, windows=dict(recommended)))
        horizon = now - self.stabilize_down
        # Исчезнувшие потребители уходят вместе со своими рекомендациями.
        self.history = [r for r in self.history if r.at >= horizon]

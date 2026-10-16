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

История рекомендаций живёт в памяти процесса. Это безопасно: лидер берётся на каждый
такт, и новый лидер с пустой историей ведёт себя строго консервативнее — пока окно
наблюдения не заполнено, уменьшение удерживается, и лишнего он не применит.
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

    @property
    def deltas(self) -> Mapping[str, int]:
        return self.windows


@dataclass(slots=True)
class Stabilizer:
    """Слой над политикой: решает, писать ли окно и какое именно."""

    tolerance: float = 0.1
    stabilize_down: timedelta = timedelta(seconds=300)
    history: list[Recommendation] = field(default_factory=list)
    observing_since: datetime | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.tolerance < 1:
            raise ValueError("tolerance вне [0, 1)")
        if self.stabilize_down < timedelta(0):
            raise ValueError("stabilize_down < 0")

    def reset(self) -> None:
        self.history.clear()
        self.observing_since = None

    def apply(
        self,
        now: datetime,
        recommended: Mapping[str, int],
        current: Mapping[str, int],
    ) -> StabilizeResult:
        self._remember(now, recommended)
        to_write: dict[str, int] = {}
        skipped = 0
        held = 0

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
                held += 1
                continue
            if self._inside_tolerance(candidate, cur):
                skipped += 1
                continue
            to_write[cid] = candidate

        return StabilizeResult(windows=to_write, skipped_by_tolerance=skipped, held_down=held)

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
        if self.observing_since is None:
            self.observing_since = now
        self.history.append(Recommendation(at=now, windows=dict(recommended)))
        horizon = now - self.stabilize_down
        # Исчезнувшие потребители уходят вместе со своими рекомендациями.
        self.history = [r for r in self.history if r.at >= horizon]

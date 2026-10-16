"""Последовательный расход выделенного пула (G3).

Главная мысль, из-за которой политика вообще нужна: аренда должна идти строго по
текущему ресурсу (`among=[current]`). Иначе Identity Hub, выбирая давно не
использованный аккаунт, начнёт их чередовать, и последовательный расход превратится
в размазывание по всему пулу.

Что в политике **не** лежит: переключение на следующий ресурс под блокировкой строки
потребителя со сравнением «текущий == умерший». Это домен проекта
(`Campaign.current_sender_id`), а не политика — поэтому `Sequential` в контуре
практически не нужен: окно выводится из статуса кампании в транзакции смены статуса.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from capacity.types import Allocation, ConsumerState


@dataclass(frozen=True, slots=True)
class Sequential:
    """Окно 1 при живом текущем ресурсе, иначе 0.

    `alive` — множество потребителей, у которых текущий ресурс жив. Потребитель без
    спроса окна не получает: выдавать окно под отсутствующую работу нельзя.
    """

    window: int = 1

    def __post_init__(self) -> None:
        if self.window < 1:
            raise ValueError("window < 1")

    def allocate(
        self,
        capacity: int,
        consumers: Sequence[ConsumerState],
        deficits: Mapping[str, int],
    ) -> Allocation:
        """`capacity` здесь — число потребителей с живым ресурсом (см. `allocate_alive`)."""
        alive = {c.consumer_id for c in consumers[:capacity]}
        return self.allocate_alive(consumers, alive)

    def allocate_alive(
        self,
        consumers: Sequence[ConsumerState],
        alive: frozenset[str] | set[str],
    ) -> Allocation:
        grants: dict[str, int] = {}
        for c in consumers:
            live = c.consumer_id in alive
            grants[c.consumer_id] = min(self.window, c.headroom) if live else 0
        # Выделенный пул: очереди между потребителями нет, дефициты не нужны.
        return Allocation(grants=grants, deficits={})

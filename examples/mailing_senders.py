"""Рассылка: выделенный пул отправителей и последовательный расход.

Ресурс: почтовые отправители, закреплённые за кампанией. Их нельзя чередовать —
репутация домена набирается последовательным прогревом одного адреса, а размазывание
нагрузки по всему пулу её портит.

Поэтому здесь не делёжка, а `Sequential`: окно 1, пока текущий отправитель жив; 0,
когда он умер. Переключение на следующего — работа приложения, и в примере видно
почему: оно идёт под блокировкой строки кампании и сравнивает «текущий == умерший»,
чтобы опоздавший отчёт не увёл кампанию через весь пул.

    python -m examples.mailing_senders
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from capacity import ConsumerState, Sequential


@dataclass
class Campaign:
    """Состояние кампании в вашей базе: пул, текущий отправитель, счётчик писем."""

    campaign_id: str
    senders: list[str]
    current: str | None = None
    dead: set[str] = field(default_factory=set)
    sent: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.current = self.senders[0]

    @property
    def alive(self) -> bool:
        return self.current is not None and self.current not in self.dead

    def switch_from(self, dead_sender: str) -> None:
        """Переключение под блокировкой строки: это домен приложения, не политика."""
        if self.current != dead_sender:
            return  # кто-то уже переключил, повтор ничего не меняет
        self.dead.add(dead_sender)
        self.current = next((s for s in self.senders if s not in self.dead), None)


def window_for(campaign: Campaign, policy: Sequential) -> int:
    """Окно кампании. В бою выставляется в транзакции смены статуса кампании."""
    state = ConsumerState(campaign.campaign_id, demand=10_000)
    alive = {campaign.campaign_id} if campaign.alive else set()
    return policy.allocate_alive([state], alive).grants[campaign.campaign_id]


async def send_batch(campaign: Campaign, window: int, letters: int) -> int:
    """Отправить письма в пределах окна. Сеть здесь заменена счётчиком."""
    if window == 0 or campaign.current is None:
        return 0
    campaign.sent[campaign.current] = campaign.sent.get(campaign.current, 0) + letters
    return letters


async def main() -> None:
    policy = Sequential()
    campaign = Campaign("spring-sale", senders=["warm-1", "warm-2", "warm-3"])

    sent = await send_batch(campaign, window_for(campaign, policy), letters=200)
    print(f"окно {window_for(campaign, policy)}, отправлено {sent} через {campaign.current}")

    # Платформа заблокировала текущего отправителя.
    campaign.dead.add("warm-1")
    print("отправитель умер, окно →", window_for(campaign, policy))
    blocked = await send_batch(campaign, window_for(campaign, policy), letters=200)
    print("писем ушло, пока окно закрыто:", blocked)

    # Приложение переключает кампанию на следующего отправителя.
    campaign.switch_from("warm-1")
    sent = await send_batch(campaign, window_for(campaign, policy), letters=200)
    print(f"после переключения окно {window_for(campaign, policy)}, "
          f"отправлено {sent} через {campaign.current}")

    # Опоздавший отчёт о смерти прежнего отправителя ничего не ломает.
    campaign.switch_from("warm-1")
    print("после опоздавшего отчёта текущий остался:", campaign.current)
    print("расход по отправителям:", campaign.sent)


if __name__ == "__main__":
    asyncio.run(main())

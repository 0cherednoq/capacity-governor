"""Квота внешнего API: когда докупать ключи и сколько.

Ресурс: ключи стороннего API, каждый с месячным лимитом в 10 000 вызовов. Закупка не
мгновенна — заявка на новый ключ обрабатывается людьми, и заказывать их нужно заранее
и не больше, чем разрешено за окно.

Это другая половина библиотеки: не «кому сколько сейчас», а «когда пополнять».
`ReorderPoint` отвечает на три вопроса сразу — упал ли запас ниже планки, хватает ли
его на уже поставленную работу, и сколько осталось от бюджета заказов.

    python -m examples.api_quota
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from capacity import ReorderPoint, SupplySnapshot

CALLS_PER_KEY = 10_000
NOW = datetime(2026, 3, 14, 9, 30, tzinfo=UTC)

policy = ReorderPoint(
    target=20,          # хотим держать запас в 20 ключей
    reorder_ratio=0.3,  # ниже 30 % от цели — дозаказать до цели
    max_per_window=5,   # не больше пяти заявок в сутки
    window=timedelta(days=1),
)


def keys(calls: int) -> float:
    """Перевести вызовы в ключи: запас считается дробным (G1)."""
    return calls / CALLS_PER_KEY


def describe(title: str, supply: SupplySnapshot) -> None:
    order = policy.to_order(supply)
    print(
        f"{title:<42} запас {supply.effective_available:>5.1f} "
        f"нужно {supply.needed_for_demand:>5.1f} "
        f"в работе {supply.ordered_in_progress:>2} → заказать {order}"
    )


async def main() -> None:
    describe(
        "обычный день: запаса хватает",
        SupplySnapshot(effective_available=18.0, needed_for_demand=keys(50_000)),
    )
    describe(
        "запас просел ниже планки",
        SupplySnapshot(effective_available=4.0, needed_for_demand=keys(20_000)),
    )
    describe(
        "наплыв: планка цела, но работы больше запаса",
        SupplySnapshot(effective_available=15.0, needed_for_demand=keys(400_000)),
    )
    describe(
        "заявки уже в работе — повторно не заказываем",
        SupplySnapshot(
            effective_available=4.0,
            needed_for_demand=keys(20_000),
            ordered_in_progress=16,
        ),
    )
    describe(
        "бюджет суток исчерпан",
        SupplySnapshot(
            effective_available=0.0,
            needed_for_demand=keys(100_000),
            ordered_in_window=5,
        ),
    )

    # Ключ заказа привязан к началу бюджетного окна, а не к моменту такта: повторный
    # такт в тех же сутках дописывает недостающие заявки, а не создаёт новую пачку.
    print("\nключ заказа в 09:30 →", policy.window_key(NOW))
    print("ключ заказа в 23:59 →", policy.window_key(NOW.replace(hour=23, minute=59)))
    print("ключ заказа назавтра →", policy.window_key(NOW + timedelta(days=1)))


if __name__ == "__main__":
    asyncio.run(main())

"""Примеры из `examples/` — исполняемая документация.

Пример, который перестал запускаться, хуже отсутствующего: читатель верит ему и теряет
время. Поэтому каждый прогоняется здесь целиком, а ключевые числа из `docs/use-cases.md`
проверяются, чтобы документ и код не разошлись.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from examples import api_quota, gpu_slots, mailing_senders, proxy_pool

from capacity import ReorderPoint, Sequential, SupplySnapshot

DOCS = Path(__file__).resolve().parent.parent / "docs" / "use-cases.md"
README = Path(__file__).resolve().parent.parent / "README.md"


@pytest.mark.parametrize(
    "example",
    [proxy_pool, gpu_slots, mailing_senders, api_quota],
    ids=lambda module: module.__name__.rsplit(".", 1)[-1],
)
async def test_example_runs(example: object) -> None:
    await example.main()  # type: ignore[attr-defined]


# --- числа, напечатанные в документации ---------------------------------------------


async def test_proxy_pool_splits_by_weight() -> None:
    """Веса 2:1:1 на двенадцати прокси — первая строка разбора в документации."""
    governor = proxy_pool.build()[0]

    first = await governor.tick()

    assert first.plan is not None
    assert first.plan.allocation.grants == {
        "urgent-scrape": 6,
        "daily-scrape": 3,
        "backfill": 3,
    }


async def test_proxy_pool_frees_the_share_when_work_ends() -> None:
    governor, _, registry, _, clock = proxy_pool.build()

    await governor.tick()
    registry.set_demand("urgent-scrape", 0)
    clock.advance(5)
    second = await governor.tick()

    assert second.plan is not None
    assert second.plan.allocation.grants == {
        "urgent-scrape": 0,
        "daily-scrape": 6,
        "backfill": 6,
    }


async def test_gpu_guarantee_and_ceiling_hold() -> None:
    governor = gpu_slots.build()[0]

    full = await governor.tick()

    assert full.plan is not None
    assert full.plan.allocation.grants == {
        "prod-inference": 4,
        "research-queue": 2,
        "batch-retrain": 2,
    }


async def test_gpu_guarantee_cannot_create_a_slot() -> None:
    """Последняя строка разбора: гарантия задаёт очерёдность, а не наличие."""
    governor, capacity, clock = gpu_slots.build()

    await governor.tick()
    capacity.eligible = 1
    clock.advance(5 + 301)
    last = await governor.tick()

    assert last.plan is not None
    assert last.plan.allocation.grants == {
        "prod-inference": 1,
        "research-queue": 0,
        "batch-retrain": 0,
    }


async def test_mailing_window_follows_the_current_sender() -> None:
    campaign = mailing_senders.Campaign("demo", senders=["warm-1", "warm-2"])
    policy = Sequential()

    assert mailing_senders.window_for(campaign, policy) == 1
    campaign.dead.add("warm-1")
    assert mailing_senders.window_for(campaign, policy) == 0
    campaign.switch_from("warm-1")
    assert mailing_senders.window_for(campaign, policy) == 1
    assert campaign.current == "warm-2"


async def test_mailing_stale_report_does_not_skip_senders() -> None:
    campaign = mailing_senders.Campaign("demo", senders=["warm-1", "warm-2", "warm-3"])

    campaign.switch_from("warm-1")
    campaign.switch_from("warm-1")  # опоздавший отчёт о том же отправителе

    assert campaign.current == "warm-2"


@pytest.mark.parametrize(
    ("title", "supply", "expected"),
    [
        ("обычный день", SupplySnapshot(18.0, 5.0), 0),
        ("запас просел", SupplySnapshot(4.0, 2.0), 5),
        ("наплыв", SupplySnapshot(15.0, 40.0), 5),
        ("заявки в работе", SupplySnapshot(4.0, 2.0, ordered_in_progress=16), 0),
        ("бюджет исчерпан", SupplySnapshot(0.0, 10.0, ordered_in_window=5), 0),
    ],
)
def test_api_quota_table(title: str, supply: SupplySnapshot, expected: int) -> None:
    """Таблица решений из разбора квоты API."""
    assert api_quota.policy.to_order(supply) == expected, title


def test_api_quota_key_covers_the_whole_budget_window() -> None:
    policy: ReorderPoint = api_quota.policy
    morning = api_quota.NOW
    midnight = api_quota.NOW.replace(hour=23, minute=59)

    assert policy.window_key(morning) == policy.window_key(midnight)
    assert policy.window_key(morning) != policy.window_key(morning + timedelta(days=1))


# --- документация -------------------------------------------------------------------


def test_every_example_is_linked_from_the_use_cases() -> None:
    """Пример, на который никто не ссылается, читатель не найдёт."""
    text = DOCS.read_text(encoding="utf-8")
    examples = sorted(
        path.stem
        for path in (Path(__file__).resolve().parent.parent / "examples").glob("*.py")
        if path.stem != "__init__"
    )

    missing = [name for name in examples if f"examples/{name}.py" not in text]

    assert missing == []


def test_readme_links_resolve() -> None:
    """Ссылки README ведут на существующие файлы: битая ссылка в нём дороже всего."""
    root = README.parent
    text = README.read_text(encoding="utf-8")
    targets = [
        part.split(")")[0]
        for part in text.split("](")[1:]
        if not part.startswith(("http://", "https://", "#"))
    ]

    broken = [t for t in targets if not (root / t.split("#")[0]).exists()]

    assert broken == []
